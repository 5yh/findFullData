import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
from shutil import rmtree
fileSaveLoc="/mnt/blockchain03/findFullData/"

    
def isInBlackList(sparkSession,neighbours,blackList,hashId):
    neighbours=neighbours.withColumnRenamed("to","id")
    blackList=blackList.withColumnRenamed("blacklist","id")
    blackList = blackList.withColumn("label", F.lit(1))
    isInBlackListResult = neighbours.join(blackList, on="id", how="leftouter")
    isInBlackListResult = isInBlackListResult.withColumn("isInBlackListResult", col("label").isNotNull())
    tmpLoc="file://"+fileSaveLoc+hashId+"/neighboursWithBlackList.csv"
    isInBlackListResult.write.option('header',True).csv(tmpLoc)
def findNeighbour(sparkSession,hashId,liuShui,label,black_list):

    #读取原始黑名单：
    # label = sparkSession.read.option("header",True).csv("hdfs://ns00/lr/xgboost/labeled_accounts.csv")
    # black_list = sparkSession.read.option("header",True).csv("hdfs://ns00/lr/black_list.csv")
    black_list = black_list.drop('id')
    black_list = black_list.withColumn("label", F.lit(1)).withColumnRenamed("blacklist", "id")
    print('black_list',black_list.count())
    # black_list.show()
    label = label.withColumn("label", F.lit(1))
    label = label.drop('id')
    label = label.withColumnRenamed("account", "id").select('id','label').distinct()
    print('label',label.count())
    # label.show()
    new_label=label.union(black_list)
    new_label = new_label.distinct()
    print('黑名单样本数量是：',new_label.count())
    # new_label.show()
    #连通分量数据和黑名单数据取交集
    

    all_data=liuShui
    all_data = all_data.filter(F.col("timestamp")>=1598889600)
    all_data = all_data.filter(F.col("timestamp")<1630425600)
    all_data = all_data.select('from','to')
    # all_data.show()
    print(hashId)
    # 这里有bug
    seed_label_data_1 = new_label.filter(new_label.id == hashId)
    seed_label_data_1 = seed_label_data_1.drop('label')
    seed_label_data_1 = seed_label_data_1.withColumnRenamed('id','from')
    # seed_label_data_1=sparkSession.withColumn("from","0x1a07650af49c5722c775da84c5b1df50aade8a0d")
    print('seed_label_data_1是：',seed_label_data_1.head(10))

        
    print('开始寻找邻居')

    source_neighbor = all_data.join(seed_label_data_1,on = 'from',how = 'inner')
    s = source_neighbor.select('to').distinct()
    seed_label_data_1_ = seed_label_data_1.withColumnRenamed('from','to')
    target_neighbor = all_data.join(seed_label_data_1_,on = 'to',how = 'inner')
    t = target_neighbor.select('from').distinct()
    s = s.union(t)#和seed_label_data_1有关的id，无论from还是to
    # 原加目标

    s=s.withColumn("originalAddress", F.lit(hashId))
    s=s.withColumn("order",F.lit(1))
    s=s.distinct()
    #一阶取10个
    s=s.sample(False, 1.0).limit(10)
    print('一阶的个数是：',s.count())
    s.show()


    # s现在有to、originaladdress、order
    s=s.withColumnRenamed('to','id')
    neigh1 = s.select("id")
    # 将 neigh1 与 DataFrame B 进行内连接，获取一阶邻居和对应二阶邻居
    neigh2 = neigh1.join(all_data, neigh1["id"] == all_data["from"], "inner").select(neigh1.id.alias("originalAddress"), all_data.to.alias("id")).distinct()
    neigh2 = neigh2.withColumn("label", F.lit(2))
    print("neigh2")
    neigh2.show()
    neigh3 = neigh1.join(all_data, neigh1["id"] == all_data["to"],"inner").select(neigh1["id"].alias("originalAddress"),all_data["from"].alias("id")).distinct()
    neigh3 = neigh3.withColumn("label", F.lit(2))
    #可能要去掉环
    print("neigh3")
    neigh3.show()
    s2=neigh2.union(neigh3)
    s2.show()







    # 有bug，没删掉，要手动删一下，之后再改
    tmpLoc="file://"+fileSaveLoc+hashId+"/neighbours.csv"
    # if os.path.exists(tmpLoc):
    #     print("已存在")
    #     rmtree(tmpLoc)
    # s2.write.option('header',True).csv(tmpLoc)
    # return s2

def findTransaction(sparkSession,hashId,liuShui,isNeighbour=False,originalHashId=None):
    tmpHashIdDataFrame = [(hashId,)]
    nodeName = sparkSession.createDataFrame(tmpHashIdDataFrame, ["from"])
    nodeName.show()
    print("相关节点数量：%d"% nodeName.count())

    fromResult=liuShui.join(nodeName,on="from",how="inner")
    print("from流水数量：%d" %fromResult.count())

    nodeName=nodeName.withColumnRenamed("from","to")#列名改成to来进行连接操作
    toResult=liuShui.join(nodeName,on="to",how="inner")
    print("to流水数量：%d"%toResult.count())

    fromResult=fromResult.withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))
    fromResult = fromResult.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
    fromResult = fromResult.withColumn("hour", date_format(col("timestamp"), "yyyy-MM-dd-HH"))
    fromResult = fromResult.withColumn("month", date_format(col("timestamp"), "yyyy-MM"))
    print("fromResult结束")
    fromDailyCounts =fromResult.groupBy("date").count().orderBy("date")
    fromDailyCounts = fromDailyCounts.withColumn("yesterday_count", lag("count").over(Window.orderBy("date")))
    fromDailyCounts = fromDailyCounts.withColumn("growth", col("count") - col("yesterday_count"))

    fromDailyTotal = fromResult.groupBy("date").agg(sum("value").alias("total_amount"))
    fromDailyResult = fromDailyCounts.join(fromDailyTotal, "date")
    fromDailyResult = fromDailyResult.select("date", "total_amount", "count","yesterday_count","growth")
    print("fromDailyResult结束")

    # daily_counts.show()
    fromHourlyCounts = fromResult.groupBy("hour").count().orderBy("hour")
    fromHourlyCounts=fromHourlyCounts.withColumn("lasthour_count",lag("count").over(Window.orderBy("hour")))
    fromHourlyCounts = fromHourlyCounts.withColumn("growth", col("count") - col("lasthour_count"))

    fromHourlyTotal = fromResult.groupBy("hour").agg(sum("value").alias("total_amount"))
    fromHourlyResult = fromHourlyCounts.join(fromHourlyTotal, "hour")
    fromHourlyResult = fromHourlyResult.select("hour", "total_amount", "count","lasthour_count","growth")
    print("fromHourlyResult")
    # hourly_counts.show()
    if(isNeighbour==True):
        locWithOriginalHashId=fileSaveLoc+originalHashId+'/'+hashId
        if os.path.exists(locWithOriginalHashId):
            rmtree(locWithOriginalHashId)
        os.makedirs(locWithOriginalHashId)        
        locOfFromDailyResult=locWithOriginalHashId+"/fromDailyResult.csv"
        locOfFromHourlyResult=locWithOriginalHashId+"/fromHourlyResult.csv"
        print("FromDailyResult的存放位置",locOfFromDailyResult)
        if os.path.exists(locOfFromDailyResult):
            rmtree(locOfFromDailyResult)
        if os.path.exists(locOfFromHourlyResult):
            rmtree(locOfFromHourlyResult)
        fileLocOfFromDailyResult="file://"+locOfFromDailyResult
        fromDailyResult.write.csv(fileLocOfFromDailyResult,header=True)
        print("fromDailyResult保存")
        fileLocOfFromHourlyResult="file://"+locOfFromHourlyResult
        fromHourlyResult.write.csv(fileLocOfFromHourlyResult,header=True)
        print("fromHourlyResult保存")
    else:
        loc=fileSaveLoc+hashId
        if os.path.exists(loc):
            rmtree(loc)
        os.makedirs(loc)
        locOfFromDailyResult=loc+"/fromDailyResult.csv"
        locOfFromHourlyResult=loc+"/fromHourlyResult.csv"
        print("FromDailyResult的存放位置",locOfFromDailyResult)
        if os.path.exists(locOfFromDailyResult):
            rmtree(locOfFromDailyResult)
        if os.path.exists(locOfFromHourlyResult):
            rmtree(locOfFromHourlyResult)
        fileLocOfFromDailyResult="file://"+locOfFromDailyResult
        fromDailyResult.write.csv(fileLocOfFromDailyResult,header=True)
        print("fromDailyResult保存")
        fileLocOfFromHourlyResult="file://"+locOfFromHourlyResult
        fromHourlyResult.write.csv(fileLocOfFromHourlyResult,header=True)
        print("fromHourlyResult保存")



    toResult = toResult.withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))
    toResult = toResult.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
    toResult = toResult.withColumn("hour", date_format(col("timestamp"), "yyyy-MM-dd-HH"))
    toResult = toResult.withColumn("month", date_format(col("timestamp"), "yyyy-MM"))
    print("toResult结束")
    toDailyCounts =toResult.groupBy("date").count().orderBy("date")
    toDailyCounts = toDailyCounts.withColumn("yesterday_count", lag("count").over(Window.orderBy("date")))
    toDailyCounts = toDailyCounts.withColumn("growth", col("count") - col("yesterday_count"))

    toDailyTotal = toResult.groupBy("date").agg(sum("value").alias("total_amount"))
    toDailyResult = toDailyCounts.join(toDailyTotal, "date")
    toDailyResult = toDailyResult.select("date", "total_amount", "count","yesterday_count","growth")
    print("toDailyResult结束")
    # daily_counts.show()
    toHourlyCounts = toResult.groupBy("hour").count().orderBy("hour")
    toHourlyCounts=toHourlyCounts.withColumn("lasthour_count",lag("count").over(Window.orderBy("hour")))
    toHourlyCounts = toHourlyCounts.withColumn("growth", col("count") - col("lasthour_count"))
    # hourly_counts.show()

    toHourlyTotal = toResult.groupBy("hour").agg(sum("value").alias("total_amount"))
    toHourlyResult = toHourlyCounts.join(toHourlyTotal, "hour")
    toHourlyResult = toHourlyResult.select("hour", "total_amount", "count","lasthour_count","growth")
    print("toHourlyResult结束")
    if(isNeighbour==True):
        locWithOriginalHashId=fileSaveLoc+originalHashId+'/'+hashId
        # if os.path.exists(locWithOriginalHashId):
        #     rmtree(locWithOriginalHashId)
        # os.makedirs(locWithOriginalHashId)        
        locOfToDailyResult=locWithOriginalHashId+"/toDailyResult.csv"
        locOfToHourlyResult=locWithOriginalHashId+"/toHourlyResult.csv"
        print("ToDailyResult的存放位置",locOfToDailyResult)
        if os.path.exists(locOfToDailyResult):
            rmtree(locOfToDailyResult)
        if os.path.exists(locOfToHourlyResult):
            rmtree(locOfToHourlyResult)
        fileLocOfToDailyResult="file://"+locOfToDailyResult
        toDailyResult.write.csv(fileLocOfToDailyResult,header=True)
        print("toDailyResult保存")
        fileLocOfToHourlyResult="file://"+locOfToHourlyResult
        toHourlyResult.write.csv(fileLocOfToHourlyResult,header=True)
        print("toHourlyResult保存")
    else:
        loc=fileSaveLoc+hashId
        # if os.path.exists(loc):
        #     rmtree(loc)
        # os.makedirs(loc)
        locOfToDailyResult=loc+"/toDailyResult.csv"
        locOfToHourlyResult=loc+"/toHourlyResult.csv"
        print("ToDailyResult的存放位置",locOfToDailyResult)
        if os.path.exists(locOfToDailyResult):
            rmtree(locOfToDailyResult)
        if os.path.exists(locOfToHourlyResult):
            rmtree(locOfToHourlyResult)
        fileLocOfToDailyResult="file://"+locOfToDailyResult
        toDailyResult.write.csv(fileLocOfToDailyResult,header=True)
        print("toDailyResult保存")
        fileLocOfToHourlyResult="file://"+locOfToHourlyResult
        toHourlyResult.write.csv(fileLocOfToHourlyResult,header=True)
        print("toHourlyResult保存")
    
def rawEachNeighbourAccount(row):
    rawNeighbourId = row["to"]
    print(rawNeighbourId)    
    spark_session = SparkSession \
    .builder \
    .appName("readLiuShui") \
    .config("spark.driver.memory", "30g") \
    .getOrCreate()
    liuShui=spark_session.read.csv("file:///mnt/blockchain03/t_edge_id/t_edge_id", header=True, inferSchema=True)
    print("流水总数量:%d"%liuShui.count())
    findTransaction(spark_session,rawNeighbourId,liuShui,isNeighbour=True,originalHashId=row["originalAddress"]) 
    spark_session.stop()



def rawEachAccount(row):
    rawAccountId = row["id"]
    print(rawAccountId)

    spark_session = SparkSession \
    .builder \
    .appName("readLiuShui") \
    .config("spark.driver.memory", "30g") \
    .getOrCreate()
    spark_session.sparkContext.setLogLevel("Error")
    # liuShui=spark_session.read.csv("file:///mnt/blockchain03/t_edge_id/t_edge_id", header=Tue, inferSchema=True)
    liuShui=spark_session.read.csv("file:///mnt/blockchain03/findFullData/tmpTestData/testLiushui.csv", header=True, inferSchema=True)
    
    print("流水读取完成")
    # label = spark_session.read.option("header",True).csv("file:///home/lxl/syh/labeled_accounts.csv")
    label = spark_session.read.option("header",True).csv("file:///home/lxl/syh/labeled_accounts.csv")
    print("label读取完成")
    black_list = spark_session.read.option("header",True).csv("file:///home/lxl/syh/black_list.csv")
    print("黑名单读取完成")
    print("流水总数量:%d"%liuShui.count())
    # findTransaction(spark_session,rawAccountId,liuShui)
    rawNeighbourAccounts=findNeighbour(spark_session,rawAccountId,liuShui,label,black_list)
    # rawNeighbourAccounts = spark_session.read.csv("file:///home/lxl/syh/new615/0x030a71c9cf65df5a710ebc49772a601ceef95745/neighbours.csv", header=True, inferSchema=True)
    # rawNeighbourAccounts.show()
    # isInBlackList(spark_session,rawNeighbourAccounts,black_list,rawAccountId)
    # # # # 应用函数到每一行
    # rawNeighbourAccounts.foreach(lambda row: rawEachNeighbourAccount(row.asDict()))
    spark_session.stop()




if __name__=='__main__':
    spark_session = SparkSession \
    .builder \
    .appName("find_neighbor_nodes") \
    .config("spark.driver.memory", "15g") \
    .getOrCreate()
    spark_session.sparkContext.setLogLevel("Error")
    # 原始五个账号
    tmpLoc="file://"+fileSaveLoc+"labeled_accounts.csv"
    rawFiveAccounts = spark_session.read.csv(tmpLoc, header=True, inferSchema=True)

    # 应用函数到每一行
    rawFiveAccounts.foreach(rawEachAccount)
    spark_session.stop()
