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

def calcPercentage(sparkSession,neighbours):
    blackRows = neighbours.filter(col("isInBlackListResult") == True)
    # 计算占比
    totalCount = neighbours.count()  # 获取 DataFrame 的总行数
    blackCount = blackRows.count()  # 获取 "label" 列为 True 的行数
    percentage = blackCount / totalCount  # 计算占比
    formatted_percentage = "异常占比：{:.2%}".format(percentage)
    with open("blackNodesInfo.txt", "a") as file:
        file.write(formatted_percentage + "\n")

#
def findQushi(sparkSession,neighbours,liuShui,hashId):
    neighboursTrue = neighbours.filter(neighbours["isInBlackListResult"] == True)
    neighboursTrue=neighboursTrue.select("id").withColumnRenamed("id","from")
    neighboursFalse = neighbours.filter(neighbours["isInBlackListResult"] == False)
    neighboursFalse=neighboursFalse.select("id").withColumnRenamed("id","from")

    print("黑名单节点数量：%d"% neighboursTrue.count())

    fromResult=liuShui.join(neighboursTrue,on="from",how="inner")
    print("from流水数量：%d" %fromResult.count())

    neighboursTrue=neighboursTrue.withColumnRenamed("from","to")#列名改成to来进行连接操作
    toResult=liuShui.join(neighboursTrue,on="to",how="inner")
    print("to流水数量：%d"%toResult.count())

    fromResult=fromResult.withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))
    fromResult = fromResult.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
    fromResult = fromResult.withColumn("hour", date_format(col("timestamp"), "yyyy-MM-dd HH"))
    fromResult = fromResult.withColumn("month", date_format(col("timestamp"), "yyyy-MM"))
    print("fromResult结束")
    fromDailyCounts =fromResult.groupBy("date").count().orderBy("date")
    fromDailyCounts = fromDailyCounts.withColumn("yesterday_count", lag("count").over(Window.orderBy("date")))
    fromDailyCounts = fromDailyCounts.withColumn("growth", col("count") - col("yesterday_count"))

    fromDailyTotal = fromResult.groupBy("date").agg(sum("value").alias("total_amount"))
    fromDailyResult = fromDailyCounts.join(fromDailyTotal, "date")
    fromDailyResult = fromDailyResult.select("date", "total_amount", "count","yesterday_count","growth")
    print("fromDailyResult结束")

    fromHourlyCounts = fromResult.groupBy("hour").count().orderBy("hour")
    fromHourlyCounts=fromHourlyCounts.withColumn("lasthour_count",lag("count").over(Window.orderBy("hour")))
    fromHourlyCounts = fromHourlyCounts.withColumn("growth", col("count") - col("lasthour_count"))

    fromHourlyTotal = fromResult.groupBy("hour").agg(sum("value").alias("total_amount"))
    fromHourlyResult = fromHourlyCounts.join(fromHourlyTotal, ["hour"])
    fromHourlyResult = fromHourlyResult.select("hour", "total_amount", "count","lasthour_count","growth")
    print("fromHourlyResult结束")
    
    loc=fileSaveLoc+hashId
    # if os.path.exists(loc):
    #     rmtree(loc)
    # os.makedirs(loc)
    locOfFromDailyQushi=loc+"/fromDailyQushiTrue.csv"
    locOfFromHourlyQushi=loc+"/fromHourlyQushiTrue.csv"
    print("fromDailyQushi的存放位置",locOfFromDailyQushi)
    # if os.path.exists(locOfFromDailyResult):
    #     rmtree(locOfFromDailyResult)
    # if os.path.exists(locOfFromHourlyResult):
    #     rmtree(locOfFromHourlyResult)
    fileLocOfFromDailyResult="file://"+locOfFromDailyQushi
    # fromDailyResult = fromDailyResult.coalesce(1)
    fromDailyResult.write.csv(fileLocOfFromDailyResult,header=True)
    print("fromDailyQushi保存")
    fileLocOfFromHourlyResult="file://"+locOfFromHourlyQushi
    # fromHourlyResult = fromHourlyResult.coalesce(1)
    fromHourlyResult.write.csv(fileLocOfFromHourlyResult,header=True)
    print("fromHourlyQushi保存")

    
    
    
    toResult=toResult.withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))
    toResult = toResult.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
    toResult = toResult.withColumn("hour", date_format(col("timestamp"), "yyyy-MM-dd HH"))
    toResult = toResult.withColumn("month", date_format(col("timestamp"), "yyyy-MM"))
    print("toResult结束")
    toDailyCounts =toResult.groupBy("date").count().orderBy("date")
    toDailyCounts = toDailyCounts.withColumn("yesterday_count", lag("count").over(Window.orderBy("date")))
    toDailyCounts = toDailyCounts.withColumn("growth", col("count") - col("yesterday_count"))

    toDailyTotal = toResult.groupBy("date").agg(sum("value").alias("total_amount"))
    toDailyResult = toDailyCounts.join(toDailyTotal, "date")
    toDailyResult = toDailyResult.select("date", "total_amount", "count","yesterday_count","growth")
    print("toDailyResult结束")

    toHourlyCounts = toResult.groupBy("hour").count().orderBy("hour")
    toHourlyCounts=toHourlyCounts.withColumn("lasthour_count",lag("count").over(Window.orderBy("hour")))
    toHourlyCounts = toHourlyCounts.withColumn("growth", col("count") - col("lasthour_count"))

    toHourlyTotal = toResult.groupBy("hour").agg(sum("value").alias("total_amount"))
    toHourlyResult = toHourlyCounts.join(toHourlyTotal, ["hour"])
    toHourlyResult = toHourlyResult.select("hour", "total_amount", "count","lasthour_count","growth")
    print("toHourlyResult结束")
    
    loc=fileSaveLoc+hashId
    # if os.path.exists(loc):
    #     rmtree(loc)
    # os.makedirs(loc)
    locOfToDailyQushi=loc+"/toDailyQushiTrue.csv"
    locOfToHourlyQushi=loc+"/toHourlyQushiTrue.csv"
    print("toDailyQushi的存放位置",locOfToDailyQushi)
    # if os.path.exists(locOftoDailyResult):
    #     rmtree(locOftoDailyResult)
    # if os.path.exists(locOftoHourlyResult):
    #     rmtree(locOftoHourlyResult)
    fileLocOfToDailyResult="file://"+locOfToDailyQushi
    # toDailyResult = toDailyResult.coalesce(1)
    toDailyResult.write.csv(fileLocOfToDailyResult,header=True)
    print("toDailyQushi保存")
    fileLocOfToHourlyResult="file://"+locOfToHourlyQushi
    # toHourlyResult = toHourlyResult.coalesce(1)
    toHourlyResult.write.csv(fileLocOfToHourlyResult,header=True)
    print("toHourlyQushi保存")
    print("b名单节点数量：%d"% neighboursFalse.count())

    fromResult=liuShui.join(neighboursFalse,on="from",how="inner")
    print("from流水数量：%d" %fromResult.count())

    neighboursFalse=neighboursFalse.withColumnRenamed("from","to")#列名改成to来进行连接操作
    toResult=liuShui.join(neighboursFalse,on="to",how="inner")
    print("to流水数量：%d"%toResult.count())

    fromResult=fromResult.withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))
    fromResult = fromResult.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
    fromResult = fromResult.withColumn("hour", date_format(col("timestamp"), "yyyy-MM-dd HH"))
    fromResult = fromResult.withColumn("month", date_format(col("timestamp"), "yyyy-MM"))
    print("fromResult结束")
    fromDailyCounts =fromResult.groupBy("date").count().orderBy("date")
    fromDailyCounts = fromDailyCounts.withColumn("yesterday_count", lag("count").over(Window.orderBy("date")))
    fromDailyCounts = fromDailyCounts.withColumn("growth", col("count") - col("yesterday_count"))

    fromDailyTotal = fromResult.groupBy("date").agg(sum("value").alias("total_amount"))
    fromDailyResult = fromDailyCounts.join(fromDailyTotal, "date")
    fromDailyResult = fromDailyResult.select("date", "total_amount", "count","yesterday_count","growth")
    print("fromDailyResult结束")

    fromHourlyCounts = fromResult.groupBy("hour").count().orderBy("hour")
    fromHourlyCounts=fromHourlyCounts.withColumn("lasthour_count",lag("count").over(Window.orderBy("hour")))
    fromHourlyCounts = fromHourlyCounts.withColumn("growth", col("count") - col("lasthour_count"))

    fromHourlyTotal = fromResult.groupBy("hour").agg(sum("value").alias("total_amount"))
    fromHourlyResult = fromHourlyCounts.join(fromHourlyTotal, ["hour"])
    fromHourlyResult = fromHourlyResult.select("hour", "total_amount", "count","lasthour_count","growth")
    print("fromHourlyResult结束")
    
    loc=fileSaveLoc+hashId
    # if os.path.exists(loc):
    #     rmtree(loc)
    # os.makedirs(loc)
    locOfFromDailyQushi=loc+"/fromDailyQushiFalse.csv"
    locOfFromHourlyQushi=loc+"/fromHourlyQushiFalse.csv"
    print("fromDailyQushi的存放位置",locOfFromDailyQushi)
    # if os.path.exists(locOfFromDailyResult):
    #     rmtree(locOfFromDailyResult)
    # if os.path.exists(locOfFromHourlyResult):
    #     rmtree(locOfFromHourlyResult)
    fileLocOfFromDailyResult="file://"+locOfFromDailyQushi
    # fromDailyResult = fromDailyResult.coalesce(1)
    fromDailyResult.write.csv(fileLocOfFromDailyResult,header=True)
    print("fromDailyQushi保存")
    fileLocOfFromHourlyResult="file://"+locOfFromHourlyQushi
    # fromHourlyResult = fromHourlyResult.coalesce(1)
    fromHourlyResult.write.csv(fileLocOfFromHourlyResult,header=True)
    print("fromHourlyQushi保存")

    
    
    
    toResult=toResult.withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))
    toResult = toResult.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
    toResult = toResult.withColumn("hour", date_format(col("timestamp"), "yyyy-MM-dd HH"))
    toResult = toResult.withColumn("month", date_format(col("timestamp"), "yyyy-MM"))
    print("toResult结束")
    toDailyCounts =toResult.groupBy("date").count().orderBy("date")
    toDailyCounts = toDailyCounts.withColumn("yesterday_count", lag("count").over(Window.orderBy("date")))
    toDailyCounts = toDailyCounts.withColumn("growth", col("count") - col("yesterday_count"))

    toDailyTotal = toResult.groupBy("date").agg(sum("value").alias("total_amount"))
    toDailyResult = toDailyCounts.join(toDailyTotal, "date")
    toDailyResult = toDailyResult.select("date", "total_amount", "count","yesterday_count","growth")
    print("toDailyResult结束")

    toHourlyCounts = toResult.groupBy("hour").count().orderBy("hour")
    toHourlyCounts=toHourlyCounts.withColumn("lasthour_count",lag("count").over(Window.orderBy("hour")))
    toHourlyCounts = toHourlyCounts.withColumn("growth", col("count") - col("lasthour_count"))

    toHourlyTotal = toResult.groupBy("hour").agg(sum("value").alias("total_amount"))
    toHourlyResult = toHourlyCounts.join(toHourlyTotal, ["hour"])
    toHourlyResult = toHourlyResult.select("hour", "total_amount", "count","lasthour_count","growth")
    print("toHourlyResult结束")
    
    loc=fileSaveLoc+hashId
    # if os.path.exists(loc):
    #     rmtree(loc)
    # os.makedirs(loc)
    locOfToDailyQushi=loc+"/toDailyQushiFalse.csv"
    locOfToHourlyQushi=loc+"/toHourlyQushiFalse.csv"
    print("toDailyQushi的存放位置",locOfToDailyQushi)
    # if os.path.exists(locOftoDailyResult):
    #     rmtree(locOftoDailyResult)
    # if os.path.exists(locOftoHourlyResult):
    #     rmtree(locOftoHourlyResult)
    fileLocOfToDailyResult="file://"+locOfToDailyQushi
    # toDailyResult = toDailyResult.coalesce(1)
    toDailyResult.write.csv(fileLocOfToDailyResult,header=True)
    print("toDailyQushi保存")
    fileLocOfToHourlyResult="file://"+locOfToHourlyQushi
    # toHourlyResult = toHourlyResult.coalesce(1)
    toHourlyResult.write.csv(fileLocOfToHourlyResult,header=True)
    print("toHourlyQushi保存")

def theLastMonth(sparkSession,hashId,liuShui,neighbours):
    all_data=liuShui
    all_data = all_data.filter(F.col("timestamp")>=1627747200)
    all_data = all_data.filter(F.col("timestamp")<1630425600)
    neighbours = neighbours.filter(neighbours["isInBlackListResult"] == True)
    neighbours=neighbours.select("id").withColumnRenamed("id","from")
    fromResult=liuShui.join(neighbours,on="from",how="inner")
    neighbours=neighbours.withColumnRenamed("from","to")
    toResult=liuShui.join(neighbours,on="to",how="inner")
    fromResult=fromResult.union(toResult)
    sumValue=fromResult.select(sum("value")).first()[0]
    countTransaction=fromResult.count()
    formattedSumValue = "近一个月异常点总交易金额：{}".format(sumValue)
    formattedCountTransaction = "近一个月异常点总交易笔数：{}".format(countTransaction)
    with open("blackNodesInfo.txt", "a") as file:
        file.write(formattedSumValue + "\n")
        file.write(formattedCountTransaction + "\n")


def newFindTransaction(sparkSession,hashId,liuShui,isNeighbour=False,originalHashId=None):
    # liuShui=sparkSession.read.csv("file:///mnt/blockchain03/t_edge_id/t_edge_id", header=True, inferSchema=True)
    # liuShui = liuShui.filter(F.col("timestamp")>=1598889600)
    # liuShui = liuShui.filter(F.col("timestamp")<1630425600)
    liuShui = liuShui.select("timestamp","from","to","value")
    tmpHashIdDataFrame = [(hashId,)]
    nodeName = sparkSession.createDataFrame(tmpHashIdDataFrame, ["from"])
    nodeName.show()
    print("相关节点数量：%d"% nodeName.count())

    fromResult=liuShui.join(nodeName,on="from",how="inner")
    print("from流水数量：%d" %fromResult.count())

    nodeName=nodeName.withColumnRenamed("from","to")#列名改成to来进行连接操作
    toResult=liuShui.join(nodeName,on="to",how="inner")
    print("to流水数量：%d"%toResult.count())
    fromResult=fromResult.select("timestamp","from","to","value")
    toResult=toResult.select("timestamp","from","to","value")
    fromResult=fromResult.union(toResult)
    fromResult.show()
    fromResult=fromResult.withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))
    # fromResult = fromResult.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
    # fromResult = fromResult.withColumn("time", date_format(col("timestamp"), "HH:mm:ss"))
    # fromResult = fromResult.withColumn("month", date_format(col("timestamp"), "yyyy-MM"))
    fromResult.show()
    # fromResult = fromResult.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    # fromResult.show()
    if(isNeighbour==True):
        locWithOriginalHashId=fileSaveLoc+originalHashId+'/'+hashId
        # if os.path.exists(locWithOriginalHashId):
        #     rmtree(locWithOriginalHashId)
        os.makedirs(locWithOriginalHashId)        
        locOfAllResult=locWithOriginalHashId+"/allResult.csv"
        print("allResult的存放位置",locOfAllResult)
        # if os.path.exists(locOfAllResult):
        #     rmtree(locOfAllResult)
        fileLocOfAllResult="file://"+locOfAllResult
        # fromResult = fromResult.coalesce(1)
        fromResult.write.csv(fileLocOfAllResult,header=True)
        print("allResult保存")
    else:
        loc=fileSaveLoc+hashId
        # if os.path.exists(loc):
        #     rmtree(loc)
        # os.makedirs(loc)
        locOfAllResult=loc+"/allResult.csv"
        print("allResult的存放位置",locOfAllResult)
        # if os.path.exists(locOfAllResult):
        #     rmtree(locOfAllResult)
        fileLocOfAllResult="file://"+locOfAllResult
        fromResult = fromResult.coalesce(1)
        fromResult.write.csv(fileLocOfAllResult,header=True)
        print("allResult保存")


    

def isInBlackList(sparkSession,neighbours,blackList,hashId):
    # neighbours=neighbours.withColumnRenamed("to","id")
    blackList=blackList.withColumnRenamed("blacklist","id")
    blackList = blackList.withColumn("label", F.lit(1))
    isInBlackListResult = neighbours.join(blackList, on="id", how="leftouter")
    isInBlackListResult = isInBlackListResult.withColumn("isInBlackListResult", col("label").isNotNull())
    isInBlackListResult=isInBlackListResult.drop("label")
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

    # source_neighbor = all_data.join(broadcast(seed_label_data_1),on = 'from',how = 'inner')
    source_neighbor = all_data.join(seed_label_data_1,on = 'from',how = 'inner')
    s = source_neighbor.select('to').distinct()
    # target_neighbor = all_data.join(broadcast(seed_label_data_1.withColumnRenamed('from','to')),on = 'to',how = 'inner')
    target_neighbor = all_data.join(seed_label_data_1.withColumnRenamed('from','to'),on = 'to',how = 'inner')
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
    tmps=s.select("id","originalAddress")
    tmps=tmps.withColumnRenamed("to","tmp").withColumnRenamed("originalAddress","to").withColumnRenamed("originalAddress","to").withColumnRenamed("tmp","originalAddress")
    neigh2=neigh2.exceptAll(tmps)
    neigh2 = neigh2.withColumn("label", F.lit(2))
    print("neigh2")
    neigh2.show()
    neigh3 = neigh1.join(all_data, neigh1["id"] == all_data["to"],"inner").select(neigh1["id"].alias("originalAddress"),all_data["from"].alias("id")).distinct()
    neigh3=neigh3.exceptAll(tmps)
    neigh3 = neigh3.withColumn("label", F.lit(2))
    #可能要去掉环
    print("neigh3")
    neigh3.show()
    s2=neigh2.union(neigh3).distinct()
    s2=s2.sample(False, 1.0).limit(15)
    s2.show()
    

    # s现在有to、originaladdress、order
    neigh4 = s2.select("id")
    # 将 neigh1 与 DataFrame B 进行内连接，获取一阶邻居和对应二阶邻居
    neigh5 = neigh4.join(all_data, neigh4["id"] == all_data["from"], "inner").select(neigh4.id.alias("originalAddress"), all_data.to.alias("id")).distinct()
    tmps2=s2.select("id","originalAddress")
    tmps2=tmps2.withColumnRenamed("to","tmp").withColumnRenamed("originalAddress","to").withColumnRenamed("originalAddress","to").withColumnRenamed("tmp","originalAddress")
    neigh5=neigh5.exceptAll(tmps2)
    neigh5 = neigh5.withColumn("label", F.lit(3))
    print("neigh5")
    neigh5.show()
    neigh6 = neigh4.join(all_data, neigh4["id"] == all_data["to"],"inner").select(neigh4["id"].alias("originalAddress"),all_data["from"].alias("id")).distinct()
    neigh6=neigh6.exceptAll(tmps2)
    neigh6 = neigh6.withColumn("label", F.lit(3))
    #可能要去掉环
    print("neigh6")
    neigh6.show()
    s3=neigh5.union(neigh6).distinct()
    s3=s3.sample(False, 1.0).limit(20)
    
    s3.show()

    # s现在有to、originaladdress、order
    neigh7 = s3.select("id")
    # 将 neigh1 与 DataFrame B 进行内连接，获取一阶邻居和对应二阶邻居
    neigh8 = neigh7.join(all_data, neigh7["id"] == all_data["from"], "inner").select(neigh7.id.alias("originalAddress"), all_data.to.alias("id")).distinct()
    tmps3=s3.select("id","originalAddress")
    tmps3=tmps3.withColumnRenamed("to","tmp").withColumnRenamed("originalAddress","to").withColumnRenamed("originalAddress","to").withColumnRenamed("tmp","originalAddress")
    neigh8=neigh8.exceptAll(tmps3)
    neigh8 = neigh8.withColumn("label", F.lit(4))
    print("neigh8")
    neigh8.show()
    neigh9 = neigh7.join(all_data, neigh7["id"] == all_data["to"],"inner").select(neigh7["id"].alias("originalAddress"),all_data["from"].alias("id")).distinct()
    neigh9=neigh9.exceptAll(tmps3)
    neigh9 = neigh9.withColumn("label", F.lit(4))
    #可能要去掉环
    print("neigh9")
    neigh9.show()
    s4=neigh8.union(neigh9).distinct()
    s4=s4.sample(False, 1.0).limit(10)
    s4.show()

    sall=s.union(s2).union(s3).union(s4).distinct()





    # 有bug，没删掉，要手动删一下，之后再改
    tmpLoc="file://"+fileSaveLoc+hashId+"/neighbours.csv"
    # if os.path.exists(tmpLoc):
    #     print("已存在")
    #     rmtree(tmpLoc)
    sall.write.option('header',True).csv(tmpLoc)
    return sall

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
    fromResult = fromResult.withColumn("hour", date_format(col("timestamp"), "HH"))
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
    # fromHourlyCounts = fromResult.groupBy("date","hour").count().orderBy("date","hour")
    # fromHourlyCounts=fromHourlyCounts.withColumn("lasthour_count",lag("count").over(Window.orderBy("hour")))
    # fromHourlyCounts = fromHourlyCounts.withColumn("growth", col("count") - col("lasthour_count"))

    # fromHourlyTotal = fromResult.groupBy("date","hour").agg(sum("value").alias("total_amount"))
    # fromHourlyResult = fromHourlyCounts.join(fromHourlyTotal, ["date","hour"])
    # fromHourlyResult = fromHourlyResult.select("date","hour", "total_amount", "count","lasthour_count","growth")
    # print("fromHourlyResult")
    # hourly_counts.show()
    if(isNeighbour==True):
        locWithOriginalHashId=fileSaveLoc+originalHashId+'/'+hashId
        if os.path.exists(locWithOriginalHashId):
            rmtree(locWithOriginalHashId)
        os.makedirs(locWithOriginalHashId)        
        locOfFromDailyResult=locWithOriginalHashId+"/fromDailyResult.csv"
        # locOfFromHourlyResult=locWithOriginalHashId+"/fromHourlyResult.csv"
        print("FromDailyResult的存放位置",locOfFromDailyResult)
        if os.path.exists(locOfFromDailyResult):
            rmtree(locOfFromDailyResult)
        # if os.path.exists(locOfFromHourlyResult):
        #     rmtree(locOfFromHourlyResult)
        fileLocOfFromDailyResult="file://"+locOfFromDailyResult
        fromDailyResult = fromDailyResult.coalesce(1)
        fromDailyResult.write.csv(fileLocOfFromDailyResult,header=True)
        print("fromDailyResult保存")
        # fileLocOfFromHourlyResult="file://"+locOfFromHourlyResult
        # fromHourlyResult = fromHourlyResult.coalesce(1)
        # fromHourlyResult.write.csv(fileLocOfFromHourlyResult,header=True)
        # print("fromHourlyResult保存")
    else:
        loc=fileSaveLoc+hashId
        if os.path.exists(loc):
            rmtree(loc)
        os.makedirs(loc)
        locOfFromDailyResult=loc+"/fromDailyResult.csv"
        # locOfFromHourlyResult=loc+"/fromHourlyResult.csv"
        print("FromDailyResult的存放位置",locOfFromDailyResult)
        if os.path.exists(locOfFromDailyResult):
            rmtree(locOfFromDailyResult)
        # if os.path.exists(locOfFromHourlyResult):
        #     rmtree(locOfFromHourlyResult)
        fileLocOfFromDailyResult="file://"+locOfFromDailyResult
        fromDailyResult = fromDailyResult.coalesce(1)
        fromDailyResult.write.csv(fileLocOfFromDailyResult,header=True)
        print("fromDailyResult保存")
        # fileLocOfFromHourlyResult="file://"+locOfFromHourlyResult
        # fromHourlyResult = fromHourlyResult.coalesce(1)
        # fromHourlyResult.write.csv(fileLocOfFromHourlyResult,header=True)
        # print("fromHourlyResult保存")



    toResult = toResult.withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))
    toResult = toResult.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
    toResult = toResult.withColumn("hour", date_format(col("timestamp"), "HH"))
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
    # toHourlyCounts = toResult.groupBy("date","hour").count().orderBy("date","hour")
    # toHourlyCounts=toHourlyCounts.withColumn("lasthour_count",lag("count").over(Window.orderBy("hour")))
    # toHourlyCounts = toHourlyCounts.withColumn("growth", col("count") - col("lasthour_count"))
    # # hourly_counts.show()

    # toHourlyTotal = toResult.groupBy("date","hour").agg(sum("value").alias("total_amount"))
    # toHourlyResult = toHourlyCounts.join(toHourlyTotal, ["date","hour"])
    # toHourlyResult = toHourlyResult.select("date","hour", "total_amount", "count","lasthour_count","growth")
    # print("toHourlyResult结束")
    if(isNeighbour==True):
        locWithOriginalHashId=fileSaveLoc+originalHashId+'/'+hashId
        # if os.path.exists(locWithOriginalHashId):
        #     rmtree(locWithOriginalHashId)
        # os.makedirs(locWithOriginalHashId)        
        locOfToDailyResult=locWithOriginalHashId+"/toDailyResult.csv"
        # locOfToHourlyResult=locWithOriginalHashId+"/toHourlyResult.csv"
        print("ToDailyResult的存放位置",locOfToDailyResult)
        if os.path.exists(locOfToDailyResult):
            rmtree(locOfToDailyResult)
        # if os.path.exists(locOfToHourlyResult):
        #     rmtree(locOfToHourlyResult)
        fileLocOfToDailyResult="file://"+locOfToDailyResult
        toDailyResult = toDailyResult.coalesce(1)
        toDailyResult.write.csv(fileLocOfToDailyResult,header=True)
        print("toDailyResult保存")
        # fileLocOfToHourlyResult="file://"+locOfToHourlyResult
        # toHourlyResult = toHourlyResult.coalesce(1)
        # toHourlyResult.write.csv(fileLocOfToHourlyResult,header=True)
        # print("toHourlyResult保存")
    else:
        loc=fileSaveLoc+hashId
        # if os.path.exists(loc):
        #     rmtree(loc)
        # os.makedirs(loc)
        locOfToDailyResult=loc+"/toDailyResult.csv"
        # locOfToHourlyResult=loc+"/toHourlyResult.csv"
        print("ToDailyResult的存放位置",locOfToDailyResult)
        if os.path.exists(locOfToDailyResult):
            rmtree(locOfToDailyResult)
        # if os.path.exists(locOfToHourlyResult):
        #     rmtree(locOfToHourlyResult)
        fileLocOfToDailyResult="file://"+locOfToDailyResult
        toDailyResult = toDailyResult.coalesce(1)
        toDailyResult.write.csv(fileLocOfToDailyResult,header=True)
        print("toDailyResult保存")
        # fileLocOfToHourlyResult="file://"+locOfToHourlyResult
        # toHourlyResult = toHourlyResult.coalesce(1)
        # toHourlyResult.write.csv(fileLocOfToHourlyResult,header=True)
        # print("toHourlyResult保存")
    
def rawEachNeighbourAccount(row):
    rawNeighbourId = row["id"]
    print(rawNeighbourId)    
    spark_session = SparkSession \
    .builder \
    .appName("readLiuShui") \
    .config("spark.driver.memory", "30g") \
    .getOrCreate()
    liuShui=spark_session.read.csv("file:///mnt/blockchain03/t_edge_id/t_edge_id", header=True, inferSchema=True)
    liuShui = liuShui.select("timestamp","from","to","value")
    liuShui = liuShui.filter(F.col("timestamp")>=1598889600)
    liuShui = liuShui.filter(F.col("timestamp")<1630425600)
    print("流水总数量:%d"%liuShui.count())
    # 不是originalAddress, 应该主动写一个值进去
    newFindTransaction(spark_session,rawNeighbourId,liuShui,isNeighbour=True,originalHashId=row["originalAddress"])
    # findTransaction(spark_session,rawNeighbourId,liuShui,isNeighbour=True,originalHashId=row["originalAddress"]) 

    spark_session.stop()



def rawEachAccount(row):
    rawAccountId = row["id"]
    print(rawAccountId)

    spark_session = SparkSession \
    .builder \
    .appName("readLiuShui") \
    .config("spark.driver.memory", "200g") \
    .config("spark.executor.memory", "120g") \
    .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/mnt/blockchain03/findFullData/tmpdata") \
    .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/mnt/blockchain03/findFullData/tmpdata") \
    .config("spark.sql.broadcastTimeout", "3000") \
    .getOrCreate()
    spark_session.sparkContext.setLogLevel("Error")
    liuShui=spark_session.read.csv("file:///mnt/blockchain03/t_edge_id/t_edge_id", header=True, inferSchema=True)
    liuShui = liuShui.filter(F.col("timestamp")>=1598889600)
    liuShui = liuShui.filter(F.col("timestamp")<1630425600)
    liuShui = liuShui.select("timestamp","from","to","value")
    # liuShui=spark_session.read.csv("file:///mnt/blockchain03/findFullData/tmpTestData/testLiushui.csv", header=True, inferSchema=True)
    
    print("流水读取完成")
    # label = spark_session.read.option("header",True).csv("file:///home/lxl/syh/labeled_accounts.csv")
    label = spark_session.read.option("header",True).csv("file:///home/lxl/syh/labeled_accounts.csv")
    print("label读取完成")
    black_list = spark_session.read.option("header",True).csv("file:///home/lxl/syh/black_list.csv")
    print("黑名单读取完成")
    # print("流水总数量:%d"%liuShui.count())
    # findTransaction(spark_session,rawAccountId,liuShui)
    # newFindTransaction(spark_session,rawAccountId)
    # rawNeighbourAccounts=findNeighbour(spark_session,rawAccountId,liuShui,label,black_list)
    rawNeighbourAccounts = spark_session.read.csv("file:///mnt/blockchain03/findFullData/0xfec1083c50c374a0f691192b137f0db6077dabbb/neighboursWithBlackList.csv", header=True, inferSchema=True)
    rawNeighbourAccounts.show()
    # findQushi(spark_session,rawNeighbourAccounts,liuShui,rawAccountId)
    calcPercentage(spark_session,rawNeighbourAccounts)
    theLastMonth(spark_session,rawAccountId,liuShui,rawNeighbourAccounts)
    # isInBlackList(spark_session,rawNeighbourAccounts,black_list,rawAccountId)
    # # # # 应用函数到每一行
    # rawNeighbourAccounts.foreach(lambda row: rawEachNeighbourAccount(row.asDict()))
    spark_session.stop()




if __name__=='__main__':
    spark_session = SparkSession \
    .builder \
    .appName("find_neighbor_nodes") \
    .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/mnt/blockchain03/findFullData/tmpdata") \
    .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/mnt/blockchain03/findFullData/tmpdata") \
    .config("spark.driver.memory", "100g") \
    .getOrCreate()
    spark_session.sparkContext.setLogLevel("Error")
    # 原始五个账号
    tmpLoc="file://"+fileSaveLoc+"labeled_accounts.csv"
    rawFiveAccounts = spark_session.read.csv(tmpLoc, header=True, inferSchema=True)

    # 应用函数到每一行
    rawFiveAccounts.foreach(rawEachAccount)
    spark_session.stop()
