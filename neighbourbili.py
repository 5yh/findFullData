#通过只找二阶且不采样计算异常节点占比
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
globalRawId=""
def calcPercentage(sparkSession,neighbours):
    blackRows = neighbours.filter(col("isInBlackListResult") == True)
    # 计算占比
    totalCount = neighbours.count()  # 获取 DataFrame 的总行数
    blackCount = blackRows.count()  # 获取 "label" 列为 True 的行数
    percentage = blackCount / totalCount  # 计算占比
    formatted_percentage = "异常占比：{:.2%}".format(percentage)
    print(totalCount)
    print(formatted_percentage)
    with open(globalRawId+"/blackNodesBili.txt", "a") as file:
        file.write(formatted_percentage + "\n")
        file.write(str(blackCount) + "\n")
        file.write(str(totalCount) + "\n")
#



    

def isInBlackList(sparkSession,neighbours,blackList,hashId):
    # neighbours=neighbours.withColumnRenamed("to","id")
    blackList=blackList.withColumnRenamed("blacklist","id")
    blackList = blackList.withColumn("islabel", F.lit(1))
    isInBlackListResult = neighbours.join(blackList, on="id", how="leftouter")
    isInBlackListResult = isInBlackListResult.withColumn("isInBlackListResult", col("islabel").isNotNull())
    isInBlackListResult=isInBlackListResult.drop("islabel")
    tmpLoc="file://"+fileSaveLoc+hashId+"/s2neighboursWithBlackList.csv"
    isInBlackListResult.write.option('header',True).csv(tmpLoc)
    return isInBlackListResult
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
    s=s.withColumn("label",F.lit(1))
    s=s.distinct()
    #一阶取10个
    # s=s.sample(False, 1.0).limit(15)
    print('一阶的个数是：',s.count())
    # s.show()
    tmpLoc="file://"+fileSaveLoc+hashId+"/s.csv"
    s.write.option('header',True).csv(tmpLoc)

    # s现在有to、originaladdress、order
    s=s.withColumnRenamed('to','id')
    neigh1 = s.select("id")
    # 将 neigh1 与 DataFrame B 进行内连接，获取一阶邻居和对应二阶邻居
    neigh2 = neigh1.join(all_data, neigh1["id"] == all_data["from"], "inner").select(neigh1["id"].alias("originalAddress"), all_data["to"].alias("id")).distinct()
    neigh2.show()
    tmps=s.select("id","originalAddress")
    tmps=tmps.withColumnRenamed("id","tmp").withColumnRenamed("originalAddress","id").withColumnRenamed("tmp","originalAddress")
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
    s2 = s2.dropDuplicates(["id", "label"])
    # s2=s2.sample(False, 1.0).limit(25)
    print('2阶的个数是：',s2.count())
    # s2 = sparkSession.read.option("header",True).csv("file:///mnt/blockchain03/findFullData/0xfec1083c50c374a0f691192b137f0db6077dabbb/s2.csv")
    # s2.show()
    tmpLoc="file://"+fileSaveLoc+hashId+"/s2.csv"
    s2.write.option('header',True).csv(tmpLoc)
    

    
    return s2



def rawEachAccount(row):
    global globalRawId
    rawAccountId = row["id"]
    print(rawAccountId)
    globalRawId=rawAccountId
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
    liuShui=spark_session.read.csv("file:///mnt/blockchain03/t_edge_id/t_edge_id_without0_2020_2021", header=True, inferSchema=True)
    # liuShui = liuShui.filter(F.col("timestamp")>=1598889600)
    # liuShui = liuShui.filter(F.col("timestamp")<1630425600)
    # liuShui = liuShui.select("timestamp","from","to","value")
    # liuShui=spark_session.read.csv("file:///mnt/blockchain03/findFullData/tmpTestData/testLiushui.csv", header=True, inferSchema=True)
    
    print("流水读取完成")
    label = spark_session.read.option("header",True).csv("file:///home/lxl/syh/labeled_accounts.csv")
    print("label读取完成")
    black_list = spark_session.read.option("header",True).csv("file:///home/lxl/syh/black_list.csv")
    print("黑名单读取完成")
    # print("流水总数量:%d"%liuShui.count())
    # findTransaction(spark_session,rawAccountId,liuShui)
    # newFindTransaction(spark_session,rawAccountId,liuShui)
    rawNeighbourAccounts=findNeighbour(spark_session,rawAccountId,liuShui,label,black_list)
    # rawNeighbourAccounts = spark_session.read.csv("file:///mnt/blockchain03/findFullData/0x3b3d4eefdc603b232907a7f3d0ed1eea5c62b5f7/s2.csv", header=True, inferSchema=True)
    # rawNeighbourAccounts.show()
    neighboursWithBlackList=isInBlackList(spark_session,rawNeighbourAccounts,black_list,rawAccountId)
    # neighboursWithBlackList=spark_session.read.csv("file:///mnt/blockchain03/findFullData/0x3b3d4eefdc603b232907a7f3d0ed1eea5c62b5f7/s2neighboursWithBlackList.csv", header=True, inferSchema=True)
    # neighboursWithBlackList.foreach(lambda row: rawEachNeighbourAccount(row.asDict()))
    calcPercentage(spark_session,neighboursWithBlackList)
    # theLastMonth(spark_session,rawAccountId,liuShui,neighboursWithBlackList) 
    # findQushi(spark_session,neighboursWithBlackList,liuShui,rawAccountId)

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
