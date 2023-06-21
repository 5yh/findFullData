import findspark
findspark.init()
from itertools import count
import sys
import time
import numpy as np
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

if __name__=='__main__':
    spark_session = SparkSession \
    .builder \
    .appName("find_neighbor_nodes") \
    .config("spark.driver.memory", "15g") \
    .getOrCreate()
    spark_session = SparkSession \
                        .builder \
                        .appName("get_all_graph_features") \
                        .config("spark.some.config.option", "some-value")\
                        .getOrCreate()
    #注意,id就是address
         
    spark_session.sparkContext.setLogLevel("Error")

    # #提取component为16463的节点
    # component_data =  spark_session.read.option("header",True).option('inferSchema',True).csv('hdfs://ns00/lr/one_year_component/components_data_112345')
    # component_data = component_data.filter(component_data.component == 16463)
    #读取原始黑名单：
    label = spark_session.read.option("header",True).csv("hdfs://ns00/lr/xgboost/labeled_accounts.csv")
    black_list = spark_session.read.option("header",True).csv("hdfs://ns00/lr/black_list.csv")
    black_list = black_list.drop('id')
    black_list = black_list.withColumn("label", F.lit(1)).withColumnRenamed("blacklist", "id")
    label = label.withColumn("label", F.lit(1))
    label = label.drop('id')
    label = label.withColumnRenamed("account", "id").select('id','label').distinct()
    new_label=label.union(black_list)
    new_label = new_label.distinct()
    print('黑名单样本数量是：',new_label.count())

    #连通分量数据和黑名单数据取交集
    

    #读取一年的交易数据
    ether_data_schema=StructType([
                            StructField('timestamp',IntegerType(),True),
                            StructField('from',StringType(),True),
                            StructField('to',StringType(),True),
                            StructField('coin',StringType(),True),
                            StructField('value',FloatType(),True),
                            StructField('transHash',StringType(),True),
                            StructField('gasUsed',FloatType(),True),
                            StructField('gaslimit',FloatType(),True),
                            StructField('fee',FloatType(),True),
                            StructField('fromType',StringType(),True),
                            StructField('toType',StringType(),True),
                            StructField('transType',StringType(),True),
                            StructField('isLoop',IntegerType(),True),
                            StructField('status',IntegerType(),True)
                            ])
    all_data= spark_session.read.option("header", True).schema(ether_data_schema).csv('hdfs://ns00/lxl/eth_data/t_edge_id')
    all_data = all_data.filter(F.col("timestamp")>=1598889600)
    all_data = all_data.filter(F.col("timestamp")<1630425600)
    all_data = all_data.select('from','to')
    # xx = all_data.select('from')
    # yy = all_data.select('to')
    # zz = xx.union(yy).distinct().withColumnRenamed('from','id')
    # component_data = component_data.join(new_label,on = 'id',how = 'inner')
    # component_data = component_data.join(zz,on = 'id',how = 'inner')
    # print(component_data.count())
    # print(component_data.head(30))
    

    #对一年数据进行邻居节点寻找
    # for i in range(1):
        #生成1个种子数据：
    seed_label_data_1 = new_label.filter(new_label.id == '0x030a71c9cf65df5a710ebc49772a601ceef95745')
    seed_label_data = new_label.filter(new_label.id == '0xb2628597aac64b136c2aae3a516ea79a44817d77')
    seed_label_data_2 = new_label.filter(new_label.id == '0x4fed1fc4144c223ae3c1553be203cdfcbd38c581')#交易所账户
    seed_label_data = seed_label_data.drop('label')
    seed_label_data = seed_label_data.withColumnRenamed('id','from')
    seed_label_data_1 = seed_label_data_1.drop('label')
    seed_label_data_1 = seed_label_data_1.withColumnRenamed('id','from')
    print('seed_label_data_1是：',seed_label_data_1.head(10))
    seed_label_data_2 = seed_label_data_2.drop('label')
    seed_label_data_2 = seed_label_data_2.withColumnRenamed('id','from')
        
    print('开始寻找邻居')

    source_neighbor = all_data.join(seed_label_data_1,on = 'from',how = 'inner')
    s = source_neighbor.select('to').distinct()
    seed_label_data_1_ = seed_label_data_1.withColumnRenamed('from','to')
    target_neighbor = all_data.join(seed_label_data_1_,on = 'to',how = 'inner')
    t = target_neighbor.select('from').distinct()
    s = s.union(t)#和seed_label_data_1有关的id，无论from还是to
    # 原加目标
    source_neighbor_ = source_neighbor.union(target_neighbor)
    s.show()
    print('s的个数是：',s.count())

    source_neighbor_1 = all_data.join(seed_label_data,on = 'from',how = 'inner')
    #seed_label_data = seed_label_data.withColumnRenamed('from','to')
    #target_neighbor_1 = all_data.join(seed_label_data,on = 'to',how = 'inner')
    #edges_neighbor_1 = source_neighbor_1.union(target_neighbor_1).distinct()#一重邻居的边的情况
    s1 = source_neighbor_1.select('to').distinct()#以骗子为from到达的7个to
    print('s1的个数是：',s1.count())
    #t1 = target_neighbor_1.select('from').distinct()
    neighnor_1 = s1.distinct().withColumnRenamed('to','from')#节点情况
    
    print('一阶邻居节点个数是：',neighnor_1.count())

    source_neighbor_2 = all_data.join(seed_label_data_2,on = 'from',how = 'inner').sample(withReplacement = False,seed = 10, fraction = 0.0005)
    s2 = source_neighbor_2.select('to')#以交易所为from到达的to
    print('s2的个数是：',s2.count())

    final_id = seed_label_data_1.union(s)
    final_id = final_id.union(s1)
    final_id = final_id.union(s2).distinct()
    print('final_id的个数是：',final_id.count())
  
    print('------------------------------')
    final_id_ = final_id.withColumnRenamed('from','id').distinct()
    final_edges = source_neighbor_.union(source_neighbor_1).union(source_neighbor_2).distinct()
    label_id = final_id_.join(new_label,on = 'id',how = 'left')
    label_id = label_id.fillna(0).distinct()
    final_id_.write.option('header',True).csv('file:///home/lxl/syh/new615/lr/final_id')
    final_edges.write.option('header',True).csv('file:///home/lxl/syh/new615/lr/final_edges')
    label_id.write.option('header',True).csv('file:///home/lxl/syh/new615/lr/label_id')

    third_id = neighnor_1.join(all_data,on = 'from' , how = 'inner')
    third_id = third_id.groupby('from').count().withColumnRenamed('count','number')
    third_id_ = third_id.filter(third_id.number == 3)
    print('三重邻居的节点个数为：',third_id.count())
    print(third_id.head(10))

    # second_id = third_id.filter(third_id.number == 2)
    # print('二重邻居的节点个数为：',second_id.count())
    # print(second_id.head(10))

    #source_neighbor_2 = all_data.join(neighnor_1,on = 'from',how = 'inner')
    # neighnor_1 = neighnor_1.withColumnRenamed('from','to')
    # target_neighbor_2 = all_data.join(neighnor_1,on = 'to',how = 'inner')
    #s2 = source_neighbor_2.select('to')
    # t2 = target_neighbor_2.select('from')
    #neighbor_2 = s2.distinct().withColumnRenamed('to','from')
    #xx = source_neighbor_2.groupby('from').count().withColumnRenamed('count','number')
    #print('七个数据作为from的邻居情况是：')
    #print(xx.head(10))
    #print(xx.count())
    # print('二阶邻居节点个数是：',neighbor_2.count())
    
    # source_neighbor_3 = all_data.join(neighbor_2,on = 'from',how = 'inner')
    # neighbor_2 = neighbor_2.withColumnRenamed('from','to')
    # target_neighbor_3 = all_data.join(neighbor_2,on = 'to',how = 'inner')
    # s3 = source_neighbor_3.select('to')
    # t3 = target_neighbor_3.select('from') 
    # neighbor_3 = t3.union(s3).union(neighbor_2).distinct()


    # print('三阶邻居节点个数是：',neighbor_3.count())
    # neighbor_3_ = neighbor_3.join(new_label,on = 'id',how = 'left')
    # neighbor_3_ = neighbor_3_.fillna(0)
    # neighbor_3_.write.option('header',True).csv('/lr/seed_2_verticles_3')

    # source_neighbor_4 = all_data.join(neighbor_3,on = 'from',how = 'inner')
    # neighbor_3 = neighbor_3.withColumnRenamed('from','to')
    # target_neighbor_4 = all_data.join(neighbor_3,on = 'to',how = 'inner')
    # s4 = source_neighbor_4.select('to')
    # t4 = target_neighbor_4.select('from')
    # neighbor_4 = t4.union(s4).union(neighbor_3).distinct().withColumnRenamed('from','id')
    # print(neighbor_4.count())
    # neighbor_4 = neighbor_4.join(new_label,on = 'id',how = 'left')
    # print(neighbor_4.count())
    # neighbor_4 = neighbor_4.fillna(0)
    # print(neighbor_4.head(3))
    # neighbor_edges = source_neighbor_4.union(target_neighbor_4).distinct().withColumnRenamed('from','source').withColumnRenamed('to','target')
    # print('一号种子的四阶邻居节点数是：',neighbor_4.count())
    # print('一号种子的邻居边情况是：',neighbor_edges.count())
    # black = neighbor_4.filter(neighbor_4.label == 1)
    # print(black.count())

    # print('----开始保存数据----')
    # # neighbor_4.write.option('header',True).csv('/lr/seed_1_verticles')
    # # neighbor_edges.write.option('header',True).csv('/lr/seed_1_edges')
    spark_session.stop()