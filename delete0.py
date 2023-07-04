#edge_id删掉0value的
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
from shutil import rmtree

if __name__=='__main__':
    spark_session = SparkSession \
    .builder \
    .appName("delete0") \
    .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/mnt/blockchain03/findFullData/tmpdata") \
    .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/mnt/blockchain03/findFullData/tmpdata") \
    .config("spark.driver.memory", "100g") \
    .getOrCreate()
    spark_session.sparkContext.setLogLevel("Error")
    # 原始五个账号
    liuShui=spark_session.read.csv("file:///mnt/blockchain03/t_edge_id/t_edge_id", header=True, inferSchema=True)
    liuShui = liuShui.filter(F.col("timestamp")>=1598889600)
    liuShui = liuShui.filter(F.col("timestamp")<1630425600)
    liuShui = liuShui.select("timestamp","from","to","value")
    liuShui = liuShui.filter(col("value") != "0.0")
    liuShui.write.csv("file:///mnt/blockchain03/t_edge_id/t_edge_id_without0_2020_2021",header=True)
    # 应用函数到每一行
    spark_session.stop()