from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntType, MapType
import pandas as pd

global spark 

spark = SparkSession.builder.appName("Process topic data in spark").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def spark_consumer(topic_name):
    df = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "broker:9092")\
        .option("subscribe", topic_name)\
        .load()
    
    df.selectExpr("CAST(key as STRING)", "CAST(value as STRING)")

    return df