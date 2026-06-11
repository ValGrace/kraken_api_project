from kafka import KafkaConsumer
from pyspark.sql import SparkSession
import pandas as pd

global spark 

spark = SparkSession.builder.appName("Process topic data in spark").getOrCreate()

def spark_consumer():
    df = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "broker:9092")\
        .option("subscribe", "krakentopic.public.assetpairs")\
        .load()








