from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntType, MapType
import pandas as pd
from cassandra.cluster import Cluster


cluster = Cluster(['cassdb'], port=9042)

session = cluster.connect()

session.execute("CREATE KEYSPACE IF NOT EXISTS krakentrades WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")

session.set_keyspace('krakentrades')
global spark 

spark = SparkSession.builder\
    .appName("Process topic data streams in spark")\
    .config('spark.cassandra.connection.host', 'cassdb')\
    .config('spark.cassandra.connection.port', '9042')\
    .config('spark.cassandra.output.consistency.level', 'ONE')\
    .getOrCreate()

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