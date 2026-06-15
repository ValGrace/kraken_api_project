from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import pyspark.sql.functions as sf
from scripts.spark_consumer import spark_consumer

def rt_consumer():
    trades_df = spark_consumer("krakentopic.public.recent_trades")

    trades_schema = StructType([
        StructField('price', StringType(), False),
        StructField('ask_volume', StringType(), False),
        StructField('timestamp', TimestampType(), False),
        StructField("side", StringType(), False),
        StructField("order", StringType(), False),
        StructField("misc", StringType(), False),
        StructField("trade_id", IntegerType(), False)
    ])

    json_tradesdf = trades_df.selectExpr("CAST(value AS STRING)") \
        .select(sf.from_json(sf.col("value"), trades_schema).alias("data"))
    
    dpa = "data.payload.after"
    
    recent_trades_df = json_tradesdf.select(
        sf.col(f"{dpa}.price").alias("price").cast("double"),
        sf.col(f"{dpa}.ask_volume").alias("ask_volume").cast("doubles"),
        sf.col(f"{dpa}.timestamp").alias("timestamp"),
        sf.col(f"{dpa}.side").alias("side"),
        sf.col(f"{dpa}.order").alias("order"),
        sf.col(f"{dpa}.misc").alias("misc"),
        sf.col(f"{dpa}.trade_id").alias("trade_id").cast("integer")
    )

    recent_trades_df.printSchema()

    assets_stats_df = recent_trades_df.agg(
        sf.count("trade_id").alias("total_trades"),
        sf.max("ask_volume").alias("max_ask_volume"),
        sf.max("price").alias("max_price"),
    )

    recent_trades_df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="recent_trades", keyspace="krakentrades")\
        .save()

