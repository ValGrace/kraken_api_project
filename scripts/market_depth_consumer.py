from pyspark.sql.types import StructType, StructField, StringType, IntType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, spark_max, count
from spark_consumer import spark_consumer

def depth_consumer():
    depth_df = spark_consumer("krakentopic.public.market_depth")

    depth_schema = StructType([
        StructField('ask_price', StringType(), False),
        StructField('ask_volume', StringType(), False),
        StructField('ask_ts', TimestampType(), False),
        StructField("bid_price", StringType(), False),
        StructField("bid_volume", StringType(), False),
        StructField("bid_ts", TimestampType(), False)
    ])

    json_depthdf = depth_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), depth_schema).alias("data"))
    
    dpa = "data.payload.after"
    
    recent_trades_df = json_depthdf.select(
        col(f"{dpa}.ask_price").alias("price").cast("double"),
        col(f"{dpa}.ask_volume").alias("ask_volume").cast("double"),
        col(f"{dpa}.ask_ts").alias("ask_time"),
        col(f"{dpa}.bid_price").alias("bid_price").cast("double"),
        col(f"{dpa}.bid_volume").alias("order").cast("double"),
        col(f"{dpa}.bid_ts").alias("bid_time"),
    )

    recent_trades_df.printSchema()

    assets_stats_df = recent_trades_df.agg(
        count("trade_id").alias("total_trades"),
        spark_max("ask_volume").alias("max_ask_volume"),
        spark_max("price").alias("max_price"),
    )

    recent_trades_df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="market_depth", keyspace="krakentrades")\
        .save()

