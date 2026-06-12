from pyspark.sql.types import StructType, StructField, StringType, IntType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, spark_max, count
from spark_consumer import spark_consumer

def rt_consumer():
    trades_df = spark_consumer("krakentopic.public.recent_trades")

    trades_schema = StructType([
        StructField('price', StringType(), False),
        StructField('ask_volume', StringType(), False),
        StructField('timestamp', TimestampType(), False),
        StructField("side", StringType(), False),
        StructField("order", StringType(), False),
        StructField("misc", StringType(), False),
        StructField("trade_id", IntType(), False)
    ])

    json_tradesdf = trades_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), trades_schema).alias("data"))
    
    dpa = "data.payload.after"
    
    recent_trades_df = json_tradesdf.select(
        col(f"{dpa}.price").alias("price").cast("double"),
        col(f"{dpa}.ask_volume").alias("ask_volume").cast("doubles"),
        col(f"{dpa}.timestamp").alias("timestamp"),
        col(f"{dpa}.side").alias("side"),
        col(f"{dpa}.order").alias("order"),
        col(f"{dpa}.misc").alias("misc"),
        col(f"{dpa}.trade_id").alias("trade_id").cast("integer")
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
        .options(table="recent_trades", keyspace="krakentrades")\
        .save()

