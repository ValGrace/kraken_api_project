from pyspark.sql.types import StructType, StructField, StringType, IntType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, spark_max, count, sum
from spark_consumer import spark_consumer

def rt_consumer():
    ohlc_df = spark_consumer("krakentopic.public.ohlc_sticks")

    ohlc_schema = StructType([
        StructField('btc_asks_ts', TimestampType(), False),
        StructField('btc_asks_open', StringType(), False),
        StructField('btc_asks_high', StringType(), False),
        StructField("btc_asks_low", StringType(), False),
        StructField("btc_asks_close", StringType(), False),
        StructField("btc_asks_vwap", StringType(), False),
        StructField("btc_asks_volume", StringType(), False),
        StructField("btc_asks_trades", IntType(), False)
    ])

    json_ohlcdf = ohlc_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), ohlc_schema).alias("data"))
    
    dpa = "data.payload.after"
    
    ohlc_df = json_ohlcdf.select(
        col(f"{dpa}.btc_asks_ts").alias("timestamp"),
        col(f"{dpa}.btc_asks_open").alias("open").cast("double"),
        col(f"{dpa}.btc_asks_high").alias("high").cast("double"),
        col(f"{dpa}.btc_asks_low").alias("low").cast("double"),
        col(f"{dpa}.btc_asks_vwap").alias("vwap").cast("double"),
        col(f"{dpa}.btc_asks_volume").alias("volume").cast("double"),
        col(f"{dpa}.btc_asks_trades").alias("trades")
    )

    ohlc_df.printSchema()

    assets_stats_df = ohlc_df.agg(
        sum("trades").alias("total_trades"),
        spark_max("trades").alias("max_trades"),
        spark_max("price").alias("max_price"),
    )

    ohlc_df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="ohlc_sticks", keyspace="krakentrades")\
        .save()

