from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import pyspark.sql.functions as sf
from scripts.spark_consumer import spark_consumer

def ohlc_consumer():
    ohlc_df = spark_consumer("krakentopic.public.ohlc_sticks")

    ohlc_schema = StructType([
        StructField('btc_asks_ts', TimestampType(), False),
        StructField('btc_asks_open', StringType(), False),
        StructField('btc_asks_high', StringType(), False),
        StructField("btc_asks_low", StringType(), False),
        StructField("btc_asks_close", StringType(), False),
        StructField("btc_asks_vwap", StringType(), False),
        StructField("btc_asks_volume", StringType(), False),
        StructField("btc_asks_trades", IntegerType(), False)
    ])

    json_ohlcdf = ohlc_df.selectExpr("CAST(value AS STRING)") \
        .select(sf.from_json(sf.col("value"), ohlc_schema).alias("data"))
    
    dpa = "data.payload.after"
    
    ohlc_df = json_ohlcdf.select(
        sf.col(f"{dpa}.btc_asks_ts").alias("ask_ts"),
        sf.col(f"{dpa}.btc_asks_open").alias("open").cast("double"),
        sf.col(f"{dpa}.btc_asks_high").alias("high").cast("double"),
        sf.col(f"{dpa}.btc_asks_low").alias("low").cast("double"),
        sf.col(f"{dpa}.btc_asks_vwap").alias("vwap").cast("double"),
        sf.col(f"{dpa}.btc_asks_volume").alias("volume").cast("double"),
        sf.col(f"{dpa}.btc_asks_trades").alias("trades")
    )

    ohlc_df.printSchema()

    assets_stats_df = ohlc_df.agg(
        sf.sum("trades").alias("total_trades"),
        sf.max("trades").alias("max_trades"),
        sf.max("price").alias("max_price"),
    )

    ohlc_df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="ohlc_sticks", keyspace="krakentrades")\
        .save()

