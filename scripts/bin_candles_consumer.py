from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import pyspark.sql.functions as sf
from scripts.spark_consumer import spark_consumer

def writeToStats(df, epochId):
    try:
        df.write\
            .format("org.apache.spark.sql.cassandra") \
            .options(table="binance_stats", keyspace="krakenstats")\
            .mode("append") \
            .save()
    except Exception as e:
        print("Error writing stats data to Cassandra", e)

def bin_consumer():
    trades_df = spark_consumer("krakentopic.public.bin_candles")

    trades_schema = StructType([
        StructField('timestamp', TimestampType(), False),
        StructField('open_p', DoubleType(), False),
        StructField('high', DoubleType(), False),
        StructField('low', DoubleType(), False),
        StructField('close', DoubleType(), False),
        StructField("volume", DoubleType(), False),
        StructField("close_time", TimestampType(), False),
        StructField("quote_volume", DoubleType(), False),
        StructField("trades", IntegerType(), False),
        StructField('taker_base', DoubleType(), False),
        StructField('taker_quote', DoubleType(), False),
        StructField("ignore", StringType(), False),

    ])

    json_tradesdf = trades_df.selectExpr("CAST(value AS STRING)") \
        .select(sf.from_json(sf.col("value"), trades_schema).alias("data"))
    
    dpa = "data.payload.after"
    
    candles_df = json_tradesdf.select(
        sf.col(f"{dpa}.timestamp").alias("timestamp"),
        sf.col(f"{dpa}.open_p").alias("open_price"),
        sf.col(f"{dpa}.high").alias("high"),
        sf.col(f"{dpa}.low").alias("low"),
        sf.col(f"{dpa}.volume").alias("ask_volume").cast("doubles"),
        sf.col(f"{dpa}.close_time").alias("close_time"),
        sf.col(f"{dpa}.quote_volume").alias("quote_volume"),
        sf.col(f"{dpa}.trades").alias("trades"),
        sf.col(f"{dpa}.taker_base").alias("taker_base"),
        sf.col(f"{dpa}.trade_quote").alias("taker_quote"),
        sf.col(f"{dpa}.ignore").alias("ignore")
    )

    candles_df.printSchema()

    candle_stats_df = candles_df.agg(
        sf.max("trades").alias("max_trades"),
        sf.max("volume").alias("highest_volume"),
        sf.max("open_price").alias("max_price"),
    )

    rolling_stats = candles_df.groupBy(
        sf.window(sf.col("timestamp"), "5 minutes")\
        .agg(sf.avg("price").alias("avg_price")),
        sf.stddev("price").alias("price_deviation"),
        sf.avg("volume").alias("avg_volume")
    )

    anomalies = candles_df.join(
        rolling_stats, candles_df.ts.between(rolling_stats.window.start, rolling_stats.window.end))\
            .filter(
                (sf.col("price") > sf.col("avg_price") + 3*sf.col("price_deviation")) |
                (sf.col("volume") > 2*sf.col("avg_volume"))
            )
    query_anomalies = anomalies\
        .writeStream \
        .outputMode("complete")\
        .foreachBatch(writeToStats)\
        .start()
    
    query_candle_stats = candle_stats_df\
        .writeStream \
        .outputMode("complete")\
        .foreachBatch(writeToStats)\
        .start()

    candles_df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="bin_candles", keyspace="krakentrades")\
        .save()


    query_anomalies.awaitTermination()
    query_candle_stats.awaitTermination()
