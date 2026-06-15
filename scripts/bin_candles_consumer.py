from pyspark.sql.types import StructType, StructField, StringType, IntType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, spark_max, window, avg, stddev
from spark_consumer import spark_consumer

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
        StructField("trades", IntType(), False),
        StructField('taker_base', DoubleType(), False),
        StructField('taker_quote', DoubleType(), False),
        StructField("ignore", StringType(), False),

    ])

    json_tradesdf = trades_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), trades_schema).alias("data"))
    
    dpa = "data.payload.after"
    
    candles_df = json_tradesdf.select(
        col(f"{dpa}.timestamp").alias("timestamp"),
        col(f"{dpa}.open_p").alias("open_price"),
        col(f"{dpa}.high").alias("high"),
        col(f"{dpa}.low").alias("low"),
        col(f"{dpa}.volume").alias("ask_volume").cast("doubles"),
        col(f"{dpa}.close_time").alias("close_time"),
        col(f"{dpa}.quote_volume").alias("quote_volume"),
        col(f"{dpa}.trades").alias("trades"),
        col(f"{dpa}.taker_base").alias("taker_base"),
        col(f"{dpa}.trade_quote").alias("taker_quote"),
        col(f"{dpa}.ignore").alias("ignore")
    )

    candles_df.printSchema()

    candle_stats_df = candles_df.agg(
        spark_max("trades").alias("max_trades"),
        spark_max("volume").alias("highest_volume"),
        spark_max("open_price").alias("max_price"),
    )

    rolling_stats = candles_df.groupBy(
        window(col("timestamp"), "5 minutes")\
        .agg(avg("price").alias("avg_price")),
        stddev("price").alias("price_deviation"),
        avg("volume").alias("avg_volume")
    )

    anomalies = candles_df.join(
        rolling_stats, candles_df.ts.between(rolling_stats.window.start, rolling_stats.window.end))\
            .filter(
                (col("price") > col("avg_price") + 3*col("price_deviation")) |
                (col("volume") > 2*col("avg_volume"))
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
