from pyspark.sql.types import StructType, StructField, StringType, IntType, DoubleType
from pyspark.sql.functions import from_json, col, spark_max, count
from spark_consumer import spark_consumer
def ap_consumer():
    assets_df = spark_consumer("krakentopic.public.assetpairs")

    assets_schema = StructType([
            StructField('altname', StringType(), False),
            StructField('base', StringType(), False),
            StructField('aclass_base', StringType(), False),
            StructField('lot', StringType(), False),
            StructField('cost_decimals', IntType(), False),
            StructField('leverage_buy', StringType(), False),
            StructField('leverage_sell', StringType(), False),
            StructField('fees', StringType(), False),
            StructField('ordermin', DoubleType(), False),
            StructField('tick_size', DoubleType(), False),
            StructField("long_position_limit", IntType, False),
            StructField("short_position_limit", IntType, False)
        ])
    
    json_assetsdf = assets_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), assets_schema).alias("data"))
    
    dpa = "data.payload.after"
    
    extracted_asset = json_assetsdf.select(
        col(f"{dpa}.altname").alias("altname"),
        col(f"{dpa}.base").alias("base"),
        col(f"{dpa}.aclass_base").alias("aclass_base"),
        col(f"{dpa}.lot").alias("lot"),
        col(f"{dpa}.cost_decimals").alias("cost_decimals"),
        col(f"{dpa}.leverage_buy").alias("leverage_buy"),
        col(f"{dpa}.leverage_sell").alias("leverage_sell"),
        col(f"{dpa}.fees").alias("fees"),
        col(f"{dpa}.ordermin").alias("ordermin"),
        col(f"{dpa}.tick_size").alias("tick_size"),
        col(f"{dpa}.short_position_limit").alias("short_position_limit"),
        col(f"{dpa}.long_position_limit").alias("long_position_limit")
    )

    extracted_asset.printSchema()

    assets_stats_df = extracted_asset.agg(
        count("altname").alias("total_assets"),
        spark_max("cost_decimals").alias("max_costs"),
        spark_max("tick_size").alias("max_tick_size"),
        spark_max("short_position_limit").alias("max_short_position"),
        spark_max("long_position_limit").alias("max_long_position")
    )

    extracted_asset.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="assetpairs", keyspace="krakentrades")\
        .save()

