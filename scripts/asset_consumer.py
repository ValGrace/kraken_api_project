from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pyspark.sql.functions as sf
from scripts.spark_consumer import spark_consumer
def ap_consumer():
    assets_df = spark_consumer("krakentopic.public.assetpairs")

    assets_schema = StructType([
            StructField('altname', StringType(), False),
            StructField('base', StringType(), False),
            StructField('aclass_base', StringType(), False),
            StructField('lot', StringType(), False),
            StructField('cost_decimals', IntegerType(), False),
            StructField('leverage_buy', StringType(), False),
            StructField('leverage_sell', StringType(), False),
            StructField('fees', StringType(), False),
            StructField('ordermin', DoubleType(), False),
            StructField('tick_size', DoubleType(), False),
            StructField("long_position_limit", IntegerType, False),
            StructField("short_position_limit", IntegerType, False)
        ])
    
    json_assetsdf = assets_df.selectExpr("CAST(value AS STRING)") \
        .select(sf.from_json(sf.col("value"), assets_schema).alias("data"))
    
    dpa = "data.payload.after"
    
    extracted_asset = json_assetsdf.select(
        sf.col(f"{dpa}.altname").alias("altname"),
        sf.col(f"{dpa}.base").alias("base"),
        sf.col(f"{dpa}.aclass_base").alias("aclass_base"),
        sf.col(f"{dpa}.lot").alias("lot"),
        sf.col(f"{dpa}.cost_decimals").alias("cost_decimals"),
        sf.col(f"{dpa}.leverage_buy").alias("leverage_buy"),
        sf.col(f"{dpa}.leverage_sell").alias("leverage_sell"),
        sf.col(f"{dpa}.fees").alias("fees"),
        sf.col(f"{dpa}.ordermin").alias("ordermin"),
        sf.col(f"{dpa}.tick_size").alias("tick_size"),
        sf.col(f"{dpa}.short_position_limit").alias("short_position_limit"),
        sf.col(f"{dpa}.long_position_limit").alias("long_position_limit")
    )

    extracted_asset.printSchema()

    assets_stats_df = extracted_asset.agg(
        sf.count("altname").alias("total_assets"),
        sf.max("cost_decimals").alias("max_costs"),
        sf.max("tick_size").alias("max_tick_size"),
        sf.max("short_position_limit").alias("max_short_position"),
        sf.max("long_position_limit").alias("max_long_position")
    )

    extracted_asset.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="assetpairs", keyspace="krakentrades")\
        .save()

