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
            StructField('tick_size', DoubleType(), False)
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
        col(f"{dpa}.fees").alias("fees")
    )

    extracted_asset.printSchema()

    assets_stats_df = extracted_asset.agg(
        count("altname").alias("total_assets"),
        spark_max("cost_decimals").alias("max_costs")
    )

