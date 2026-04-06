import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import explode, col, coalesce

# init
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# repair partitions so new daily data is visible before reading
spark.sql("MSCK REPAIR TABLE market_pipeline_dev.prices_prices")

# read raw data from glue catalog
df = glueContext.create_dynamic_frame.from_catalog(
    database="market_pipeline_dev",
    table_name="prices_prices"
).toDF()

# flatten array
df_flat = df.withColumn("item", explode("data"))

# select clean columns and normalize close
df_clean = df_flat.select(
    col("symbol"),
    col("extract_date"),
    col("item.date").alias("date"),
    coalesce(
        col("item.close.double").cast("double"),
        col("item.close.int").cast("double")
    ).alias("close"),
    col("item.volume").cast("bigint").alias("volume")
)

# write clean parquet
df_clean.write.mode("overwrite").parquet(
    "s3://artyom-market-pipeline-dev/curated/prices/"
)
