import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import explode, col, coalesce, row_number
from pyspark.sql.window import Window

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

# flatten nested data array, each element becomes one row
df_flat = df.withColumn("item", explode("data"))

# select clean columns and normalize close
# close is normalized from struct{double, int} to a single double
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

# deduplicate: keep one row per symbol + date, taking most recent extract
window = Window.partitionBy("symbol", "date").orderBy(col("extract_date").desc())
df_dedup = df_clean.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")

# write clean deduplicated parquet (overwrite prevents duplicate rows on reruns)
df_dedup.write.mode("overwrite").parquet(
    "s3://artyom-market-pipeline-dev/curated/prices/"
)
