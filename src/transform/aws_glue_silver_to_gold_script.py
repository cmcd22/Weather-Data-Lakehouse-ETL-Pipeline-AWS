from awsglue.context import GlueContext
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.functions import col

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SILVER_PATH = "s3://cmcd-etl-weather-lake/silver/hourly/"
GOLD_PATH = "s3://cmcd-etl-weather-lake/gold/daily/"

# Load Silver hourly Parquet
df = spark.read.parquet(SILVER_PATH)

# Create a "date" column for grouping
df = df.withColumn("date", F.to_date("timestamp"))

# Group by city + date and compute metrics
daily_df = (
    df.groupBy("city", "date")
    .agg(
        F.max("temperature").alias("max_temp"),
        F.min("temperature").alias("min_temp"),
        F.avg("temperature").alias("avg_temp"),

        F.sum("precipitation").alias("total_precip"),
        F.sum(F.when(df.precipitation > 0, 1).otherwise(0)).alias("rainy_hours"),

        F.max("windgusts").alias("max_windgust"),
        F.avg("windspeed").alias("avg_windspeed"),

        F.max("uv_index").alias("max_uv"),
        F.sum("radiation").alias("total_radiation")
    )
)

# Add partition columns
daily_df = (
    daily_df.withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day", F.dayofmonth("date"))
)

# Data quality check
invalid_gold = daily_df.filter(
    (col("max_temp") < -50) |
    (col("max_temp") > 60) |
    (col("min_temp") < -50) |
    (col("min_temp") > 60) |
    (col("avg_temp") < -50) |
    (col("avg_temp") > 60) |
    (col("total_precip") < 0) |
    (col("rainy_hours") < 0) |
    (col("avg_windspeed") < 0) |
    (col("max_windgust") < 0) |
    (col("max_uv") < 0) |
    (col("max_uv") > 15) |
    (col("total_radiation") < 0)
)

if invalid_gold.count() > 0:
    invalid_gold.show(10, truncate=False)
    raise Exception(
        "Gold data quality check failed: invalid daily weather aggregates detected"
    )

# Write Gold Parquet output
daily_df.write.mode("overwrite").partitionBy("city", "year", "month", "day").parquet(GOLD_PATH)
