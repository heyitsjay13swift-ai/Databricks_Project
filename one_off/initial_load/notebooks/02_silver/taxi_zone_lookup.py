# Databricks notebook source
from pyspark.sql.functions import lit,col,current_timestamp
from pyspark.sql.types import TimestampType, IntegerType

# COMMAND ----------

df = spark.read.format("csv").option('header',True).load("/Volumes/nyc_taxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv")

# COMMAND ----------

df = df.select(
    col("LocationID").cast(IntegerType()).alias("location_id"),
    col("Borough").alias("borough"),
    col("Zone").alias("zone"),
    col("service_zone"),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date") #stores null value for now
)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyc_taxi.02_silver.taxi_zone_lookup")

# COMMAND ----------

