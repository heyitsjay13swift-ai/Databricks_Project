# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

df = spark.read.format("parquet").load("/Volumes/nyc_taxi/00_landing/data_sources/nyctaxi_yellow/*")

# COMMAND ----------

df = df.withColumn("processed_timestamp", current_timestamp())

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("Delta").mode("overwrite").saveAsTable("nyc_taxi.01_bronze.yellow_trips_raw")

# COMMAND ----------

