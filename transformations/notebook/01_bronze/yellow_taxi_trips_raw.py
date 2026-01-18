# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from dateutil.relativedelta import relativedelta

# COMMAND ----------

from datetime import date

# Obtains the year-month for 2 months prior to the current month in yyyy-MM format
two_months_ago = date.today() - relativedelta(months=2)
formatted_date = two_months_ago.strftime("%Y-%m")

# COMMAND ----------

#Checking and loading data only 2 months prior (since source has lag of 2 months)
df = spark.read.format("parquet").load(f"/Volumes/nyc_taxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}")

# COMMAND ----------

#Adding field for timestamp information for data ingestion
df = df.withColumn("processed_timestamp", current_timestamp())

# COMMAND ----------

#Appending new data to existing data in our managed delta table
df.write.mode("append").saveAsTable("nyc_taxi.01_bronze.yellow_trips_raw")

# COMMAND ----------

