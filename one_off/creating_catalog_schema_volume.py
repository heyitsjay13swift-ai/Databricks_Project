# Databricks notebook source
#using python api
spark.sql("create catalog if not exists nyc_taxi")

# COMMAND ----------

#creating schemas
spark.sql("create schema if not exists nyc_taxi.00_landing")
spark.sql("create schema if not exists nyc_taxi.01_bronze")
spark.sql("create schema if not exists nyc_taxi.02_silver")
spark.sql("create schema if not exists nyc_taxi.03_gold")

# COMMAND ----------

#creating volume for raw data ingestion
spark.sql("create volume if not exists nyc_taxi.00_landing.data_sources")

# COMMAND ----------

