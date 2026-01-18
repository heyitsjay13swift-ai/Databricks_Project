# Databricks notebook source
from pyspark.sql.functions import col,max,min,sum,avg,count, round

# COMMAND ----------

df_gold = spark.read.table("nyc_taxi.02_silver.yellow_trips_enriched")

# COMMAND ----------

df_gold.display()

# COMMAND ----------

df_gold = df_gold.\
    groupBy(df_gold.tpep_pickup_datetime.cast("date").alias("pickup_date")).\
    agg(
        count("*").alias("total_trips"),
        round(avg("passenger_count"),1).alias("avg_passengers_per_trip"),
        round(avg("trip_distance"),1).alias("avg_distance_per_trip"),
        round(avg("fare_amount"),2).alias("avg_fare_per_trip"),
        max("fare_amount").alias("max_fare"),
        min("fare_amount").alias("min_fare"),
        round(sum("total_amount"),2).alias("total_revenue")
    )

# COMMAND ----------

df_gold.display()

# COMMAND ----------

df_gold.write.mode("overwrite").saveAsTable("nyc_taxi.03_gold.daily_trips_summary")