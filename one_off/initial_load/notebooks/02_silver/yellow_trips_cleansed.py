# Databricks notebook source
from pyspark.sql.functions import col, when,timestamp_diff, max ,min

# COMMAND ----------

df = spark.read.table("nyc_taxi.01_bronze.yellow_trips_raw")

df.display()

# COMMAND ----------

df = df.filter("tpep_pickup_datetime >= '2025-05-01' and tpep_pickup_datetime < '2025-11-01'")

# COMMAND ----------

df.select("VendorID").distinct().display()

df.select("RatecodeID").distinct().display()

# COMMAND ----------

df.show(10)

# COMMAND ----------

df.select("payment_type").distinct().display()

# COMMAND ----------

df = df.select (
    when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
        .when(col("VendorID") == 2, "Curb Mobility, LLC")
        .when(col("VendorID") == 6, "Myle Technologies Inc")
        .when(col("VendorID") == 7, "Helix")
        .otherwise("Unknown")
        .alias("vendor"),
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    when(col("RatecodeID") == 1, "Standard Rate")
        .when(col("RatecodeID") == 2, "JFK")
        .when(col("RatecodeID") == 3, "Newark")
        .when(col("RatecodeID") == 4, "Nassau or Westchester")
        .when(col("RatecodeID") == 5, "Negotiated fare")
        .when(col("RatecodeID") == 6, "Group ride")
        .otherwise("Unknown")
        .alias("rate_type"),
    "store_and_fwd_flag",
    col("PULocationID").alias("pu_location_id"),
    col("DOLocationID").alias("do_location_id"),
    when(col("payment_type") == 0, "Flex fare trip")
        .when(col("payment_type") == 1, "Credit card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No Charge")
        .when(col("payment_type") == 4, "Dispute")
        .when(col("payment_type") == 6, "Voided trip")
        .otherwise("Unknown")
        .alias("payment_type"),
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",
    "processed_timestamp"
    )

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyc_taxi.02_silver.yellow_trips_cleansed")

# COMMAND ----------

spark.read.table("nyc_taxi.02_silver.yellow_trips_cleansed").display()