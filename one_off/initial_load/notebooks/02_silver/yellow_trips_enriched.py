# Databricks notebook source
from pyspark.sql.functions import col, timestamp_diff

# COMMAND ----------

df = spark.read.table("nyc_taxi.02_silver.yellow_trips_cleansed")

# COMMAND ----------

#adding new field for representing trip duration in minutes
df_trips = df.select(
    '*',
    timestamp_diff('MINUTE', col("tpep_pickup_datetime"), col("tpep_dropoff_datetime")).alias("trip_duration_mins")
)

# COMMAND ----------

df_zones = spark.read.table("nyc_taxi.02_silver.taxi_zone_lookup")

# COMMAND ----------

#join for pu borough
df_join_1 = df_trips.join(df_zones,
                       df_trips.pu_location_id == df_zones.location_id,
                       "left").select(
                           df_trips.vendor,
                           df_trips.tpep_pickup_datetime,
                           df_trips.tpep_dropoff_datetime,
                           df_trips.trip_duration_mins,
                           df_trips.passenger_count,
                           df_trips.trip_distance,
                           df_trips.rate_type,
                           df_trips.store_and_fwd_flag,
                           df_zones.borough.alias("pu_borough"),
                           df_zones.zone.alias("pu_zone"),
                           df_trips.do_location_id, #since we need to join for do_borough as well
                           df_trips.payment_type,
                           df_trips.fare_amount,
                           df_trips.extra,
                           df_trips.mta_tax,
                           df_trips.tip_amount,
                           df_trips.tolls_amount,
                           df_trips.improvement_surcharge,
                           df_trips.total_amount,
                           df_trips.congestion_surcharge,
                           df_trips.Airport_fee,
                           df_trips.cbd_congestion_fee,
                           df_trips.processed_timestamp
                       )

# COMMAND ----------

#join fo do borough
df_join_final = df_join_1.join(df_zones,
                              df_join_1.do_location_id == df_zones.location_id,
                              "left").select(
                            df_join_1.vendor,
                           df_join_1.tpep_pickup_datetime,
                           df_join_1.tpep_dropoff_datetime,
                           df_join_1.trip_duration_mins,
                           df_join_1.passenger_count,
                           df_join_1.trip_distance,
                           df_join_1.rate_type,
                           df_join_1.store_and_fwd_flag,
                           df_join_1.pu_borough,
                           df_join_1.pu_zone,
                           df_zones.borough.alias("do_borough"),
                           df_zones.zone.alias("do_zone"),
                           df_join_1.payment_type,
                           df_join_1.fare_amount,
                           df_join_1.extra,
                           df_join_1.mta_tax,
                           df_join_1.tip_amount,
                           df_join_1.tolls_amount,
                           df_join_1.improvement_surcharge,
                           df_join_1.total_amount,
                           df_join_1.congestion_surcharge,
                           df_join_1.Airport_fee.alias("airport_fee"),
                           df_join_1.cbd_congestion_fee,
                           df_join_1.processed_timestamp
                              )

# COMMAND ----------

df_join_final.write.mode("overwrite").saveAsTable("nyc_taxi.02_silver.yellow_trips_enriched")