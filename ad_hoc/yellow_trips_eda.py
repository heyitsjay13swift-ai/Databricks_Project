# Databricks notebook source
# MAGIC %md
# MAGIC Which vendor makes most revenue

# COMMAND ----------

# MAGIC %md
# MAGIC What is the most popular pickup borough

# COMMAND ----------

# MAGIC %md
# MAGIC What is the most common journey (borough to borough)

# COMMAND ----------

# MAGIC %md
# MAGIC Create a time series chart showing the number of trips & total revenue per day

# COMMAND ----------

# MAGIC %sql
# MAGIC select vendor,round(sum(total_amount),2) as total_revenue from nyc_taxi.02_silver.yellow_trips_enriched
# MAGIC group by vendor
# MAGIC order by sum(total_amount) desc
# MAGIC --limit 1;

# COMMAND ----------

df = spark.read.table("nyc_taxi.02_silver.yellow_trips_enriched")

df.display(limit = 10)

# COMMAND ----------

from pyspark.sql.functions import count
df_int = df.groupBy("pu_borough").agg(count("*").alias("pu_borough_visits"))
df_op = df_int.orderBy("pu_borough_visits", ascending=False).limit(1)
display(df_op)

# COMMAND ----------

# MAGIC %sql
# MAGIC --3) Most popular borough to borough
# MAGIC select pu_borough||' to '||do_borough as Route, count(*) as no_of_trips 
# MAGIC from nyc_taxi.02_silver.yellow_trips_enriched
# MAGIC group by pu_borough||' to '||do_borough
# MAGIC order by no_of_trips desc
# MAGIC limit 1;

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit
df_routes = df.withColumn("Route", concat(col("pu_borough"), lit(" to "), col("do_borough")))

df_routes = df_routes.groupBy("Route").agg(count("*").alias("no_of_trips"))

df_most_trips = df_routes.select("Route").orderBy("no_of_trips", ascending = False).limit(1)

df_most_trips.display()


# COMMAND ----------

import pandas as pd #not required, we can directly utilize by display() output, (+) option

df2 = spark.read.table("nyc_taxi.03_gold.daily_trips_summary")

df2.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from nyc_taxi.02_silver.yellow_trips_enriched
# MAGIC where month(tpep_pickup_datetime) = 11
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select month(tpep_pickup_datetime),count(*) from nyc_taxi.`01_bronze`.yellow_trips_raw
# MAGIC group by month(tpep_pickup_datetime);