# Databricks notebook source
raw_data_path_trips="dbfs:/mnt/s3/mount/tripdata"
raw_data_path_rate="dbfs:/mnt/s3/mount/ratecode"

# COMMAND ----------

DDLSchema = "vendor_id string, pickup_datetime timestamp, dropoff_datetime timestamp, passenger_count int, trip_distance float, pickup_longitude float, pickup_latitude float,rate_code int, store_and_fwd_flag int, dropoff_longitude float, dropoff_latitude float, payment_type string, fare_amount float, surcharge float, mta_tax float, tip_amount float, tolls_amount float, total_amount float"

dfraw= spark.read.option("header", True).schema(DDLSchema).csv(raw_data_path_trips)

# COMMAND  ----------

user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_").replace("+","_")
source_db = f"{user_name}"
tgt_tbl_directory=f"dbfs:/mnt/s3/{user_name}/{user_name}"
dbutils.fs.mkdirs(f"dbfs:/mnt/s3/{user_name}/{user_name}/nyctaxi_trips")
dbutils.fs.mkdirs(f"dbfs:/mnt/s3/{user_name}/{user_name}/nyctaxi_ratecodes")
dbutils.fs.mkdirs(f"dbfs:/mnt/s3/{user_name}/{user_name}/nyctaxi_silver_trips")
dbutils.fs.mkdirs(f"dbfs:/mnt/s3/{user_name}/{user_name}/nyctaxi_gold_trips")
print("My extermal table location is "+tgt_tbl_directory)
print("My schema is -"+source_db)
spark.sql(f"create schema if not exists {source_db}")


# COMMAND ----------

dfraw.write.mode("overwrite").format("delta").option("path", f"{tgt_tbl_directory}/nyctaxi_trips").saveAsTable(f"{source_db}.nyctaxi_trips")

# COMMAND ----------

dfrate= spark.read.option("header", True).parquet(raw_data_path_rate)
dfrate.write.mode("overwrite").format("delta").option("path", f"{tgt_tbl_directory}/nyctaxi_ratecodes").option("mergeSchema", "true").saveAsTable(f"{source_db}.nyctaxi_ratecodes")

# COMMAND ----------

dfsilver=spark.sql(f"SELECT * FROM {source_db}.nyctaxi_trips WHERE passenger_count >1")
dfsilver.write.mode("overwrite").format("delta").option("path", f"{tgt_tbl_directory}/nyctaxi_silver_trips").saveAsTable(f"{source_db}.nyctaxi_silver_trips")

# COMMAND ----------

dfgold=spark.sql(f"SELECT RT.rate_code, avg(T.passenger_count) avgPassengers,avg(T.trip_distance) avgTrip,sum(t.total_amount) totalAmount,avg(tip_amount /(T.trip_distance * T.passenger_count)) avgTipsPPM,max(tip_amount /(T.trip_distance * T.passenger_count)) bigTipper FROM {source_db}.nyctaxi_silver_trips T LEFT JOIN {source_db}.nyctaxi_ratecodes RT on RT.rate_code = T.rate_code WHERE (T.passenger_count * T.trip_distance > 5) and (RT.rate_Code is not null) GROUP BY 1 ORDER BY 1")
dfgold.write.mode("overwrite").format("delta").option("path", f"{tgt_tbl_directory}/nyctaxi_gold_trips").saveAsTable(f"{source_db}.nyctaxi_gold_trips")
