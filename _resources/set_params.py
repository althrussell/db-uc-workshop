# Databricks notebook source
user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_").replace("+","_")
source_db = f"hms_{user_name}"

scale_factor = "10"
tpcdi_directory = "s3://db-tpcdi-datagen/"
files_directory = f"{tpcdi_directory}{scale_factor}"
#tgt_table = dbutils.widgets.get("table")

catalog = 'hive_metastore' #f"{dbutils.widgets.get('catalog')}"
uc_catalog = f"uc_catalog_{user_name}"
print("Your UC Catalog is : "+uc_catalog)
uc_database = f"uc_db_{user_name}"
print("Your UC Database is : "+uc_database)
volume =  f"volume_{user_name}"

