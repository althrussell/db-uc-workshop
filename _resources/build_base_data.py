# Databricks notebook source
import json

with open("../tools/traditional_config.json", "r") as json_conf:
  tables_conf = json.load(json_conf)['tables']

# The raw tables we want to load direct include all staging tables AND all Warehouse tables that are considered BRONZE layer 
all_tables = ([k for (k,v) in tables_conf.items() if v['layer']=='bronze'])
all_tables.sort()


# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

#dbutils.widgets.text("catalog", f"hive_metastore",'Target Catalog')
user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_").replace("+","_")
# dbutils.widgets.text("source_db", f"uc_ws_{user_name}",'HMS Database')
# #dbutils.widgets.text("ext_db", f"{user_name}_ext",'Database of External Tables')
# dbutils.widgets.text("tpcdi_directory", "s3://db-tpcdi-datagen/", "Raw Files")
# dbutils.widgets.text("scale_factor", "100", "Scale factor")
# dbutils.widgets.dropdown("table", all_tables[0], all_tables, "Target Table Name")
# dbutils.widgets.text("ext_loc", "s3://db-workshop-332745928618-ap-southeast-2-3a6d1540/", "External Location")

#managed_db = f"{dbutils.widgets.get('managed_db')}"

source_db = f"uc_ws_{user_name}"
scale_factor = "100"
tpcdi_directory = "s3://db-tpcdi-datagen/"
files_directory = f"{tpcdi_directory}{scale_factor}"
#tgt_table = dbutils.widgets.get("table")
ext_loc = "s3://"+spark.conf.get("da.workshop_bucket") +"/"+user_name+"/"
catalog = 'hive_metastore' #f"{dbutils.widgets.get('catalog')}"
uc_catalog = f"uc_catalog_{user_name}"
uc_database = f"uc_db_{user_name}"

# COMMAND ----------

spark.sql(f"""CREATE CATALOG IF NOT EXISTS {uc_catalog} MANAGED LOCATION '{ext_loc}'""")
spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {uc_catalog}.{uc_database} """)

# COMMAND ----------

spark.sql(f"""USE CATALOG {catalog} """)
spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {source_db} """)
spark.sql(f"""USE SCHEMA {source_db}""") 

# COMMAND ----------

# DBTITLE 1,Helper Function to Build out the Autoloader Streaming Tables. 
def build_autoloader_stream(table,ext):
  table_conf = tables_conf[table]
  file_format = str(table_conf.get('file_format') or 'csv')
  df = spark.readStream.format('cloudFiles').option('cloudFiles.format', file_format).option("pathGlobfilter", table_conf['filename']).option("inferSchema", False) 
  if file_format == 'csv': 
    df = df.schema(table_conf['raw_schema']).option("delimiter", table_conf['sep']).option("header", table_conf['header'])
  df = df.load(f"{files_directory}/{table_conf['path']}")
  if table_conf.get('add_tgt_query') is not None:
    df = df.selectExpr("*", table_conf.get('add_tgt_query'))
  
  print(f"{files_directory}/{table_conf['path']}")
  # Now Write

  if ext == 'ext':
    table_name = table + '_ext'
  else:
    table_name = table
    
  checkpoint_path = f"{ext_loc}{user_name}/_checkpoints/{table_name}"
  dbutils.fs.rm(checkpoint_path, True) #Drop existing checkpoint if one exists
  output_path = ext_loc + user_name + '/' + table_name

  if ext == 'ext':
    df.writeStream.format("delta").outputMode("append").option("checkpointLocation", checkpoint_path).option("mergeSchema", "true").trigger(availableNow=True).start(output_path)
  
    spark.sql(f"""DROP TABLE IF EXISTS {table_name}""")
    spark.sql(f"""CREATE EXTERNAL TABLE {table_name} LOCATION '{output_path}';""")
  else:
    spark.sql(f"""DROP TABLE IF EXISTS {table_name}""")
    df.writeStream.format("delta").outputMode("append").option("checkpointLocation", checkpoint_path).option("mergeSchema", "true").trigger(availableNow=True).toTable(f"{table_name}")
    print(f"Managed Table : {table_name} ")

# COMMAND ----------

#Build External Tables
for table in all_tables:
  build_autoloader_stream(table,'ext')


# COMMAND ----------

#Build Managed Tables
#for table in all_tables:
#  build_autoloader_stream(table,'')

# COMMAND ----------


