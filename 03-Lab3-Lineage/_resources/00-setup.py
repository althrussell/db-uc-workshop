# Databricks notebook source
dbutils.widgets.text("catalog", "dbdemos", "UC Catalog")

# COMMAND ----------

user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_").replace("+","_")
uc_catalog = f"uc_catalog_{user_name}"


# COMMAND ----------

spark.sql(f"""USE CATALOG {uc_catalog}""")

# COMMAND ----------

# import pyspark.sql.functions as F

# catalog = uc_catalog#dbutils.widgets.get("catalog")
# catalog_exists = False
# for r in spark.sql("SHOW CATALOGS").collect():
#     if r['catalog'] == catalog:
#         catalog_exists = True

# #As non-admin users don't have permission by default, let's do that only if the catalog doesn't exist (an admin need to run it first)     
# if not catalog_exists:
#     spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
#     spark.sql(f"GRANT CREATE, USAGE on CATALOG {catalog} TO `account users`")
# spark.sql(f"USE CATALOG {catalog}")

db_not_exist = len([db for db in spark.catalog.listDatabases() if db.name == 'uc_lineage']) == 0
if db_not_exist:
  print("creating lineage database")
  spark.sql(f"CREATE DATABASE IF NOT EXISTS uc_lineage ")
  spark.sql(f"GRANT CREATE, USAGE on DATABASE uc_lineage TO `account users`")


# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE TABLE IF NOT EXISTS uc_lineage.dinner ( recipe_id INT, full_menu STRING);
# MAGIC CREATE TABLE IF NOT EXISTS uc_lineage.dinner_price ( recipe_id INT, full_menu STRING, price DOUBLE);
# MAGIC CREATE TABLE IF NOT EXISTS uc_lineage.menu ( recipe_id INT, app STRING, main STRING, desert STRING);
# MAGIC CREATE TABLE IF NOT EXISTS uc_lineage.price ( recipe_id BIGINT, price DOUBLE) ;

# COMMAND ----------

if db_not_exist:
  #spark.sql(f"GRANT MODIFY, SELECT ON TABLE uc_lineage.dinner TO `account users`")
  spark.sql(f"GRANT MODIFY, SELECT ON TABLE uc_lineage.dinner_price TO `account users`")
  spark.sql(f"GRANT MODIFY, SELECT ON TABLE uc_lineage.menu TO `account users`")
  spark.sql(f"GRANT MODIFY, SELECT ON TABLE uc_lineage.price TO `account users`")
