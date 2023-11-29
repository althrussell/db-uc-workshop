# Databricks notebook source
# MAGIC %python
# MAGIC user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_").replace("+","_");
# MAGIC print(user_name);
# MAGIC uc_catalog = f"uc_catalog_{user_name}";
# MAGIC print(uc_catalog);
# MAGIC uc_database = f"uc_db_{user_name}";
# MAGIC print(uc_database);
# MAGIC


# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql(f"SYNC SCHEMA {uc_catalog}.{uc_database} FROM hive_metastore.{user_name} DRY RUN").display()

# COMMAND ----------

# MAGIC %python
# MAGIC spark.sql(f"SYNC SCHEMA {uc_catalog}.{uc_database} FROM hive_metastore.{user_name}").display()
