# Databricks notebook source
# MAGIC %run ../../_resources/01-config

# COMMAND ----------

# MAGIC %python
# MAGIC  aws_bucket_name="s3://"+spark.conf.get("da.workshop_bucket")
# MAGIC print({aws_bucket_name})

# COMMAND ----------

mount_name = "s3"
dbutils.fs.mount(f"s3a://{aws_bucket_name}", f"/mnt/{mount_name}")
display(dbutils.fs.ls(f"/mnt/{mount_name}"))

# COMMAND ----------

spark.sql(f"create catalog if not exists raw managed location '{aws_bucket_name}/catstoragedefault'");

# COMMAND ----------

spark.sql("create schema if not exists raw.nyctaxis");

# COMMAND ----------

spark.sql(f"CREATE EXTERNAL VOLUME if not exists raw.nyctaxis.volratecode LOCATION '{aws_bucket_name}/mount/ratecode' COMMENT 'This is raw volume'").show()

#  COMMAND ----------

spark.sql(f"CREATE EXTERNAL VOLUME if not exists raw.nyctaxis.voltripdata LOCATION '{aws_bucket_name}/mount/tripdata' COMMENT 'This is raw volume'").show()

# COMMAND ----------

spark.sql("grant READ VOLUME  on VOLUME `raw`.`nyctaxis`.`voltripdata` to `account users`").show()

# COMMAND ----------

spark.sql("grant READ VOLUME  on VOLUME `raw`.`nyctaxis`.`volratecode` to `account users`").show()
