# Databricks notebook source
# MAGIC %run ../_resources/01-config

# COMMAND ----------

# DBTITLE 1,Set some Parameters
user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_").replace("+","_")
connection_name = user_name + '_mysql_connection'
rds_endpoint = spark.conf.get("da.rds_endpoint")
foreign_cat = user_name + '_mysql'
uc_catalog = f"uc_catalog_{user_name}"
uc_database = f"uc_db_{user_name}"
new_table = uc_catalog + "." + uc_database + ".new_states"
rds_password = spark.conf.get("da.rds_password")

# COMMAND ----------

# MAGIC %md
# MAGIC #You can also create Connection via SQL  
# MAGIC
# MAGIC
# MAGIC
# MAGIC > Example 
# MAGIC ```
# MAGIC CREATE CONNECTION <connection-name> TYPE postgresql
# MAGIC OPTIONS (
# MAGIC   host '<hostname>',
# MAGIC   port '<port>',
# MAGIC   user '<user>',
# MAGIC   password '<password>'
# MAGIC );
# MAGIC ```

# COMMAND ----------

spark.sql(f"""CREATE CONNECTION `{connection_name}` TYPE mysql 
OPTIONS (
  host '{rds_endpoint}',
  port '3306',
  user 'labuser',
  password '{rds_password}'
)""")

# COMMAND ----------

# MAGIC %md
# MAGIC #You can also create Catalog via SQL  
# MAGIC
# MAGIC
# MAGIC
# MAGIC > Example 
# MAGIC ```
# MAGIC CREATE CONNECTION <connection-name> TYPE postgresql
# MAGIC OPTIONS (
# MAGIC   host '<hostname>',
# MAGIC   port '<port>',
# MAGIC   user secret ('<secret-scope>','<secret-key-user>'),
# MAGIC   password secret ('<secret-scope>','<secret-key-password>')
# MAGIC )
# MAGIC ```

# COMMAND ----------

spark.sql(f"""CREATE FOREIGN CATALOG IF NOT EXISTS {foreign_cat} USING CONNECTION `{connection_name}`""")

# COMMAND ----------

df = spark.sql(f"""
SELECT a.city as city1, a.state, b.city as city2, b.stateprov FROM {foreign_cat}.demodb.customers a INNER JOIN {uc_catalog}.{uc_database}.customer_ext b ON a.state = b.stateprov
""")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Save Dataframe as a Delta Table in UC so we can See Lineage back to the mysql database  

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable(new_table)

# COMMAND ----------

print(new_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Go to Catalog Explorer and look at the lineage on Table as shown above
