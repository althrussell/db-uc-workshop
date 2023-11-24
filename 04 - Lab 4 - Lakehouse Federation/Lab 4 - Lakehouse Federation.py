# Databricks notebook source
# MAGIC %run ../_resources/01-config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using the UI create Connection 
# MAGIC 1. In your Databricks workspace, right click on Catalog, Open link in new Tab
# MAGIC
# MAGIC 2. In the left pane, expand the External Data menu and select Connections.
# MAGIC
# MAGIC 3. Click Create connection.
# MAGIC
# MAGIC 4. Enter a user-friendly Connection name.
# MAGIC
# MAGIC 5. Select the Connection type (database provider, like MySQL or PostgreSQL).
# MAGIC
# MAGIC 6. Enter the connection properties (such as host information, path, and access credentials).
# MAGIC
# MAGIC       Each connection type requires different connection information. See the article for your connection type, listed in the table of contents to the left.
# MAGIC
# MAGIC 7. (Optional) Click Test connection to confirm that it works.
# MAGIC
# MAGIC 8. (Optional) Add a comment.
# MAGIC
# MAGIC 9. Click Create.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Using UI to Create Catalog 
# MAGIC In your Databricks workspace, click Catalog icon Catalog.
# MAGIC
# MAGIC Click the Create Catalog button.
# MAGIC
# MAGIC On the Create a new catalog dialog, enter a name for the catalog and select a Type of Foreign.
# MAGIC
# MAGIC Select the Connection that provides access to the database that you want to mirror as a Unity Catalog catalog.
# MAGIC
# MAGIC Enter the name of the Database that you want to mirror as a catalog.
# MAGIC
# MAGIC Requirements differ depending on the data source:
# MAGIC
# MAGIC MySQL uses a two-layer namespace and therefore does not require a database name.
# MAGIC
# MAGIC For connections to a catalog in another Databricks workspace, enter the Databricks Catalog name instead of a database name.
# MAGIC
# MAGIC Click Create.

# COMMAND ----------

user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_").replace("+","_")
connection_name = user_name + '_mysql_connection'
rds_endpoint = spark.conf.get("da.rds_endpoint")
foreign_cat = user_name + '_mysql'
uc_catalog = f"uc_catalog_{user_name}"
uc_database = f"uc_db_{user_name}"
new_table = uc_catalog + "." + uc_database + ".new_states"


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
  password secret('q_fed','mysql')
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
