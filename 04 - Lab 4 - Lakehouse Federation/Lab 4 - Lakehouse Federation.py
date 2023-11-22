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

# MAGIC %sql
# MAGIC CREATE CONNECTION <connection_name> TYPE mysql
# MAGIC OPTIONS (
# MAGIC   host '<host name>',
# MAGIC   port '3306',
# MAGIC   user 'labuser',
# MAGIC   password '<password>'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS <catalog_name> USING CONNECTION <connection_name>;
