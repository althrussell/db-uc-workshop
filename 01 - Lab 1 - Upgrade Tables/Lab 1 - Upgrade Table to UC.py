# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Upgrade your tables to Databricks Unity Catalog
# MAGIC
# MAGIC Unity catalog provides all the features required to your data governance & security:
# MAGIC
# MAGIC - Table ACL
# MAGIC - Row level access with dynamic view
# MAGIC - Secure access to external location (blob storage)
# MAGIC - Lineage at row & table level for data traceability
# MAGIC - Traceability with audit logs
# MAGIC
# MAGIC Because unity catalog is added as a supplement to your existing account, migrating your existing data to the new UC is very simple.
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-base-1.png" style="float: right" width="700px"/> 
# MAGIC
# MAGIC Unity Catalog works with 3 layers:
# MAGIC
# MAGIC * CATALOG
# MAGIC * SCHEMA (or DATABASE)
# MAGIC * TABLE
# MAGIC
# MAGIC The table created without Unity Catalog are available under the default `hive_metastore` catalog, and they're scoped at a workspace level.
# MAGIC
# MAGIC New tables created with Unity Catalog will available at the account level, meaning that they're cross-workspace.
# MAGIC
# MAGIC <!-- tracking, please Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&aip=1&t=event&ec=dbdemos&ea=VIEW&dp=%2F_dbdemos%2Fgovernance%2Fuc-05-upgrade%2F00-Upgrade-database-to-UC&cid=1444828305810485&uid=8264923750561714">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understanding the upgrade process
# MAGIC
# MAGIC ### External table and Managed table
# MAGIC
# MAGIC Before your upgrade, it's important to understand the difference between tables created with `External Location` vs `Managed table`
# MAGIC
# MAGIC * Managed tables are table created without any `LOCATION` instruction. They use the default database storage location which is usually your root bucket `/user/hive/warehouse/your_database/your_table`  (note that you can also change this property at the database level). If you drop a managed table it'll delete the actual data.
# MAGIC * External tables are tables with data stored to a given LOCATION. Any table created with an instruction like `LOCATION 's3a:/xxx/xxx'` is external. Dropping an External table won't delete the underlying data.
# MAGIC
# MAGIC
# MAGIC ### UC Access control
# MAGIC
# MAGIC * UC is in charge of securing your data. Tables in UC can be stored in 2 locations:
# MAGIC   * Saved under the UC metastore bucket (the one you created to setup the metastore), typically as Managed table. This is the recommended thing to do by default.
# MAGIC   * Saved under an [EXTERNAL LOCATION](https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-location.html) (one of your S3 bucket, ADLS...) secured with a STORAGE CREDENTIAL: `CREATE EXTERNAL LOCATION location_name URL url WITH (STORAGE CREDENTIAL credential_name)` 
# MAGIC   
# MAGIC In both case, **only the Unity Catalog should have the permission to access the underlying files on the cloud storage**. If a user can access the data directly at a file level (ex: in a workspace root bucket, with an instance profile already existing, or a SP) they could bypass the UC.
# MAGIC
# MAGIC ### 2 Upgrade options: pointing metadata to the external location or copying data to UC storage
# MAGIC
# MAGIC Knowing that, you have 2 options to upgrade your tables to UC:
# MAGIC * **Moving the data:** If your data resides in a cloud storage and you know users will have direct file access (ex: a managed table in your root bucket or a cloud storage on which you want to keep file access), then you should move the data to a safe location in the UC (use your UC metastore default location as a recommended choice)
# MAGIC * **Pointing to the existing data with an External Location**: If your data is in a cloud storage and you can ensure that only the UC process will have access (by removing any previously existing direct permission on instance profile/SP), you can just re-create the table metadata in the UC with an EXTERNAL LOCATION (without any data copy) 
# MAGIC
# MAGIC *Notes:*
# MAGIC * *External tables saved in a bucket you can’t secure with UC external location (ex: saved in the workspace root storage `dbfs:/...` , or a storage with instance profile / Service Principal granting direct file access you can't remove) should be fully cloned too.* 
# MAGIC * *Alternatively, Managed tables stored in an external bucket (this can be the case changing the default LOCATION of the DATABASE) can be upgrade using an External Location (without copying data).*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migrating a single External table to UC
# MAGIC Let's see how your upgrade can be executed with a simple SQL query with the 2 options.
# MAGIC
# MAGIC *Note: Databricks provides a `SYNC`command to simplify these operation over the next few months: `SYNC SCHEMA hive_metastore.syncdb TO SCHEMA main.syncdb_uc2 DRY RUN`*

# COMMAND ----------

# MAGIC %run ../_resources/set_params

# COMMAND ----------

# DBTITLE 1,Perform a DRY RUN

df = spark.sql(f"""SYNC TABLE {uc_catalog}.{uc_database}.customer_ext FROM hive_metastore.{source_db}.customer_ext DRY RUN""")
df.display()

# COMMAND ----------

# DBTITLE 1,Perform the SYNC
df = spark.sql(f"""SYNC TABLE {uc_catalog}.{uc_database}.customer_ext FROM hive_metastore.{source_db}.customer_ext""")
df.display()

# COMMAND ----------

df = spark.sql(f"""DESCRIBE EXTENDED {uc_catalog}.{uc_database}.customer_ext""")
df.display()

# COMMAND ----------

# DBTITLE 1,Query the new UC Table
df = spark.sql(f"""SELECT * FROM {uc_catalog}.{uc_database}.customer_ext LIMIT 10""")
df.display()

# COMMAND ----------

# DBTITLE 1,Alter the Schema in HIVE and lets see if it SYNC's
spark.sql(f"""ALTER TABLE hive_metastore.{source_db}.customer_ext ADD COLUMN new_column STRING""")

# COMMAND ----------

df = spark.sql(f"""SELECT new_column FROM {uc_catalog}.{uc_database}.customer_ext LIMIT 10""")
df.display()

# COMMAND ----------

# DBTITLE 1,If table is out of Sync we can run REPAIR
spark.sql(f"""REPAIR TABLE {uc_catalog}.{uc_database}.customer_ext SYNC METADATA""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC Unity Catalog can easily be added as an addition to your workspace-level databases.
# MAGIC
# MAGIC You can easily upgrade your table using the UI for Managed Tables or with simple SQL command.
# MAGIC
# MAGIC For more details on the SYNC Command please reference https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-sync.html
