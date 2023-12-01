# Databricks notebook source
pip install -U databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/01-config

# COMMAND ----------

# MAGIC %run ../_resources/02-dms-cdc-data

# COMMAND ----------

# DBTITLE 1,Create UC Resources

startAt = 1
numberLabUser = 1 

end = numberLabUser - 1 + startAt


admin_user = spark.sql("select current_user()").collect()[0][0]
for i in range(startAt, end + 1):
    user_email = "labuser+"+str(i)+"@awsbricks.com"
    user_name = user_email.split("@")[0].replace(".","_").replace("+","_")
    print(user_name)

    uc_catalog = f"uc_catalog_{user_name}"
    uc_database = f"uc_db_{user_name}"
        
    # Execute SQL command
    catalogs_df = spark.sql("SHOW CATALOGS")
    # Check if catalog exists
    catalog_exists = any(row.catalog == uc_catalog for row in catalogs_df.collect())
    if catalog_exists:
    #Take ownership of objects
        spark.sql(f"""ALTER CATALOG {uc_catalog} OWNER TO `{admin_user}`;""")
        spark.sql(f"""ALTER SCHEMA {uc_catalog}.{uc_database} OWNER TO `{admin_user}`;""")

   

    spark.sql(f"""DROP CATALOG IF EXISTS {uc_catalog} CASCADE """)
    spark.sql(f"""DROP SCHEMA IF EXISTS {uc_catalog}.{uc_database} CASCADE """)

    spark.sql(f"""CREATE CATALOG IF NOT EXISTS {uc_catalog} MANAGED LOCATION '{ext_loc}'""")
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {uc_catalog}.{uc_database} """)

    #Volumes
    volume =  f"volume_{user_name}"
    volume_path = "s3://"+spark.conf.get("da.workshop_bucket") +"/volume_"+user_name+"/"
    spark.sql(f"""CREATE EXTERNAL VOLUME IF NOT EXISTS {uc_catalog}.{uc_database}.{volume} LOCATION '{volume_path}' COMMENT 'This is volume for {user_name}'""")
    spark.sql(f"""ALTER VOLUME {uc_catalog}.{uc_database}.{volume} OWNER TO `{user_email}`;""")

    #Permissions
    spark.sql(f"""ALTER CATALOG {uc_catalog} OWNER TO `{user_email}`;""")
    spark.sql(f"""ALTER SCHEMA {uc_catalog}.{uc_database} OWNER TO `{user_email}`;""")

    


# COMMAND ----------

# DBTITLE 1,Assign permissions

