# Databricks notebook source
dbutils.widgets.text('reset_all_data', 'false')
dbutils.widgets.text('catalog', 'main')
dbutils.widgets.text('schema', 'billing_forecast')
catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')

# COMMAND ----------

# MAGIC %run ./00-global-setup $reset_all_data=$reset_all_data $catalog=$catalog $db=$schema
