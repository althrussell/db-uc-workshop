# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Sharing: Easily Share & distribute Data as a Product   <img src="https://i.ibb.co/vdNHFZN/deltasharingimage.png" width = "100"></a>
# MAGIC
# MAGIC With Delta Sharing, organizations can better govern, package, and share their data to both <b>Internal</b> and <b>External Customers.</b>
# MAGIC
# MAGIC Delta Sharing is an Open protocol, meaning that any recipient can access the data regardless of their data platform, without any locking. 
# MAGIC
# MAGIC This allow you to extend your reach and share data with any organization.
# MAGIC
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&aip=1&t=event&ec=dbdemos&ea=VIEW&dp=%2F_dbdemos%2Fgovernance%2Fdelta-sharing-airlines%2F01-Delta-Sharing-presentation&cid=984752964297111&uid=6480688324232166">

# COMMAND ----------

#Hide this code during presentation
displayHTML(f'''<div style="width:1150px; margin:auto"><iframe src="https://docs.google.com/presentation/d/131A0KwpOO7vX3f5Z3KRoHWo_Aq3GyG0POpUa2iBb1hM/embed?slide=2" frameborder="0" width="1150" height="683"></iframe></div>''') #hide_this_code

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 1/ Sharing data as a Provider
# MAGIC
# MAGIC In this [first notebook: 02-provider-delta-sharing-demo]($./02-provider-delta-sharing-demo), we'll discover how a Data Provider can securely share data with external organization.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 2/ Accessing Delta Share data as a Receiver
# MAGIC
# MAGIC Now that our data is shared, discover in the [second notebook 03-receiver-delta-sharing-demo]($./03-receiver-delta-sharing-demo) how a recipient can access the data from any system.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## 3/ Sharing data betwee Databricks customers with Unity Catalog
# MAGIC
# MAGIC If the organization receiving the data also has a Lakehouse with Databricks, sharing data between entities is even simpler.
# MAGIC
# MAGIC Open the [last notebook 04-share-data-within-databricks]($./04-share-data-within-databricks) to discover how this is done.
