# Databricks Unity Catalog - Hands On Lab
Interactive Workshop to allow customers to get hands on with Unity Catalog in a safe space


Labs:

ACL

Migration

Slides on : Managed vs External
    
Discovery Tags

RLS/CLM

Lineage

Volumes

Delta Sharing

Lakehouse Federation

System Tables

Marketplace


Slides : before each session : Abhishek
Lab 1 - Upgrade (1 h 30 m)
Infra ,Workspaces,Storage,AL: Al
1 WS vs Multiple-1 WS
1 notebook which will create catalog/volumes etc for the user - Artifact - notebook :Al
User runs the SYNC command -need to check multiple destination on same source possible ?: Abhishek
User checks their table and data
Grants/play with the permission : Al
Clone the job notebook and make changes as demosntrated
Use the pre created simgle user clusters to run the upgraded notebooks/job -Artifact : cluster: Al
View the lineage
-------------
Lab 2 - Row and column access controls (30-45 mins)
Use DB demos
James
-------------
Lab 3 - Lineage and tags (30 mins)
Use DB demos
Abhishek
------------ (45 mins)
Lab 4 - Federation
Database setup by us
Users create connection and create foreign catalogs
Apply persissions
Al
--------------- (30 mins)----
Demo :System Tables
Demo : Delta sharing
----------------(30  mins)----
Abhi,James,Al
Questions
FInal slides
-------------
UC Upgrade steps
-----------------

1. Add instanceprofile to sql warehouse.Why ? To enable users to see sampledata in hive tables seamlessly

2. Copy mount folder in old s3 to new s3 ToDO: Move mount to a secure location

3. Create mount from an instance profile attached cluster.Why we need it- we need it for raw data ingestion demonstration 

aws_bucket_name = "<<new bucketname>>"
mount_name = "s3"
dbutils.fs.mount(f"s3a://{aws_bucket_name}", f"/mnt/{mount_name}")
display(dbutils.fs.ls(f"/mnt/{mount_name}"))

output:
dbfs:/mnt/raw/yellow_tripdata_2019-12.csv

4.Create non UC cluster with instance profile

5.Create workflow with NYtaxiJObNotebookOld from Repo with cluster in (4)



6. Create default volume on the path and register files

create catalog if not exists raw managed location 's3://<<bucketname>>/catstoragedefault';

create schema if not exists raw.nyctaxis;

CREATE EXTERNAL VOLUME if not exists raw.nyctaxis.volratecode
    LOCATION 's3://<<bucketname>>/mount/ratecode'
    COMMENT 'This is raw volume';

CREATE EXTERNAL VOLUME if not exists raw.nyctaxis.voltripdata
    LOCATION 's3://<<bucketname>>/mount/tripdata'
    COMMENT 'This is raw volume';

grant READ VOLUME  on VOLUME `raw`.`nyctaxis`.`volratecode` to `account users`;
grant READ VOLUME  on VOLUME `raw`.`nyctaxis`.`voltripdata` to `account users`;

7.set spark.databricks.sql.initial.catalog.name "uc_catalog_"+<<username>> to UC clusters(To automate during cluster creation) and remove 'instance profile'

8. Add new notebooks for hms raw->silver->gold and run it to UC compatible cluster(step 7)

