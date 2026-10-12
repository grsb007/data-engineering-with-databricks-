# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Generation
# MAGIC
# MAGIC This notebook will set up prerequisite tables and perform the `CLUSTER BY` operation. It is recommended that you use a large cluster with a minimum of 4 workers to run this notebook in a shorter amount of time.

# COMMAND ----------

src_catalog = 'dbacademy_flightdata'
src_schema = 'v01'
target_catalog = 'dbacademy_airline'
target_schema = 'flightdata'

# COMMAND ----------

spark.sql(f'CREATE CATALOG IF NOT EXISTS {target_catalog}')
spark.sql(f"GRANT USE CATALOG, USE SCHEMA, SELECT, BROWSE ON CATALOG {target_catalog} TO `account users`")
spark.sql(f'USE CATALOG {target_catalog}')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {target_schema}')
spark.sql(f'USE SCHEMA {target_schema}')

# COMMAND ----------

df = spark.read.csv(f'/Volumes/{src_catalog}/{src_schema}/airlines/part-00000', inferSchema=True, header=True)

# COMMAND ----------

from pyspark.sql.functions import *

try:
    spark.table('flights')
except:
    (spark
     .read
     .csv(f'/Volumes/{src_catalog}/{src_schema}/airlines/', schema=df.schema, header=True)
     .withColumn('id', monotonically_increasing_id())
     .select(
        'id',
        'year',
        'FlightNum',
        'ArrDelay',
        'UniqueCarrier',
        'TailNum')
    .write
    .mode("overwrite")
    .saveAsTable('flights'))

    spark.sql('OPTIMIZE flights ZORDER BY FlightNum')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS flights_cluster_id
# MAGIC   CLUSTER BY (id)
# MAGIC AS 
# MAGIC SELECT * 
# MAGIC FROM flights

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS flights_cluster_id_flightnum
# MAGIC   CLUSTER BY (id, FlightNum)
# MAGIC AS 
# MAGIC SELECT * 
# MAGIC FROM flights

# COMMAND ----------

try:
    spark.table('flights_small')
except:
    (spark
     .read
     .csv(f'/Volumes/{src_catalog}/{src_schema}/airlines/part-000*', schema=df.schema, header=True)
     .filter('year = 1998 or year = 1999')
     .write
     .mode("overwrite")
     .saveAsTable('flights_small')
)