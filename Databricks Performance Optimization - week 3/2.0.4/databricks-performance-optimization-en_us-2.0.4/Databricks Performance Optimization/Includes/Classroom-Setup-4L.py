# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

# Set the default configurations 
print('Set the spark.sql.autoBroadcastJoinThreshold and spark.databricks.adaptive.autoBroadcastJoinThreshold configs to the defaults.')
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
spark.conf.unset("spark.databricks.adaptive.autoBroadcastJoinThreshold")

## Create lab schema and disable predictive optimization to showcase exploding joins.
lab_schema_name = 'lab'
r = spark.sql(f'CREATE SCHEMA IF NOT EXISTS {DA.catalog_name}.{lab_schema_name}')
spark.sql(f'ALTER SCHEMA {DA.catalog_name}.{lab_schema_name} DISABLE PREDICTIVE OPTIMIZATION')

r = spark.sql(f'USE CATALOG {DA.catalog_name}')
r = spark.sql(f'USE SCHEMA {lab_schema_name}')

## Display the course catalog and schema name for the user.
DA.display_config_values(
  [('Course Catalog',DA.catalog_name),
   ('Your Schema',lab_schema_name)
   ]
)