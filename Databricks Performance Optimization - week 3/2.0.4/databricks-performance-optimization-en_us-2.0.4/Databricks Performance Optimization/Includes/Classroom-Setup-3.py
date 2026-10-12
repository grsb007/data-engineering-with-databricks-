# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()


# Set the default configurations 
print('Set the spark.sql.autoBroadcastJoinThreshold and spark.databricks.adaptive.autoBroadcastJoinThreshold configs to the defaults.')
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
spark.conf.unset("spark.databricks.adaptive.autoBroadcastJoinThreshold")

r = spark.sql(f'USE CATALOG {DA.catalog_name}')
r = spark.sql(f'USE SCHEMA default')

## Display the course catalog and schema name for the user.
DA.display_config_values(
  [('Course Catalog', DA.catalog_name),
   ('Your Schema', 'default')
   ]
)