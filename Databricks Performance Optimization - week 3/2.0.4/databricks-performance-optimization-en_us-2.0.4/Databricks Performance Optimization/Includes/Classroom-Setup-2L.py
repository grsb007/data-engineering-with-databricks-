# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

r = spark.sql(f'USE CATALOG {DA.catalog_name}')
r = spark.sql(f'USE SCHEMA default')


## Display the course catalog and schema name for the user.
DA.display_config_values(
  [('Course Catalog', DA.catalog_name),
   ('Your Schema', 'default'),
   ('Airline Data Catalog', 'dbacademy_airline'),
   ('Airline Schema', 'flightdata')
   ]
)