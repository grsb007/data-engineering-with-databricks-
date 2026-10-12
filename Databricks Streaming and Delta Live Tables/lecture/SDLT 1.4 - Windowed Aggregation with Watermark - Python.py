# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-610f09e1-5f74-4b55-be94-2879bd22bbe9
# MAGIC %md
# MAGIC # Windowed Aggregation with Watermark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objectives
# MAGIC 1. Build streaming DataFrames with time window aggregates with watermark
# MAGIC 1. Write streaming query results to Delta table using `update` mode and `forEachBatch()`
# MAGIC 1. Monitor the streaming query

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html" target="_blank">StreamingQuery</a>
# MAGIC - <a href="https://spark.apache.org/docs/3.5.2/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch" target="_blank">foreachbatch</a>
# MAGIC - <a href="https://docs.databricks.com/en/structured-streaming/delta-lake.html#upsert-from-streaming-queries-using-foreachbatch&language-python" target="_blank">update mode with foreachbatch</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC     - In the drop-down, select **More**.
# MAGIC
# MAGIC     - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 1. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04

# COMMAND ----------

# DBTITLE 0,--i18n-e06055f0-b40e-4997-b656-bf8cf94156a0
# MAGIC %md
# MAGIC
# MAGIC ## Build Streaming DataFrames
# MAGIC
# MAGIC Obtain an initial streaming DataFrame from a Delta-format file source.

# COMMAND ----------

print(DA.paths.events)

# COMMAND ----------

(spark.read
                    .load(DA.paths.events)
                    .withColumn("event_timestamp", (col("event_timestamp") / 1e6).cast("timestamp"))
                    .withColumn("event_previous_timestamp", (col("event_previous_timestamp") / 1e6).cast("timestamp"))

                    # filter out zero revenue events
                    .filter("ecommerce.purchase_revenue_in_usd IS NOT NULL AND ecommerce.purchase_revenue_in_usd != 0")
                    .limit(100)
                    ).display()

# COMMAND ----------


from pyspark.sql.functions import window, sum, col
from pyspark.sql.types import TimestampType

parsed_df = (spark.readStream
                    .load(DA.paths.events)
                    .withColumn("event_timestamp", (col("event_timestamp") / 1e6).cast("timestamp"))
                    .withColumn("event_previous_timestamp", (col("event_previous_timestamp") / 1e6).cast("timestamp"))

                    # filter out zero revenue events
                    .filter("ecommerce.purchase_revenue_in_usd IS NOT NULL AND ecommerce.purchase_revenue_in_usd != 0")
)

# COMMAND ----------

display(parsed_df)

# COMMAND ----------

from pyspark.sql.functions import expr
windowed_df = (parsed_df
                    # now add up revenues by city by 60 minute time window
                    .withWatermark(eventTime="event_timestamp", delayThreshold="90 minutes")
                    # group by city by hour
                    .groupBy(window(timeColumn="event_timestamp", windowDuration="60 minutes"), "geo.city")
                    .agg(sum("ecommerce.purchase_revenue_in_usd").alias("total_revenue"))
                    .withColumn("event_timestamp_with_watermak", expr("window.start + interval 90 minutes"))

)

# COMMAND ----------

display(windowed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write streaming results
# MAGIC
# MAGIC Let's explore a couple options for writing the results.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write streaming results in `append` mode (option 1)
# MAGIC
# MAGIC In the below example, the sink table is appended with new rows from results table on each trigger.
# MAGIC Think about how append mode writing data to sink. What will happen to records of those city whose hourly revenue got updated due to late arrival data. Would it be updated into the sink with "append" mode?

# COMMAND ----------

print(f"{DA.paths.working_dir}")

# COMMAND ----------

checkpoint_path = f"{DA.paths.working_dir}/query_revenue_by_city_by_hour_append"
# Write the output of a streaming aggregation query into Delta table as updates.The implication of append output modes in the context of window aggregation and watermarks is that an aggregate can be produced only once and can not be updated. Therefore, once the aggregate is produced, the engine can delete the aggregate's state and thus keep the overall aggregation state bounded.
windowed_query = (windowed_df.writeStream
                  .queryName("query_revenue_by_city_by_hour_append")
                  .option("checkpointLocation", checkpoint_path)
                  .trigger(availableNow=True)
                  .outputMode("append")
                  .table("revenue_by_city_by_hour_append")
                )

# COMMAND ----------

windowed_query.name

# COMMAND ----------

windowed_query.status

# COMMAND ----------

windowed_query.lastProgress

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM revenue_by_city_by_hour_append

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY revenue_by_city_by_hour_append

# COMMAND ----------

# DBTITLE 0,--i18n-5a10eead-7c3b-41e6-bcbe-0d85095de1d7
# MAGIC %md
# MAGIC
# MAGIC ### Write streaming query results in `update` mode (option 2)
# MAGIC
# MAGIC Take the final streaming DataFrame (our result table) and write it to a Delta Table sink in `update` mode. This approach gives much greater control to the developer when it comes to updating the sink, albeit with greater complexity.
# MAGIC
# MAGIC **NOTE:** The syntax for Writing streaming results to a Delta table or dataset in `update` mode is a little different. It requires use of the `MERGE` command within a `forEachBatch()` function call. This also requires the target table to be pre-created.
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType, TimestampType

# Here we are creating the table with the same schema as the incoming dataframe
schema = StructType([StructField('window',StructType([StructField('start', TimestampType(), True), 
                                                      StructField('end', TimestampType(), True)]), False), 
                     StructField('city', StringType(), True), 
                     StructField('total_revenue', DoubleType(), True)])

empty_df = spark.createDataFrame([], schema=schema)

empty_df.write.saveAsTable(name="revenue_by_city_by_hour", mode='overwrite')

# COMMAND ----------

# Function to upsert microBatchOutputDF into Delta table using merge

def upsertToDelta(microBatchOutputDF, batchId):
  # Set the dataframe to view name
  microBatchOutputDF.createOrReplaceTempView("updates")
  # IMP: You have to use the SparkSession that has been used to define the `updates` dataframe

  # In Databricks Runtime 10.5 and below, you must use the following:
  # microBatchOutputDF._jdf.sparkSession().sql("""
  microBatchOutputDF.sparkSession.sql("""
    MERGE INTO revenue_by_city_by_hour t
    USING updates s
    ON t.window.start = s.window.start AND t.window.end = s.window.end AND t.city = s.city
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)

# COMMAND ----------

# MAGIC %md
# MAGIC Check the `revenue_by_city_by_hour` table before writing it to a Delta table sink in `UPDATE` mode.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM revenue_by_city_by_hour

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute below code to write result table into delta table using **update** mode
# MAGIC Dive deep into the query raw metrics and pay close attention to stateful operator section, see if you could identify **watermark** work in action and number of rows removed due to it's settings

# COMMAND ----------

print(f"{DA.paths.working_dir}/query_revenue_by_city_by_hour")

# COMMAND ----------

checkpoint_path = f"{DA.paths.working_dir}/query_revenue_by_city_by_hour"

# Write the output of a streaming aggregation query into Delta table as updates
windowed_query = (windowed_df.writeStream
                  .foreachBatch(upsertToDelta)
                  .outputMode("update")
                  .queryName("query_revenue_by_city_by_hour")
                  .option("checkpointLocation", checkpoint_path)
                  .trigger(availableNow=True)
                  .start()
                )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM revenue_by_city_by_hour

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY revenue_by_city_by_hour

# COMMAND ----------

for s in spark.streams.active:
  print(s.name)
  # s.stop()

# COMMAND ----------

for s in spark.streams.active:
  print(s.name)
  s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>