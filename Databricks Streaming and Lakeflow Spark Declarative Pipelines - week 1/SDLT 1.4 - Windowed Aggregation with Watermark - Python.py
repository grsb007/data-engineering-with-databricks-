# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

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
# MAGIC - <a href="https://spark.apache.org/docs/3.5.7/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch" target="_blank">foreachbatch</a>
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

# MAGIC %md
# MAGIC
# MAGIC ## Build Streaming DataFrames
# MAGIC
# MAGIC Obtain an initial streaming DataFrame from a Delta-format file source.

# COMMAND ----------

from pyspark.sql.functions import window, sum, col
from pyspark.sql.types import TimestampType

parsed_df = (spark.readStream
                    .load('/Volumes/dbacademy_ecommerce/v01/delta/events_hist')
                    .withColumn("event_timestamp", (col("event_timestamp") / 1e6).cast("timestamp"))
                    .withColumn("event_previous_timestamp", (col("event_previous_timestamp") / 1e6).cast("timestamp"))

                    # filter out zero revenue events
                    .filter("ecommerce.purchase_revenue_in_usd IS NOT NULL AND ecommerce.purchase_revenue_in_usd != 0")
)

# COMMAND ----------

display(parsed_df)

# COMMAND ----------

# now add up revenues by city by 60 minute time window
windowed_df = (parsed_df
                     # The watermark sets up the timing window. The watermark says "I'll accept events up to 90 minutes late." So if a window closes at 3:00 PM, Spark will still accept events timestamped as late as 4:30 PM before finalizing results.
                    .withWatermark(eventTime="event_timestamp", delayThreshold="90 minutes")
                    # group by city by hour
                    # For each 60-minute window and city combination, Spark groups matching records
                    .groupBy(window(timeColumn="event_timestamp", windowDuration="60 minutes"), "geo.city")
                    .agg(sum("ecommerce.purchase_revenue_in_usd").alias("total_revenue"))

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

# MAGIC %sql
# MAGIC SELECT * FROM revenue_by_city_by_hour_append

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY revenue_by_city_by_hour_append

# COMMAND ----------

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
# upserts (updates or inserts) data into a table 
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
    WHEN MATCHED THEN UPDATE SET * --If a matching row exists (meaning you've already seen this city/time window combo before), replace all its values with the new incoming data. The * means "all columns."
    WHEN NOT MATCHED THEN INSERT * --If no matching row exists (this is the first time seeing this city/time window combo), insert the entire row as a new record.
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
  s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>