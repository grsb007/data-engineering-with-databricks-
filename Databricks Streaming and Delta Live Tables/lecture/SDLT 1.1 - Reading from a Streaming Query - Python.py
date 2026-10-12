# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading from a Streaming Query

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objectives
# MAGIC 1. Build a streaming DataFrame
# MAGIC 1. Display streaming query results
# MAGIC 1. Write streaming query results to table
# MAGIC 1. Monitor the streaming query

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

# MAGIC %md
# MAGIC
# MAGIC ### Classes referenced
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html" target="_blank">StreamingQuery</a>

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-01

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prototyping in Batch Mode
# MAGIC
# MAGIC Explore the dataset and test out transformation logic using batch dataframes.

# COMMAND ----------

print(DA.paths.events)

# COMMAND ----------

spark.read .load(DA.paths.events).display()

# COMMAND ----------

from pyspark.sql.functions import col, approx_count_distinct, count

batch_df = (spark.read  
              .load(DA.paths.events)
              .filter(col("traffic_source") == "email")
              .withColumn("mobile", col("device").isin(["iOS", "Android"]))
              .select("user_id", "event_timestamp", "mobile")
           )

print(f"Is data stream : {batch_df.isStreaming}")

display(batch_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Build streaming DataFrames
# MAGIC
# MAGIC Switching from batch to stream is easy! 
# MAGIC
# MAGIC Change the code from `spark.read` to `spark.readStream` - everything else, including the transformation logic remain unchanged.

# COMMAND ----------

from pyspark.sql.functions import col, approx_count_distinct, count
 
streaming_df = (spark
                .readStream
                .load(DA.paths.events)
                .filter(col("traffic_source") == "email")
                .withColumn("mobile", col("device").isin(["iOS", "Android"]))
                .select("user_id", "event_timestamp", "mobile")
            )

print(f"Is data stream : {streaming_df.isStreaming}")

display(streaming_df, streamName = "display_user_devices")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Write streaming query results
# MAGIC
# MAGIC Take the final streaming DataFrame (our result table) and write it to a Delta Table sink in `append` mode. In this labs setup, the table will be created in Unity Catalog.
# MAGIC
# MAGIC **NOTE:** `append` mode is the default mode when writing stateless queries to sink.

# COMMAND ----------

print(f"{DA.paths.working_dir}")

# COMMAND ----------

checkpoint_path = f"{DA.paths.working_dir}/email_traffic"

devices_query = (streaming_df
                  .writeStream
                  .outputMode("append")
                  .format("delta")
                  .queryName("email_traffic")
                  .trigger(processingTime="1 second")
                  .option("checkpointLocation", checkpoint_path)
                  .toTable("email_traffic_only")
                )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitor streaming query
# MAGIC
# MAGIC Use the streaming query handle to monitor and control it.

# COMMAND ----------

devices_query.name

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Let's see the query status.

# COMMAND ----------

devices_query.status

# COMMAND ----------

devices_query.status

# COMMAND ----------

devices_query.status

# COMMAND ----------

devices_query.status

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC [lastProgress](https://spark.apache.org/docs/3.5.2/structured-streaming-programming-guide.html#managing-streaming-queries) gives us metrics from the previous query

# COMMAND ----------

devices_query.lastProgress

# COMMAND ----------

import time
# Run for 10 more seconds
time.sleep(10) 

devices_query.stop()

# COMMAND ----------

devices_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC [awaitTermination](https://spark.apache.org/docs/3.5.2/structured-streaming-programming-guide.html#managing-streaming-queries) 
# MAGIC  blocks the current thread until the streaming query is terminated.
# MAGIC
# MAGIC For stand-alone structured streaming applications, this is used to prevent the main thread from terminating while the streaming query is still executing. In the context of this training environment, it's useful in case you use **Run all** to run the notebook. This prevents subsequent command cells from executing until the streaming query has fully terminated.

# COMMAND ----------

# Stop the streaming query with the query's name or query's Id
result = stop_streaming_query("email_traffic")
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>

# COMMAND ----------

