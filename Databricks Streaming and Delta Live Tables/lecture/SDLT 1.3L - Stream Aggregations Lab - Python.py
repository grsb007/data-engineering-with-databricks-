# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Stream Aggregations Lab
# MAGIC
# MAGIC ### Activity by Traffic
# MAGIC
# MAGIC Process streaming data to display total active users by traffic source.
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Read data stream
# MAGIC 2. Get active users by traffic source
# MAGIC 3. Execute query with display() and plot results
# MAGIC 4. Execute the same streaming query with DataStreamWriter
# MAGIC 5. View results being updated in the query table
# MAGIC 6. List and stop all active streams
# MAGIC
# MAGIC ##### Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html" target="_blank">StreamingQuery</a>

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
# MAGIC ### Setup
# MAGIC Run the cells below to generate data and create the **`schema`** string needed for this lab.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-03L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1. Read data stream
# MAGIC - Set to process 1 file per trigger
# MAGIC - Read from Delta with filepath stored in **`DA.paths.events`**
# MAGIC
# MAGIC Assign the resulting Query to **`df`**.

# COMMAND ----------

events_df = (spark.readStream
           .option("maxFilesPerTrigger", 1)
           .format("delta")
           .load(DA.paths.events))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **1.1: CHECK YOUR WORK**

# COMMAND ----------

# Define the list of required columns

events_required_columns = ["device", "ecommerce", "event_name", "event_previous_timestamp", "event_timestamp", "geo", "items", "traffic_source", "user_first_touch_timestamp", "user_id"]

# COMMAND ----------

DA.validate_dataframe(events_df,events_required_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Get active users by traffic source
# MAGIC - Set default shuffle partitions to number of cores on your cluster (not required, but runs faster)
# MAGIC - Group by **`traffic_source`**
# MAGIC   - Aggregate the approximate count of distinct users and alias with "active_users"
# MAGIC - Sort by **`traffic_source`**

# COMMAND ----------

from pyspark.sql.functions import col, approx_count_distinct, count

spark.conf.set("spark.sql.shuffle.partitions", 200)

traffic_df = (events_df
              .groupBy("traffic_source")
              .agg(approx_count_distinct("user_id").alias("active_users"))
              .sort("traffic_source")
             )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **2.1: CHECK YOUR WORK**

# COMMAND ----------

# Expected schema fields and types

expected_fields = {
    "traffic_source": "StringType",
    "active_users": "LongType"
}

# COMMAND ----------

DA.validate_schema(traffic_df.schema,expected_fields)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 3. Execute query with display() and plot results
# MAGIC - Execute results for **`traffic_df`** using display()
# MAGIC - Plot the streaming query results as a bar graph

# COMMAND ----------

display(traffic_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **3.1: CHECK YOUR WORK**
# MAGIC - Your bar chart should plot **`traffic_source`** on the x-axis and **`active_users`** on the y-axis
# MAGIC - The top three traffic sources in descending order should be **`google`**, **`facebook`**, and **`instagram`**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Execute the same streaming query with DataStreamWriter
# MAGIC - Name the query "active_users_by_traffic"
# MAGIC - Set to "memory" format and "complete" output mode
# MAGIC - Set a trigger interval of 1 second

# COMMAND ----------

traffic_query = (traffic_df
                 .writeStream
                 .queryName("active_users_by_traffic")
                 .format("memory")
                 .outputMode("complete")
                 .trigger(processingTime="1 second")
                 .start())

# COMMAND ----------

# MAGIC %md
# MAGIC **4.1: CHECK YOUR WORK**

# COMMAND ----------

DA.validate_traffic_query(traffic_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. View results being updated in the query table
# MAGIC Run a query in a SQL cell to display the results from the **`active_users_by_traffic`** table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM active_users_by_traffic

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. List and stop all active streams
# MAGIC - Use SparkSession to get list of all active streams
# MAGIC - Iterate over the list and stop each query

# COMMAND ----------

for s in spark.streams.active:
    print(s.name)
    s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **6.1: CHECK YOUR WORK**

# COMMAND ----------

DA.validate_query_state(traffic_query)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>

# COMMAND ----------

