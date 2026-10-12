# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Streaming Query Lab
# MAGIC ### Coupon Sales
# MAGIC
# MAGIC Process and append streaming data on transactions using coupons.
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Read data stream
# MAGIC 2. Filter for transactions with coupons codes
# MAGIC 3. Write streaming query results to Delta
# MAGIC 4. Monitor streaming query
# MAGIC 5. Stop streaming query
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

# MAGIC %run ./Includes/Classroom-Setup-02L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1. Read data stream
# MAGIC
# MAGIC Assign the resulting DataFrame to **`df`**:
# MAGIC - Read from Delta files in the source directory specified by **`DA.paths.sales`**
# MAGIC - Set to process 1 file per trigger
# MAGIC
# MAGIC

# COMMAND ----------

print(DA.paths.sales)

# COMMAND ----------

df = (
    spark
    .readStream
    .format("delta")
    .option("maxFilesPerTrigger", 1)
    .load(DA.paths.sales)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **1.1: CHECK YOUR WORK**

# COMMAND ----------

# Define the list of required columns
sales_required_columns = [
    'order_id', 'email', 'transaction_timestamp',
    'total_item_quantity', 'purchase_revenue_in_usd',
    'unique_items', 'items'
]

# COMMAND ----------

DA.validate_dataframe(df,sales_required_columns)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Filter for transactions with coupon codes
# MAGIC - Explode the **`items`** field in **`df`** with the results replacing the existing **`items`** field
# MAGIC - Filter for records where **`items.coupon`** is not null
# MAGIC
# MAGIC Assign the resulting DataFrame to **`coupon_sales_df`**.

# COMMAND ----------

from pyspark.sql.functions import col, explode
coupon_sales_df = (
    df
    .withColumn("items", explode(col("items")))
    .filter(col("items.coupon").isNotNull())
)
coupon_sales_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **2.1: CHECK YOUR WORK**

# COMMAND ----------

# Expected schema fields and types

expected_fields = {
    "order_id": "LongType",
    "email": "StringType",
    "transaction_timestamp": "LongType",
    "total_item_quantity": "LongType",
    "purchase_revenue_in_usd": "DoubleType",
    "unique_items": "LongType",
    "items": "StructType"
}

# COMMAND ----------

DA.validate_schema(coupon_sales_df.schema,expected_fields)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 3. Write streaming query results to Delta
# MAGIC - Configure the streaming query to write Delta format files in "append" mode
# MAGIC - Set the query name to "coupon_sales"
# MAGIC - Set a trigger interval of 1 second
# MAGIC - Set the checkpoint location to **`coupons_checkpoint_path`**
# MAGIC - Set the output path to **`coupons_output_path`**
# MAGIC
# MAGIC Start the streaming query and assign the resulting handle to **`coupon_sales_query`**.

# COMMAND ----------

coupons_checkpoint_path = f"{DA.paths.working_dir}/coupon-sales"
coupons_output_path = f"{DA.paths.working_dir}/coupon-sales/output"

coupon_sales_query = (coupon_sales_df
                      .writeStream
                      .outputMode("append")
                      .format("delta")
                      .queryName("coupon_sales")
                      .trigger(availableNow=True)
                      .option("checkpointLocation", coupons_checkpoint_path)
                      .start(coupons_output_path))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **3.1: CHECK YOUR WORK**
# MAGIC
# MAGIC Note: Please wait for stream to get started before validating

# COMMAND ----------

DA.validate_coupon_sales_query(coupon_sales_query,coupons_checkpoint_path,coupons_output_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Monitor streaming query
# MAGIC - Get the ID of streaming query and store it in **`queryID`**
# MAGIC - Get the status of streaming query and store it in **`queryStatus`**

# COMMAND ----------

query_id = coupon_sales_query.id
query_id

# COMMAND ----------

query_status = coupon_sales_query.status
query_status

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **4.1: CHECK YOUR WORK**

# COMMAND ----------

DA.validate_query_status(query_status)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 5. Stop streaming query
# MAGIC - Stop the streaming query

# COMMAND ----------

coupon_sales_query.stop()
coupon_sales_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC **5.1: CHECK YOUR WORK**

# COMMAND ----------

DA.validate_query_state(coupon_sales_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Verify the records were written in Delta format

# COMMAND ----------

display(spark.read.format("delta").load(coupons_output_path))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>

# COMMAND ----------

