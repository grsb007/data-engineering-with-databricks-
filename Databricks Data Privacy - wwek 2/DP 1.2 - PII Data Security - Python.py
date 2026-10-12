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
# MAGIC # PII Data Security
# MAGIC
# MAGIC In this demo you will learn how to:
# MAGIC
# MAGIC * How to handle PII Data Security with **Pseudonymization and Anonymization**
# MAGIC
# MAGIC Further, you will also learn:
# MAGIC * Generate and trigger a Lakeflow Spark Declarative Pipeline that manages both processes
# MAGIC * Explore the resultant DAG
# MAGIC * Land a new batch of data

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
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course. It will also set your default catalog to your unique catalog name and the schema to your specific schema name shown below using the `USE` statements.
# MAGIC <br></br>
# MAGIC
# MAGIC
# MAGIC ```
# MAGIC USE CATALOG your-catalog;
# MAGIC USE SCHEMA your-catalog.pii_data;
# MAGIC ```
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-1.2

# COMMAND ----------

# MAGIC %md
# MAGIC Run the code below to view your current default catalog and schema. Confirm that they have the same name as the cell above.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## B. Generate and Trigger the Lakeflow Spark Declarative Pipeline
# MAGIC Run the cell below to auto-generate your Lakeflow Spark Declarative Pipeline using the provided configuration values.
# MAGIC
# MAGIC After creation, the pipeline will run. The initial run will take a few minutes while a cluster is provisioned.
# MAGIC
# MAGIC **NOTE:** The `DeclarativePipelineCreator` class is a custom class we use to setup the Lakeflow Spark Declarative Pipeline. The class is built using the Databricks SDK and RESTAPI calls.

# COMMAND ----------

demo_pipeline = DeclarativePipelineCreator(
    pipeline_name=f"1.2_PII_Data_Security_{DA.catalog_name}", 
    catalog_name=DA.catalog_name,
    schema_name="pii_data",
    root_path_folder_name='Pipeline',
    source_folder_names=[
        'DP 1.2.1 - Pseudonymized PII Lookup Table',
        'DP 1.2.2 - Anonymized Users Age'
    ],
    configuration={
        'user_reg_source':f'/Volumes/{DA.catalog_name}/pii_data/pii/stream_source/user_reg',
        'daily_user_events_source':f"/Volumes/{DA.catalog_name}/pii_data/pii/stream_source/daily",
        'lookup_catalog': DA.catalog_name
        },
    serverless=True,
    channel='CURRENT',
    delete_pipeine_if_exists = True
)

demo_pipeline.create_pipeline()

demo_pipeline.start_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Lakeflow Spark Declarative Pipeline Overview
# MAGIC
# MAGIC This Lakeflow Spark Declarative Pipeline is based in two notebooks located in the "Pipeline" folder:
# MAGIC
# MAGIC - [DP 1.2.1 - Pseudonymized PII Lookup Table]($./Pipeline/DP 1.2.1 - Pseudonymized PII Lookup Table): Provides an overview of how to ingest and stream **registered_user_data** to apply two **Pseudonymization** techniques such as:
# MAGIC   - Hashing
# MAGIC   - Tokenization
# MAGIC
# MAGIC - [DP 1.2.2 - Anonymized Users Age]($./Pipeline/DP 1.2.2 - Anonymized Users Age): Provides an overview of how to ingest and stream **user_events_raw** data into a **users_bronze** and apply **Binning Anonymization** on User's Ages into a materialized view **user_bins**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Open the Lakeflow Spark Declarative Pipeline
# MAGIC
# MAGIC In the left navigation bar, complete the following to open your Lakeflow Spark Declarative Pipeline:
# MAGIC
# MAGIC 1. Right-click on Jobs and Pipelines and select *Open in New Tab*.
# MAGIC
# MAGIC 2. Find and select your pipeline named **1.2_PII_Data_Security_labuser-name**.
# MAGIC
# MAGIC 3. Leave the pipeline page open and continue to the next steps.
# MAGIC
# MAGIC 4. Once the pipeline completes, here is the graphed execution flow:
# MAGIC
# MAGIC ![ddemo01_2_full_pipeline.png](./Includes/images/demo01_2_full_pipeline.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Pseudonymization
# MAGIC
# MAGIC As a recap:
# MAGIC
# MAGIC - Switches original data point with pseudonym for later re-identification
# MAGIC - Only authorized users will have access to keys/hash/table for re-identification
# MAGIC - Protects datasets on record level for machine learning
# MAGIC - A pseudonym is still considered to be personal data according to the GDPR
# MAGIC Two main pseudonymization methods: hashing and tokenization
# MAGIC
# MAGIC
# MAGIC [DP 1.2.1 - Pseudonymized PII Lookup Table]($./Pipeline/DP 1.2.1 - Pseudonymized PII Lookup Table): Provides an overview of how to ingest and stream **registered_users** to apply two **Pseudonymization** techniques such as:
# MAGIC   1. Creates the **registered_users** table from the source JSON files with PII.
# MAGIC
# MAGIC   1. Hashing: Handled in table **user_lookup_hashed**
# MAGIC
# MAGIC   1. Tokenization: Handled in tables **registered_users_tokens** and **user_lookup_tokenized**
# MAGIC
# MAGIC
# MAGIC #### Pseudonymization section in DAG
# MAGIC
# MAGIC ![demo01_2_pii_data_security_pseudo_dag.png](./Includes/images/demo01_2_pii_data_security_pseudo_dag.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Preview the registered_users Table
# MAGIC
# MAGIC The table **registered_users** will be our source for the ingested users, where we'll apply *Pseudonymization* and *Anonymization*. 
# MAGIC
# MAGIC Run the cell and view the original source data. Notice that no data has been anonymized.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     user_id,
# MAGIC     device_id,
# MAGIC     mac_address
# MAGIC FROM registered_users 
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### C2. Option 1 - Hashing
# MAGIC
# MAGIC Objectives:
# MAGIC
# MAGIC - Apply SHA or other hashes to all PII.
# MAGIC - Add a random string "salt" to values before hashing.
# MAGIC - Databricks secrets can be leveraged for obfuscating the salt value.
# MAGIC - This leads to a slight increase in data size.
# MAGIC - Some operations may be less efficient.
# MAGIC
# MAGIC In our pipeline, we leverage the **registered_users** table and apply hashing to the **user_id** column using a salt value of *BEANS*, creating a column **alt_id** in the **user_lookup_hashed** table.
# MAGIC
# MAGIC See the cell below for the results and compare both the **user_id** and **alt_id** columns.
# MAGIC
# MAGIC **NOTE:** The **user_id** column should be removed after processing. It is kept for demo purposes.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   alt_id,
# MAGIC   user_id,
# MAGIC   device_id,
# MAGIC   mac_address 
# MAGIC FROM user_lookup_hashed

# COMMAND ----------

# MAGIC %md
# MAGIC ### C3. Option 2 - Tokenization
# MAGIC
# MAGIC **Tokenization** objectives:
# MAGIC
# MAGIC - Converts all PII to keys.
# MAGIC - Values are stored in a secure lookup table.
# MAGIC - Slow to write, but fast to read.
# MAGIC - De-identified data is stored in fewer bytes.
# MAGIC
# MAGIC Similar to the previous step, our pipeline leverages the **registered_users** table. This time, the pipeline creates a new table called **registered_users_tokens** to store the relationship between the generated token (using the [uuid function](https://docs.databricks.com/en/sql/language-manual/functions/uuid.html)) and the **user_id** column.
# MAGIC
# MAGIC See the token column generated for each **user_id** in the **registered_users_tokens** table below.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM registered_users_tokens

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can use and leverage the table **registered_users_tokens** and create a new lookup table with tokenized **user_id** column, held in **user_lookup_tokenized** table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   alt_id as Tokenized,
# MAGIC   device_id,
# MAGIC   mac_address, 
# MAGIC   registration_timestamp 
# MAGIC FROM user_lookup_tokenized

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Anonymization
# MAGIC
# MAGIC As a recap:
# MAGIC
# MAGIC - Protects entire dataset (tables, databases or entire data catalogues) mostly for Business Intelligence
# MAGIC - Personal data is irreversibly altered in such a way that a data subject can no longer be identified directly or indirectly
# MAGIC - Usually a combination of more than one technique used in real-world scenarios
# MAGIC - Two main anonymization methods: data suppression and generalization
# MAGIC
# MAGIC
# MAGIC [DP 1.2.2 - Anonymized Users Age]($./Pipeline/DP 1.2.2 - Anonymized Users Age): Provides an overview of how to ingest and stream **user_events_raw** data into a **users_bronze** and apply **Binning Anonymization** on User's Ages into a materialized view **user_age_bins**.
# MAGIC
# MAGIC #### Anonymization section in DAG
# MAGIC
# MAGIC ![demo01_2_anonymization_dag.png](./Includes/images/demo01_2_anonymization_dag.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Explore the Date Lookup and User Events Raw tables
# MAGIC
# MAGIC - The **date_lookup** table is used for the **date** and **week_part** association. It is joined with the **user_events_raw** data to identify which **week_part** the **Date of Birth (DOB)** belongs to. 
# MAGIC   - For example: (date) 2020-07-02 = (week_part) 2020-27.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM date_lookup
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC - The **user_events_raw** table represents the ingested user event data in JSON format, which is later unpacked and filtered to retrieve only user information.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   string(key), 
# MAGIC   string(value)
# MAGIC FROM user_events_raw
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Users Bronze
# MAGIC
# MAGIC The table **users_bronze** is our focus and will be our source for the ingested user information, where we'll apply **Binning Anonymization** to the **Date of Birth (DOB)**.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   user_id,
# MAGIC   dob,
# MAGIC   gender,
# MAGIC   city,
# MAGIC   state 
# MAGIC FROM users_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### D3. User Age Bins
# MAGIC
# MAGIC The table **user_age_bins** shows the results of the **binning anonymization** applied, check **age** column and the range provide for each user.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM user_age_bins

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Land New Data and Trigger the Pipeline
# MAGIC
# MAGIC Run the cell below to land more data in the source directory, then navigate to the Pipelines UI and manually trigger a pipeline update.
# MAGIC
# MAGIC As we continue through the course, you can return to this notebook and use the method provided below to land new data. Running this entire notebook again will delete the underlying data files for both the source data and your Lakeflow Spark Declarative Pipeline and enable you to start over.

# COMMAND ----------

## Load files into (your catalog -> pii_data -> volumes -> pii -> stream_source -> daily)
DA.load(copy_from=DA.paths.stream_source.daily_working_dir,
        copy_to=DA.paths.stream_source.daily,
        n=4)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>

# COMMAND ----------

