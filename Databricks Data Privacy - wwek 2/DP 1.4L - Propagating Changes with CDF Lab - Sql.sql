-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img
-- MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
-- MAGIC     alt="Databricks Learning"
-- MAGIC   >
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC # Propagating Changes with CDF Lab
-- MAGIC
-- MAGIC We'll be using [Change Data Feed](https://docs.databricks.com/en/delta/delta-change-data-feed.html) to propagate changes to many tables from a single source.
-- MAGIC
-- MAGIC For this lab, we'll work with the fitness tracker datasets to propagate changes through a Lakehouse with Delta Lake Change Data Feed (CDF).
-- MAGIC
-- MAGIC Because the **user_lookup** table links identifying information between different pipelines, we'll make this the point where changes propagate from.
-- MAGIC
-- MAGIC
-- MAGIC ## Objectives
-- MAGIC By the end of this lab, you should be able to:
-- MAGIC - Enable Change Data Feed on a particular table
-- MAGIC - Read CDF output with Spark SQL or PySpark
-- MAGIC - Refactor ELT code to process CDF output

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
-- MAGIC
-- MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
-- MAGIC
-- MAGIC Follow these steps to select the classic compute cluster:
-- MAGIC
-- MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
-- MAGIC
-- MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
-- MAGIC
-- MAGIC     - In the drop-down, select **More**.
-- MAGIC
-- MAGIC     - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
-- MAGIC
-- MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
-- MAGIC
-- MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
-- MAGIC
-- MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
-- MAGIC
-- MAGIC 1. Wait a few minutes for the cluster to start.
-- MAGIC
-- MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A. Classroom Setup
-- MAGIC
-- MAGIC Run the following cell to configure your working environment for this course. It will also set your default catalog to your unique catalog name and the schema to your specific schema name shown below using the `USE` statements.
-- MAGIC <br></br>
-- MAGIC
-- MAGIC
-- MAGIC ```
-- MAGIC USE CATALOG your-catalog;
-- MAGIC USE SCHEMA your-catalog.pii_data;
-- MAGIC ```
-- MAGIC
-- MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-1.4L

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check your default catalog and schema. Notice the lab uses a new schema named **cdf_lab**.

-- COMMAND ----------

SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## B. Show available tables
-- MAGIC
-- MAGIC We'll focus on the **user_lookup** table with the intent to delete a user, check the CDF changes and then propagate such changes into the tables **users** and **users_bin**.
-- MAGIC
-- MAGIC Run the `SHOW TABLES` command and confirm the following tables are available:
-- MAGIC
-- MAGIC - **user_lookup**: Table containing user lookup information.
-- MAGIC - **users**: Table containing detailed user information.
-- MAGIC - **users_bin**: Table containing user bin assignments.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## C. Review the user_lookup table data
-- MAGIC
-- MAGIC Run the following query to view the **user_lookup** table. Notice that the table contains an **alt_id** column, as well as **device_id**, **mac_address**, and **user_id**.
-- MAGIC

-- COMMAND ----------

SELECT *
FROM user_lookup
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## D. Enable CDF for the user_lookup table
-- MAGIC
-- MAGIC Enable change data feed on the **user_lookup** table.
-- MAGIC
-- MAGIC **HINT:** Use the `ALTER TABLE` and `SET TBLPROPERTIES` to activate CDF. View the [Enable change data feed](https://docs.databricks.com/en/delta/delta-change-data-feed.html#enable-change-data-feed) documentation for more information.
-- MAGIC

-- COMMAND ----------

<FILL IN>

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC ALTER TABLE user_lookup                              -- Table Name
-- MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed=True)  -- property = True

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## E. Confirm CDF is enabled on user_lookup table
-- MAGIC
-- MAGIC **HINT**: Use the `DESCRIBE TABLE EXTENDED` statement to view information about the **user_lookup** table. Scroll to the bottom of the results and view the *Table Properties* row. Confirm that this table has `delta.enableChangeDataFeed` activated.
-- MAGIC
-- MAGIC

-- COMMAND ----------

DESCRIBE TABLE EXTENDED user_lookup;

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC DESCRIBE TABLE EXTENDED user_lookup;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## F. View user_lookup table History
-- MAGIC
-- MAGIC Enabling CDF generates a new entry in the table's history.
-- MAGIC
-- MAGIC
-- MAGIC View the history of the **user_lookup** table. Confirm the entry is in the history of the **user_lookup** table as version `1` and that the **operation** column has updated the *SET TBLPROPERTIES*. 
-- MAGIC

-- COMMAND ----------

DESCRIBE HISTORY user_lookup;

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC DESCRIBE HISTORY user_lookup;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## G. Delete a record from user_lookup table
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### G1. Query and pick a record from the User Lookup table
-- MAGIC
-- MAGIC Run the query below and pick a **user_id** value to use in the next section. Notice that there are 100 records in the **user_lookup** table.

-- COMMAND ----------

SELECT * 
FROM user_lookup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### G2. Delete a record from User Lookup table
-- MAGIC
-- MAGIC To delete record from the table pick a value from the **user_id** column from above to delete. Then complete the following:
-- MAGIC
-- MAGIC - Place the picked **user_id** value after the `DEFAULT` keyword in the `DECLARE OR REPLACE VARIABLE` statement below. This will save the value into a variable named **user_id** that will be used across this lab.
-- MAGIC - Use `DELETE` statement with column_name of table
-- MAGIC - Run the cell below to proceed with the record's deletion from the table. 
-- MAGIC - Confirm a row was deleted.

-- COMMAND ----------

-- -- Define a variable for user_id to delete
DECLARE OR REPLACE VARIABLE user_id INT DEFAULT 11745; -- Place your user_id here

-- -- Delete user with user_id equals to variable
DELETE FROM user_lookup 
WHERE user_id = session.user_id;

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC
-- MAGIC -- -- Define a variable for user_id to delete
-- MAGIC DECLARE OR REPLACE VARIABLE user_id INT DEFAULT 11745; -- Place your user_id here
-- MAGIC
-- MAGIC -- -- Delete user with user_id equals to variable
-- MAGIC DELETE FROM user_lookup 
-- MAGIC WHERE user_id = session.user_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### G3. Confirm the record is deleted from the User Lookup table
-- MAGIC
-- MAGIC Run the query below to confirm the selected **user_id** was deleted from the **user_lookup** table. Confirm no rows are returned.

-- COMMAND ----------

SELECT * 
FROM user_lookup
WHERE user_id=session.user_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the query below and notice that the table now holds *99* records.

-- COMMAND ----------

SELECT count(*)
FROM user_lookup;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## H. Review user_lookup table History
-- MAGIC
-- MAGIC Now that our record is deleted, a new entry in the **user_lookup** table history is added corresponding to this event. Let's get a view to this table's history. 
-- MAGIC
-- MAGIC   Run the query and confirm version **2** and that the **operation** column is *DELETE*.

-- COMMAND ----------

DESCRIBE HISTORY user_lookup;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## I. Read the user_lookup CDF output (starting from version 1)
-- MAGIC
-- MAGIC This section we'll proceed to read CDF data from a the **user_lookup** table, well apply two approaches:
-- MAGIC - Using SQL via `table_changes` function
-- MAGIC - Using Python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### I1. Using SQL to View Row Level Changes
-- MAGIC
-- MAGIC To read the CDF data:
-- MAGIC
-- MAGIC - Using [table_changes](https://docs.databricks.com/en/sql/language-manual/functions/table_changes.html) function
-- MAGIC - Select all changes from version **1** of the **user_lookup** table
-- MAGIC
-- MAGIC Confirm that one row is returned in the output. The row should be the row you deleted in the earlier step.
-- MAGIC
-- MAGIC **HINT:** [Use Delta Lake change data feed on Databricks](https://docs.databricks.com/en/delta/delta-change-data-feed.html#use-delta-lake-change-data-feed-on-databricks) documentation.

-- COMMAND ----------

-- -- USING SQL
SELECT * 
FROM table_changes("user_lookup", 1);

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC -- -- USING SQL
-- MAGIC SELECT * 
-- MAGIC FROM table_changes("user_lookup", 1);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### I2. (OPTIONAL) Using Python to View Row Level Changes 
-- MAGIC
-- MAGIC To read the CDF data:
-- MAGIC - Read the **user_lookup** table
-- MAGIC - Configure the stream to enable reading change data with the `readChangeData` option
-- MAGIC - Configure the stream to start reading from version **1** with `startingVersion` option
-- MAGIC
-- MAGIC Confirm that one row is returned in the output. The row should be the row you deleted in the earlier step.
-- MAGIC
-- MAGIC **HINT:** [Use Delta Lake change data feed on Databricks](https://docs.databricks.com/en/delta/delta-change-data-feed.html#use-delta-lake-change-data-feed-on-databricks) documentation.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC user_lookup_df = (spark
-- MAGIC                   .read
-- MAGIC                   .format("delta")
-- MAGIC                   .option("readChangeData", True) # Reading Change Data
-- MAGIC                   .option("startingVersion", 1)  # Start Reading Version
-- MAGIC                   .table("user_lookup")
-- MAGIC )
-- MAGIC
-- MAGIC display(user_lookup_df)

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC %python
-- MAGIC user_lookup_df = (spark
-- MAGIC                   .read
-- MAGIC                   .format("delta")
-- MAGIC                   .option("readChangeData", True) # Reading Change Data
-- MAGIC                   .option("startingVersion", 1)  # Start Reading Version
-- MAGIC                   .table("user_lookup")
-- MAGIC )
-- MAGIC
-- MAGIC display(user_lookup_df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## J. Propagate Deletes To Multiple Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### J1. Create a Temporary View of the user_lookup Table CDF Changes
-- MAGIC
-- MAGIC Let's create a temporary view to hold the CDF changes of **user_lookup** table.
-- MAGIC
-- MAGIC **HINTS**:
-- MAGIC - Use `CREATE OR REPLACE TEMPORARY VIEW` name it **user_lookup_deletes_vw**
-- MAGIC - Use `table_changes` and starting version of **2**
-- MAGIC - Select all records in view where **_change_type** is *delete*
-- MAGIC - Then display the results of the view. 
-- MAGIC - Confirm one row is returned.

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW user_lookup_deletes_vw AS
-- MAGIC SELECT * 
-- MAGIC FROM table_changes('user_lookup',2)
-- MAGIC WHERE _change_type ='delete';
-- MAGIC
-- MAGIC
-- MAGIC -- -- Display the view
-- MAGIC SELECT *
-- MAGIC FROM user_lookup_deletes_vw;

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC CREATE OR REPLACE TEMPORARY VIEW user_lookup_deletes_vw AS
-- MAGIC SELECT * 
-- MAGIC FROM table_changes('user_lookup',2)
-- MAGIC WHERE _change_type ='delete';
-- MAGIC
-- MAGIC
-- MAGIC -- -- Display the view
-- MAGIC SELECT *
-- MAGIC FROM user_lookup_deletes_vw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### J2. Merge the Temporary View into **Users** Table
-- MAGIC
-- MAGIC Merge using **user_lookup_deletes_vw** as source into **users** table and **DELETE** when **alt_id** gets matched.
-- MAGIC
-- MAGIC The `MERGE` operation should return a value of *1* for **num_affected_rows** and **num_deleted_rows**.
-- MAGIC
-- MAGIC **HINTS**:
-- MAGIC - Use the [MERGE INTO](https://docs.databricks.com/en/sql/language-manual/delta-merge-into.html) Syntax:
-- MAGIC ```
-- MAGIC
-- MAGIC MERGE [ WITH SCHEMA EVOLUTION ] INTO target_table_name [target_alias]
-- MAGIC    USING source_table_reference [source_alias]
-- MAGIC    ON merge_condition
-- MAGIC    { WHEN MATCHED [ AND matched_condition ] THEN matched_action |
-- MAGIC      WHEN NOT MATCHED [BY TARGET] [ AND not_matched_condition ] THEN not_matched_action |
-- MAGIC      WHEN NOT MATCHED BY SOURCE [ AND not_matched_by_source_condition ] THEN not_matched_by_source_action } [...]
-- MAGIC
-- MAGIC matched_action
-- MAGIC  { DELETE |
-- MAGIC    UPDATE SET * |
-- MAGIC    UPDATE SET { column = { expr | DEFAULT } } [, ...] }
-- MAGIC
-- MAGIC not_matched_action
-- MAGIC  { INSERT * |
-- MAGIC    INSERT (column1 [, ...] ) VALUES ( expr | DEFAULT ] [, ...] )
-- MAGIC
-- MAGIC not_matched_by_source_action
-- MAGIC  { DELETE |
-- MAGIC    UPDATE SET { column = { expr | DEFAULT } } [, ...] }
-- MAGIC
-- MAGIC
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC MERGE INTO users u
-- MAGIC USING user_lookup_deletes_vw ul
-- MAGIC ON u.alt_id = ul.alt_id
-- MAGIC WHEN MATCHED
-- MAGIC   THEN DELETE;

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC MERGE INTO users u
-- MAGIC USING user_lookup_deletes_vw ul
-- MAGIC ON u.alt_id = ul.alt_id
-- MAGIC WHEN MATCHED
-- MAGIC   THEN DELETE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the query below to check the record count in the **users** table, notice this reflects record deleted propagated via the `MERGE_INTO` executed in the cell above.

-- COMMAND ----------

SELECT count(*)
FROM users

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### J3. Merge the Temporary View into the users_bin Table
-- MAGIC
-- MAGIC Similarly, merge using **user_lookup_deletes_vw** as source into **user_bins** table and `DELETE` when **user_id** gets matched.
-- MAGIC
-- MAGIC The `MERGE` operation should return a value of *1* for **num_affected_rows** and **num_deleted_rows**.

-- COMMAND ----------

MERGE INTO users_bin ub
USING user_lookup_deletes_vw ul
ON ub.user_id = ul.user_id
WHEN MATCHED
  THEN DELETE;

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC MERGE INTO users_bin ub
-- MAGIC USING user_lookup_deletes_vw ul
-- MAGIC ON ub.user_id = ul.user_id
-- MAGIC WHEN MATCHED
-- MAGIC   THEN DELETE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the query below to check for the deleted record in the **users_bin** table by filtering and using the selected **user_id** saved in a variable. 
-- MAGIC
-- MAGIC Notice there are no results confirming the delete as correctly propagated via the `MERGE INTO`.

-- COMMAND ----------

SELECT *
FROM users_bin
WHERE user_id = session.user_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
