# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 05L - Use a Databricks Default DAB Template
# MAGIC
# MAGIC ### Estimated Duration: ~10 minutes
# MAGIC
# MAGIC In this lab, you will use a Databricks default bundle template and deploy the default bundle. 
# MAGIC

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
# MAGIC Run the following cell to configure your working environment for this course.
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-05L

# COMMAND ----------

# MAGIC %md
# MAGIC ## IMPORTANT LAB INFORMATION
# MAGIC
# MAGIC Recall that your credentials are stored in a file when running [0 - REQUIRED - Course Setup and Authentication]($../0 - REQUIRED - Course Setup and Authentication).
# MAGIC
# MAGIC If you end your lab or your lab session times out, your environment will be reset.
# MAGIC
# MAGIC If you encounter an error regarding unavailable catalogs or if your Databricks CLI is not authenticated, you will need to rerun the [0 - REQUIRED - Course Setup and Authentication]($../0 - REQUIRED - Course Setup and Authentication) notebook to recreate the catalogs and your Databricks CLI credentials.
# MAGIC
# MAGIC **Use classic compute to use the CLI through a notebook.**

# COMMAND ----------

# MAGIC %md
# MAGIC Run the Databricks CLI command below to confirm the Databricks CLI is authenticated.
# MAGIC
# MAGIC </br>
# MAGIC
# MAGIC ##### DATABRICKS CLI ERROR TROUBLESHOOTING:
# MAGIC   - If you encounter an Databricks CLI authentication error, it means you haven't created the PAT token specified in notebook **0 - REQUIRED - Course Setup and Authentication**. You will need to set up Databricks CLI authentication as shown in that notebook.
# MAGIC
# MAGIC   - If you encounter the error below, it means your `databricks.yml` file is invalid due to a modification. Even for non-DAB CLI commands, the `databricks.yml` file is still required, as it may contain important authentication details, such as the host and profile, which are utilized by the CLI commands.
# MAGIC
# MAGIC ![CLI Invalid YAML](../Includes/images/databricks_cli_error_invalid_yaml.png)

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks catalogs list

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Deploy a Databricks Bundle Template
# MAGIC
# MAGIC Databricks provides the following default bundle templates (as of Q1 2025):
# MAGIC
# MAGIC | Template | Description |
# MAGIC |----------|-------------|
# MAGIC | default-python   | A template for using Python with Databricks. This template creates a bundle with a job and Delta Live Tables pipeline. See default-python.                          |
# MAGIC | default-sql      | A template for using SQL with Databricks. This template contains a configuration file that defines a job that runs SQL queries on a SQL warehouse. See default-sql.   |
# MAGIC | dbt-sql          | A template which leverages dbt-core for local development and bundles for deployment. This template contains the configuration that defines a job with a dbt task, as well as a configuration file that defines dbt profiles for deployed dbt jobs. See dbt-sql. |
# MAGIC | mlops-stacks     | An advanced full stack template for starting new MLOps Stacks projects. See mlops-stacks and Databricks Asset Bundles for MLOps Stacks.                            |
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the `--help` command to view documentation for the `bundle init` command.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle init --help

# COMMAND ----------

# MAGIC %md
# MAGIC 2. To use a Databricks default bundle template to create your bundle, use the Databricks CLI `databricks bundle init` command, specifying the name of the default template to use.
# MAGIC
# MAGIC     For example, the following command creates a bundle using the default Python bundle template.
# MAGIC
# MAGIC     **NOTE**: If you do not specify a default template, the `bundle init` command presents the set of available templates from which you can choose in interactive mode. You can then select different options based on your needs. Interactive mode for templates is not available when using notebooks to execute Databricks CLI commands.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle init default-python

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Navigate to the **my_project** folder. Explore the bundle template folder structure at a high level:
# MAGIC
# MAGIC   - **resources** contains additional YAML files to include:
# MAGIC     - **my_project.job.yml**
# MAGIC     - **my_project.pipeline.yml**
# MAGIC
# MAGIC   - **scratch** contains a exploration notebook.
# MAGIC
# MAGIC   - **src** contains our production code. In this example, we have a notebook and a DLT pipeline notebook.
# MAGIC
# MAGIC   - **tests** contains unit and integration tests for this project.
# MAGIC
# MAGIC   - This project also contains a **pytest.ini** file that configures pytest, a **README.md** file, and a few others.

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Open the **databricks.yml** file and explore the bundle configuration. Notice that the template provides two targets.

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Run the `pwd` and `ls` commands. Notice that we are not within the **my_project** folder.

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd
# MAGIC ls

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Complete the following cell to validate the template bundle.
# MAGIC
# MAGIC - Since we are executing Databricks CLI commands within the notebooks, you will need to change the directory to the **my_project** folder and then deploy the bundle within one cell.
# MAGIC - Use the `cd my_project` command first to change the directory, 
# MAGIC - Then run below the `cd` command use the `databricks bundle validate` command all within one cell.

# COMMAND ----------

# MAGIC %sh
# MAGIC cd my_project
# MAGIC databricks bundle validate

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** After validation, review the generated template. In these labs, we will stop at this step because you do not have the required permissions to create clusters for deploying and running the template.

# COMMAND ----------

# MAGIC %md
# MAGIC 7. To save time for a live class, we will end at validation and exploration of the template for training purposes only. If you have time, explore more of the bundle.
# MAGIC
# MAGIC     **NOTE:** **This bundle will not deploy and run in the Databricks Academy lab. The lab restricts the ability for a user to create a cluster.**
# MAGIC
# MAGIC     **NOTE:** This lab also provides a working VSCode environment. To deploy a template using VSCode view the demonstration **08 - Using VSCode with Databricks**.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
# MAGIC
