# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 - Deploy a Simple DAB
# MAGIC
# MAGIC ### Estimated Duration: 15-20 minutes
# MAGIC
# MAGIC In this lab, you will:
# MAGIC
# MAGIC 1. Create a simple job using the UI.
# MAGIC
# MAGIC 2. Examine its YAML configuration.
# MAGIC
# MAGIC 3. Use the YAML configuration to update the **databricks.yml** configuration file.
# MAGIC
# MAGIC 4. Use the Databricks CLI to run the following DAB commands:
# MAGIC     - Validate the job
# MAGIC     - Deploy the job
# MAGIC     - Run the job
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

# MAGIC %run ../Includes/Classroom-Setup-02L

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
# MAGIC ## SCENARIO
# MAGIC
# MAGIC You are in charge of deploying Databricks projects for your CI/CD pipelines in your organization using Databricks Asset Bundles. A coworker has sent you a notebook located in **./src/our_project_code**, and they want your help to begin the CI/CD process to deploy their project.
# MAGIC
# MAGIC To start, let's first deploy the project to the development environment. Your task is to retrieve the YAML job configuration and update the **databricks.yml** bundle file located in the **02L - Deploy a Simple DAB** folder. The notebook given to you simply creates a simple bronze table and silver table using the development data **nyctaxi_raw** within your **username_1_dev** catalog. The dev catalog will be specified within the job parameters.
# MAGIC
# MAGIC Follow the instructions below to complete your task.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Preview the Development Data
# MAGIC Preview the **nyctaxi_raw** data in your **username_1_dev** catalog. Notice that the development data contains a small sample of the production data (100 rows).

# COMMAND ----------

spark.sql(f'''
SELECT *
FROM {DA.catalog_dev}.default.nyctaxi_raw
''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. TO DO: STEPS
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the following cell to obtain your cluster id for the lab. You will need this for the YAML configuration.
# MAGIC
# MAGIC     **NOTE:** By default, if you create the job using the UI and specify the required cluster, the cluster ID will already be shown in the YAML configuration. You can also obtain it using the code below.

# COMMAND ----------

spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Get the YAML configuration for the job to deploy the notebook **./src/our_project_code** by creating the job using the UI.
# MAGIC
# MAGIC     **HINT:** The best way to get the necessary YAML job configuration is to manually create the job and copy the configuration. You can also refer to the [Databricks REST API reference](https://docs.databricks.com/api/workspace/introduction) for additional help.
# MAGIC
# MAGIC     The job has the following requirements:
# MAGIC
# MAGIC    - Name the job **lab02_job_yourfirstname**.
# MAGIC
# MAGIC    - Create a job with a single task named **create_nyc_tables** using the **./02L - Deploy a Simple DAB/src/our_project_code** notebook.
# MAGIC
# MAGIC    - Use your current lab cluster for the job's compute (selecting your lab compute cluster will automatically return the cluster ID in the YAML configuration).
# MAGIC
# MAGIC    - Add two **Job parameters** (make sure these are for the job, not the task):
# MAGIC
# MAGIC      - **catalog_name** referencing the catalog shown in step **A. Classroom Setup**.
# MAGIC      
# MAGIC      - **display_target** with the value **Development**.
# MAGIC
# MAGIC    - (Optional) Under **Job notifications**, send an email notification to yourself when the **Job** completes successfully (ensure the notification is for the job, not the task).

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Once you have created the job, copy the YAML configuration.
# MAGIC
# MAGIC     **NOTE:** You can also test the configuration by running the job if you would like.

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Modify the **databricks.yml** configuration file for your Databricks Asset Bundle (DAB) in the folder **02 - Deploy a Simple DAB**. Complete the following steps:
# MAGIC
# MAGIC    - Add the job YAML configuration from the previous step under the RESOURCES comment in the **databricks.yml** file. In the job configuration:
# MAGIC
# MAGIC      - Ensure the correct extension for the notebook is included (**HINT:** The notebook is an SQL notebook).
# MAGIC      
# MAGIC      - Modify the notebook path to use a relative path. (**HINT: ./src/our_project_code.sql**)
# MAGIC      
# MAGIC
# MAGIC    - In the **target** mapping, create a target environment called **dev** with the following configurations:
# MAGIC      
# MAGIC      - Set **dev** as the default target.
# MAGIC      
# MAGIC      - Set it to development mode.
# MAGIC      
# MAGIC      - For **Workspace** within **targets**, add the following:
# MAGIC        
# MAGIC ```yaml
# MAGIC workspace:
# MAGIC   root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
# MAGIC ```
# MAGIC
# MAGIC **HINT:** View the Databricks Asset Bundles [Mappings](https://docs.databricks.com/en/dev-tools/bundles/settings.html#mappings) documentation for additional information. An example solution to the **databricks.yml** file can be found in the accompanying **solution** folder.
# MAGIC
# MAGIC </br>
# MAGIC
# MAGIC ### NOTE - PLEASE READ!
# MAGIC
# MAGIC Starting December 20, 2024, the [default format for new notebooks is now IPYNB (Jupyter) format](https://docs.databricks.com/en/release-notes/product/2024/december.html#the-default-format-for-new-notebooks-is-now-ipynb-jupyter-format). This may cause issues when referencing notebooks with DABs, as you must specify the file extension.
# MAGIC
# MAGIC For the purpose of this course, all notebooks will be in either **.py** or **.sql** format. However, to confirm the file extension of a notebook, complete the following steps:
# MAGIC
# MAGIC - In the top navigation bar, below the notebook name, select **File**.
# MAGIC
# MAGIC - Scroll down and find the **Notebook format** option, then select it.
# MAGIC
# MAGIC - Here, you should see the notebook format listed as **Source (.scala, .py, .sql, .r)**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the DAB to the Development Environment

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the Databricks CLI command below to confirm the Databricks CLI is authenticated.
# MAGIC </br>
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
# MAGIC 2. Check the version of the Databricks CLI. Confirm that the version is **v0.257.0**.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks -v

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Use the `ls` command to view the available files in the current directory. Confirm that you see the **databricks.yml** file, the **src** folder, and the **02L - Deploy a Simple DAB** notebook.

# COMMAND ----------

# MAGIC %sh
# MAGIC ls

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Validate your **databricks.yml** bundle configuration file using the Databricks CLI. Run the cell and confirm that the validation was successful. If there is an error, fix it.
# MAGIC
# MAGIC     **HINT:** You can refer to the documentation for the [bundle command group](https://docs.databricks.com/en/dev-tools/cli/bundle-commands.html) for help with validating, deploying, running, and destroying a bundle.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle validate

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Deploy the bundle using the development environment target with the Databricks CLI.
# MAGIC
# MAGIC     After the cell completes, manually check to see if the job was created successfully. The job name will be **[dev user_name] lab02_job_firstname**.
# MAGIC
# MAGIC     Specifically:
# MAGIC     - check the task and make sure the notebook is referenced correctly
# MAGIC     - check the job parameters and confirm they are referencing the correct catalog
# MAGIC
# MAGIC     **NOTE:** This will take about a minute to complete.
# MAGIC
# MAGIC     **HINT:** You can refer to the documentation for the [bundle command group](https://docs.databricks.com/en/dev-tools/cli/bundle-commands.html) for help with validating, deploying, running, and destroying a bundle.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Untitled
# MAGIC %sh
# MAGIC databricks bundle deploy -t dev

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Run the bundle using the target development environment with the Databricks CLI.
# MAGIC
# MAGIC     **NOTE:** This will take about 1-2 minutes to complete.
# MAGIC
# MAGIC     **HINT:** You can refer to the documentation for the [bundle command group](https://docs.databricks.com/en/dev-tools/cli/bundle-commands.html) for help with validating, deploying, running, and destroying a bundle.
# MAGIC
# MAGIC     **HINT:** Remember to use the key name from the resources mapping in the **databricks.yml** file (your name will differ):
# MAGIC
# MAGIC ```
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     lab02_job_yourfirstname:    # <--- The job key name here
# MAGIC       name: lab02_job_yourfirstname
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle run -t dev lab02_job_duc

# COMMAND ----------

# MAGIC %md
# MAGIC 7. After the job successfully completes, run the following cell to confirm that the bronze table from your job was created correctly.
# MAGIC
# MAGIC **NOTE:** The job created two tables in your **username_1_dev** catalog, **nyctaxi_bronze** and **nyctaxi_silver**. We will simply check the number of rows in the bronze table.

# COMMAND ----------

check_nyctaxi_bronze_table(user_catalog = DA.catalog_dev, total_count=100)

# COMMAND ----------

# MAGIC %md
# MAGIC 8. Lastly, destroy the bundle you created for this lab.
# MAGIC
# MAGIC     **HINT:** You can refer to the documentation for the [bundle command group](https://docs.databricks.com/en/dev-tools/cli/bundle-commands.html) for help with validating, deploying, running, and destroying a bundle.
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle destroy --auto-approve

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
# MAGIC