# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 04L - Deploy a DAB to Multiple Environments
# MAGIC
# MAGIC ### Estimated Duration: 15-20 minutes
# MAGIC
# MAGIC In this lab, you will:
# MAGIC
# MAGIC 1. Modify the variables in the **databricks.yml** file to reference the correct resources and variables (dev and prod).
# MAGIC
# MAGIC 2. Deploy a project to the **development** and **production** environments with different configurations for each.
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
# MAGIC   - In the drop-down, select **More**.
# MAGIC
# MAGIC   - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
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

# MAGIC %run ../Includes/Classroom-Setup-04L

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
# MAGIC You are in charge of deploying Databricks projects in your organization using Databricks Asset Bundles. So far, you've configured the project to deploy to the **development** environment (**02L - Deploy a Simple DAB**). Your next task is to modify the **databricks.yml** file to deploy the project into the development and production environments with different configurations. You will accomplish this with variable substitution.
# MAGIC
# MAGIC #### Development configuration target requirements:
# MAGIC - Use the development data in your **username_1_dev** catalog
# MAGIC
# MAGIC #### Production configuration target requirements:
# MAGIC - Use the development data in your **username_3_stage** catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Preview the Development and Production Data

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Preview the **nyctaxi_raw** development data within your **username_1_dev** catalog. Notice that the dev data contains 100 rows.
# MAGIC

# COMMAND ----------

spark.sql(f'''
          SELECT * 
          FROM {DA.catalog_dev}.default.nyctaxi_raw
          ''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. View the tables in your **username_1_dev** catalog. Notice that the **nyctaxi_bronze** and **nyctaxi_silver** tables do not exist.
# MAGIC

# COMMAND ----------

spark.sql(f'SHOW TABLES IN {DA.catalog_dev}.default').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Preview the **nyctaxi_raw** production data within your **username_3_prod** catalog. Notice that the production data contains about 22,000 rows.

# COMMAND ----------

spark.sql(f'''
          SELECT count(*) AS TotalRows 
          FROM {DA.catalog_prod}.default.nyctaxi_raw
          ''').display()

# COMMAND ----------

spark.sql(f'''
          SELECT * 
          FROM {DA.catalog_prod}.default.nyctaxi_raw
          ''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 4. View the tables in your **username_3_prod** catalog. Notice that the **nyctaxi_bronze** and **nyctaxi_silver** tables do not exist.

# COMMAND ----------

spark.sql(f'SHOW TABLES IN {DA.catalog_prod}.default').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. TO DO: STEPS
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the cell below to obtain your lab user name.

# COMMAND ----------

print(DA.catalog_name)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. In a new tab, open the **./resources/lab04_nyc.job.yml** file. Explore the file and complete the following:
# MAGIC
# MAGIC    a. Name the actual job **lab04_dab_`${workspace.current_user.userName}`**. This will dynamically add your user name to the end of the job.
# MAGIC
# MAGIC    b. Under **parameters** add the bundle target variable as the default value of **display_target**
# MAGIC     - **HINT:** [Variable substitutions](https://docs.databricks.com/aws/en/dev-tools/bundles/variables)
# MAGIC
# MAGIC
# MAGIC <br></br>
# MAGIC **Solution Resources File**
# MAGIC ```YAML
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     lab04_dab:
# MAGIC       name: lab04_dab_${workspace.current_user.userName}  # <----- lab04_dab_ + Append your user name variable value to the end of the job name
# MAGIC       tasks:
# MAGIC         - task_key: create_nyc_tables
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../src/our_project_code.sql
# MAGIC             source: WORKSPACE
# MAGIC       parameters:
# MAGIC         - name: target
# MAGIC           default: ${bundle.target}       # <---- Add the bundle.target variable here as a job value
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 3. In the new tab, open the **databricks.yml** file and explore the bundle configuration. Notice the following:
# MAGIC
# MAGIC   - The bundle name is **demo04_lab_bundle**.
# MAGIC
# MAGIC   - The **include** mapping is empty.
# MAGIC
# MAGIC   - The **variables** mapping contains a variety of variables. Explore the variables.
# MAGIC
# MAGIC   - The **target** mapping contains a **dev** and **prod** target environment.
# MAGIC
# MAGIC   Leave the **databricks.yml** file open.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 4. In the **databricks.yml**, complete the following:
# MAGIC
# MAGIC    a. In **includes**, add the **./resources/lab04_nyc.job.yml** file.
# MAGIC       - **HINT:** [include mapping](https://docs.databricks.com/aws/en/dev-tools/bundles/settings#include)
# MAGIC
# MAGIC    b. In **variables**, add your username to the variable `user_name` (your lab username can be found in step 1 of this section).
# MAGIC       - The `user_name` variable populates the `catalog_dev` and `catalog_prod` variables dynamically.
# MAGIC
# MAGIC    c. Under **targets**, complete the following to modify the catalog for the **dev** and **prod** targets by adding a job parameter specific to each target:
# MAGIC    
# MAGIC    - For the **dev** environment, create a job parameter named **catalog_name** that uses the `var.catalog_dev` value.
# MAGIC    
# MAGIC    - For the **prod** environment, create a job parameter named **catalog_name** that uses the `var.catalog_prod` value.
# MAGIC
# MAGIC    This is a great way to change the deployment based on the target environment. This example keeps it simple by adding basic job parameters, but you can modify a variety of configuration values using this method.
# MAGIC
# MAGIC    **NOTE:** You can add or modify the configuration of your resources within the **targets** configuration based on the environment requirements. In this example, we are adding a specific job parameter for dev and prod to our job defined in the **./resources/lab04_nyc.job.yml** file:

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Deploy to Development

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the Databricks CLI command below to confirm the Databricks CLI is authenticated.
# MAGIC
# MAGIC <br></br>
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
# MAGIC 2. Check the version of the Databricks CLI. Confirm the version is **v0.257.0**.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks -v

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Validate your **databricks.yml** bundle configuration file using the Databricks CLI. Run the cell and confirm the validation was successful. If there is an error fix the error. 
# MAGIC
# MAGIC     **HINT:** You can refer to the documentation for the [bundle command group](https://docs.databricks.com/en/dev-tools/cli/bundle-commands.html) for help with validating, deploying, running, and destroying a bundle.
# MAGIC
# MAGIC
# MAGIC     **NOTE:** For an example solution you can view the **databricks_solution.yml** file within the **solutions** folder. 
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle validate

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Deploy the bundle to the development environment using the Databricks CLI.
# MAGIC
# MAGIC     After the cell completes:
# MAGIC     - Manually check to see if the job was created successfully. The job name will be **[dev user_name] lab04_job_username**.
# MAGIC     - Check the **job parameters** and confirm it's using your **username_1_dev** catalog and that the **target** is *dev*.
# MAGIC
# MAGIC     **NOTE:** This will take about a minute to complete.
# MAGIC
# MAGIC     **HINT:** You can refer to the documentation for the [bundle command group](https://docs.databricks.com/en/dev-tools/cli/bundle-commands.html) for help with validating, deploying, running, and destroying a bundle.
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle deploy -t dev

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Run the bundle using the target development environment using the Databricks CLI. 
# MAGIC
# MAGIC     **NOTE:** This will take about a 1-2 minutes to complete.
# MAGIC
# MAGIC     **HINT:** You can refer to the documentation for the [bundle command group](https://docs.databricks.com/en/dev-tools/cli/bundle-commands.html) for help with validating, deploying, running, and destroying a bundle.
# MAGIC
# MAGIC
# MAGIC     **HINT:** Remember to use the key name from the resources mapping in the databricks.yml file(your name will differ):
# MAGIC ```
# MAGIC ...
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     lab04_dab:    # <--- The job key name here
# MAGIC       name: lab04_dab_${var.user_name}
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle run -t dev lab04_dab

# COMMAND ----------

# MAGIC %md
# MAGIC 6. After the job successfully completes, run the following cells to confirm both tables **nyctaxi_bronze** and **nyctaxi_silver**  were created in the **username_1_dev** catalog, and the **nyctaxi_bronze** table contains 100 rows.

# COMMAND ----------

spark.sql(f'SHOW TABLES IN {DA.catalog_dev}.default').display()

# COMMAND ----------

check_nyctaxi_bronze_table(user_catalog = DA.catalog_dev, total_count=100)

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Deploy to Production

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Deploy the bundle to the production environment using the Databricks CLI. This will take about a minute to complete.
# MAGIC
# MAGIC     After the cell completes:
# MAGIC     - Manually check to see if the job was created successfully. The job name will be **lab04_job_username**.
# MAGIC     - Check the **job parameters** and confirm it's using your **username_3_prod** catalog and that the **target** is *prod*.
# MAGIC
# MAGIC **HINT:** You can refer to the documentation for the [bundle command group](https://docs.databricks.com/en/dev-tools/cli/bundle-commands.html) for help with validating, deploying, running, and destroying a bundle.
# MAGIC
# MAGIC **NOTE:** Typically when running in production you will want to run the job using a service principal. For more information, check out the [Set a bundle run identity](https://docs.databricks.com/aws/en/dev-tools/bundles/run-as). For demonstration purposes, we are simply running the production job as the user.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle deploy -t prod

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the bundle using the target production environment using the Databricks CLI. 
# MAGIC
# MAGIC     **NOTE:** This will take about a 1-2 minutes to complete.
# MAGIC
# MAGIC     **HINT:** You can refer to the documentation for the [bundle command group](https://docs.databricks.com/en/dev-tools/cli/bundle-commands.html) for help with validating, deploying, running, and destroying a bundle.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle run -t prod lab04_dab

# COMMAND ----------

# MAGIC %md
# MAGIC 3. After the job successfully completes, run the following cells to confirm both tables **nyctaxi_bronze** and **nyctaxi_silver**  were created in the **username_3_prod** catalog, and the **nyctaxi_bronze** table contains 21,932 rows.

# COMMAND ----------

spark.sql(f'SHOW TABLES IN {DA.catalog_prod}.default').display()

# COMMAND ----------

check_nyctaxi_bronze_table(user_catalog = DA.catalog_prod, total_count=21932)

# COMMAND ----------

# MAGIC %md
# MAGIC ### BONUS
# MAGIC This was a simple example of deploying a DAB to multiple environments.
# MAGIC
# MAGIC - There are a variety of ways to set a variable's value. In this lab, we set values within the **databricks.yml** configuration file. You can also set variable values within the Databricks CLI. For more information, view the [Set a variable’s value](https://docs.databricks.com/en/dev-tools/bundles/variables.html#set-a-variables-value) documentation.
# MAGIC
# MAGIC - For additional information on overriding configuration values for environments, view the [Override cluster settings in Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/cluster-override) documentation.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
# MAGIC