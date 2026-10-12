# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 07L - Adding ML to Engineering Workflows with DABs
# MAGIC
# MAGIC ### Estimated Duration: 25-30 minutes
# MAGIC
# MAGIC In this lab, you will
# MAGIC - Create and maintain variable configurations for data assets with DABs.
# MAGIC - Understand and modify bundle YAML configuration files.
# MAGIC - Use the Databricks CLI with Notebooks to validate and deploy DABs with a ML asset.

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
# MAGIC
# MAGIC **NOTE:** This will take 2-3 minutes to setup and create the models.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-7L

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
# MAGIC Congratulations! You’ve successfully built the bulk of your workflow. The ML team has asked you to ensure your tests meet their requirements for inferencing a model they've deployed in the Dev environment. You don’t need to learn ML—just know how to attach the model to the workflow using the bundle you’ve already built.
# MAGIC
# MAGIC **Optional task before starting:** For data scientists with ML knowledge, you can inspect the pre-trained model by navigating to experiments. For this demonstration, you don't need to understand the model—your goal is simply to add it to your bundle.
# MAGIC

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

creds = DA.load_credentials()

# COMMAND ----------

# MAGIC %sh 
# MAGIC databricks workspace list /Users

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Update **variables.yml**
# MAGIC
# MAGIC Within the folder where this notebook is located, you will find a folder called [**TODO - Lab DABs Workflows**]($./TODO - Lab DABs Workflow). You will be updating some of the files in this folder to attach a machine learning model to the workflow. You *do not* need to know what this model does. The goal of this exercise is to understand how to attach an additional Unity Catalog asset, which is a registered ML model in this case.
# MAGIC
# MAGIC ### Instructions:
# MAGIC 1. Navigate to the **src** folder. Here you will find two folders, **dlt_pipelines** and **helpers**, and two notebooks, **Final Visualization** and **Inference**. The notebook we’ll focus on for the lab is **Inference**. Click on it and inspect the cells.
# MAGIC
# MAGIC 2. In a separate tab, navigate to **resources** and click on **variables.yml**. We will need to update this YAML file with some additional variables.
# MAGIC
# MAGIC 3. In the **Inference** notebook, you’ll see some variables being called in the cell under the header **Parameterize the notebook for our workflow and passing variables**—namely:
# MAGIC
# MAGIC     - `base_model_name` - The name of the model
# MAGIC
# MAGIC     - `silver_table_name` - The name and location of the silver table **catalog.schema.silver_sample_ml**
# MAGIC
# MAGIC 4. We will need to point these to our bundle YAML files.
# MAGIC
# MAGIC     - Complete the two new variables, `base_model_name` and `silver_table_name`, in the **variables.yml** file in the section marked **PLEASE ONLY CHANGE THE VARIABLES IN THE FOLLOWING SECTION**.
# MAGIC
# MAGIC         - To find the default value for `base_model_name`, locate the model in the dev catalog under **Models**.
# MAGIC
# MAGIC         - The default value for `silver_table_name` should is set to **silver_sample_ml**.
# MAGIC
# MAGIC 4. We will use a cluster for this ML task. Define a third variable called `cluster_id`. You have four options for defining this variable:
# MAGIC
# MAGIC     - Option 1: Define the lookup variable in username and use `${var.username}` to reference the `username` variable.
# MAGIC
# MAGIC     - Option 2: Use `lookup` and set the `cluster` value to `${workspace.current_user.userName}`.
# MAGIC
# MAGIC     - Option 3: Hardcode the default value using the `lookup` method.
# MAGIC
# MAGIC     - Option 4: Find your cluster ID by navigating to Compute on the left menu, clicking on your cluster, selecting the three vertical dots, and clicking **View JSON**. Copy the cluster ID near the top of the JSON. Alternatively, you can get the cluster ID by running the following code snippet in a new cell: `print(spark.conf.get("spark.databricks.clusterUsageTags.clusterId"))`. Paste this value for the `default` value of `cluster_id` in the `variables.yml` file.
# MAGIC
# MAGIC ### Summary:
# MAGIC By completing the tasks, you should have created three new variables: `base_model_name`, `silver_table_name`, and `cluster_id`. Each variable will have a description and a default value.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Update **dabs_workflow_with_ml.job.yml**
# MAGIC
# MAGIC Now that we’ve updated our **variables.yml** file, let's move on to updating our workflow. We will not be configuring the Lakeflow Declarative pipeline in this step.
# MAGIC
# MAGIC ### Instructions:
# MAGIC Navigate to **resources** and open the **job** folder. Here you will find all the tasks previously created. Create a new task for inferencing the ML model with the following constraints:
# MAGIC
# MAGIC 1. The task name can be anything you choose.
# MAGIC
# MAGIC 1. The task must depend on **Health_ETL**.
# MAGIC
# MAGIC 1. Add a key called `existing_cluster_id` and set its value to reference the `cluster_id` variable you created in the previous step.
# MAGIC
# MAGIC 1. Add a `notebook_task` that contains a `notebook_path`, `base_parameters`, and `source`:
# MAGIC     - `notebook_path` should reference our `Inference` notebook.
# MAGIC
# MAGIC     - `base_parameters` should have 3 keys. There are two keys that reference the variables we created earlier: `base_model_name` and `silver_table_name` and one that will reference the dev catalog (_Hint: use a variable that was already pre-configured in the `variables.yml` file_).
# MAGIC     
# MAGIC     - You can also provide a description if desired.
# MAGIC
# MAGIC **HINT**: Use the existing tasks as templates to help with this step.
# MAGIC
# MAGIC ### Summary:
# MAGIC By completing this task, you should have created a new task in your workflow within `dabs_workflow.job.yml` and be ready to validate the bundle.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Bundle Validation and Deployment
# MAGIC Now that you have updated your **variables.yml** and **dabs_workflow.job.yml`** files, you are ready to validate your bundle before deployment!

# COMMAND ----------

# MAGIC %md
# MAGIC Use the Databricks CLI to print out a summary of all of the resources defined in the project and the corresponding names which will be generated after deploying the bundle. Note, you will have to `cd` into the bundle folder.

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd "./TODO - Lab DABs Workflow"
# MAGIC databricks bundle summary

# COMMAND ----------

# MAGIC %md
# MAGIC Use the Databricks CLI to validate the bundle. Note, you will have to `cd` into the bundle folder.

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd "./TODO - Lab DABs Workflow"
# MAGIC pwd;
# MAGIC databricks bundle validate -t development;

# COMMAND ----------

# MAGIC %md
# MAGIC Deploy the bundle to the development environment. Note, you will have to `cd` into the bundle folder.

# COMMAND ----------

# MAGIC %sh
# MAGIC cd "./TODO - Lab DABs Workflow"
# MAGIC databricks bundle deploy -t development

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Run the Job
# MAGIC Option 1: Use the UI to run the job and visually watch the tasks kick off. Click on each task to inspect the notebooks or Lakeflow Declarative pipeline. 
# MAGIC
# MAGIC Option 2: Run the job using the CLI.

# COMMAND ----------

# MAGIC %md
# MAGIC Note: You will need to delete the Lakeflow Declarative Pipeline if you worked through the previous demonstration. The name of the pipeline created in the previous demonstration is of the form **[dev <usesrname>] health_etl_pipeline_development**.

# COMMAND ----------

# MAGIC %sh
# MAGIC cd "./TODO - Lab DABs Workflow"
# MAGIC databricks bundle run ml_health_etl_workflow -t development

# COMMAND ----------

# MAGIC %md
# MAGIC Destroy the bundle for development.

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd "./TODO - Lab DABs Workflow"
# MAGIC databricks bundle destroy -t development --auto-approve

# COMMAND ----------

# MAGIC %md
# MAGIC ### Staging Bundle Validation, Deployment, and Run
# MAGIC
# MAGIC Imagine now that you have gone through the process of reviewing your code, analyzed code coverage, etc. and you are ready to now deploy and test within a staging environment. DABs makes this extremely easy by changing and passing a few parameter values. 
# MAGIC
# MAGIC #### Instructions:
# MAGIC Using the stage environment (catalog), do the following:
# MAGIC 1. Explore the stage environment in your **databricks.yml** file and make note of completed changes for stage.
# MAGIC
# MAGIC 1. Run a summary on the bundle.
# MAGIC
# MAGIC 1. Validate the bundle. 
# MAGIC
# MAGIC 1. Deploy the bundle. 
# MAGIC
# MAGIC 1. Run the bundle. 
# MAGIC
# MAGIC 1. Destroy the bundle.

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd "./TODO - Lab DABs Workflow"
# MAGIC databricks bundle summary -t stage

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd "./TODO - Lab DABs Workflow"
# MAGIC databricks bundle validate -t stage

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd "./TODO - Lab DABs Workflow"
# MAGIC databricks bundle deploy -t stage

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd "./TODO - Lab DABs Workflow"
# MAGIC databricks bundle run ml_health_etl_workflow -t stage

# COMMAND ----------

# MAGIC %md
# MAGIC Destroy the bundle for stage.

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd "./TODO - Lab DABs Workflow"
# MAGIC databricks bundle destroy -t stage --auto-approve

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary:
# MAGIC
# MAGIC In this lab you used a new data asset stored in Unity Catalog to create a new task for your workflow. By understanding how the YAML files are structured, you were able to update your workflow by defining new variables and create a new task.
# MAGIC
# MAGIC ### Next Steps:
# MAGIC Try to create your own DAB from scratch using the results from this lab. It's recommended that you incrementally build your workflow one task at a time and making small changes until you are comfortable with understanding the architecture of your workflow.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
# MAGIC

# COMMAND ----------

