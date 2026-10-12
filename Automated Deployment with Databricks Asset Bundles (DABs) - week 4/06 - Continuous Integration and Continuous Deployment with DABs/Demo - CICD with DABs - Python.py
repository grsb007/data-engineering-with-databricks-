# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # CI/CD with DABs
# MAGIC ## Overview
# MAGIC
# MAGIC In this section, we will build on our understanding of DABs and apply our knowledge to a CI/CD workflow.
# MAGIC
# MAGIC In this lesson, you will learn about continuous deployment using Databricks Asset Bundles (DABs) within a workflow that features a more complex architecture.
# MAGIC
# MAGIC ## Learning Objectives:
# MAGIC _By the end of the demonstration, you will be able to do the following:_
# MAGIC - Understand how to set variables for a bundle
# MAGIC - Perform unit and integration tests with a Lakeflow Declarative pipeline by deploying a DAB
# MAGIC - Deploy across multiple environments (catalogs): development, staging, and production. 
# MAGIC
# MAGIC **NOTE:** DLT has been renamed to **Lakeflow Declarative Pipelines**.

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

# MAGIC %run ../Includes/Classroom-Setup-06

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
# MAGIC ## B. Inspect Pre-Configured YAML Files
# MAGIC
# MAGIC Our goal is to deploy our project to the **dev**, **stage** and **prod** environments for our CI/CD pipeline. In this example, the project is a simple workflow that contains unit tests, a Lakeflow Declarative Pipeline and a notebook visualization. 
# MAGIC
# MAGIC ![Workflow](./images/06_Final_Workflow_Desc.png)
# MAGIC
# MAGIC
# MAGIC **NOTE:** In this advanced-level course, we assume prerequisites of essential DevOps concepts like code modularization, custom Python functions, unit testing with pytest, and integration tests with Declarative Pipelines. For more information on these topics, you can review the Databricks course: **DevOps Essentials for Data Engineering**. We will briefly explore each of these here, but will not spend much time on those fundamentals. The focus of this course is deployment with Databricks Asset Bundles.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Let's explore our project folder called **Full Project**. This folder contains all of our Databricks resources to deploy.
# MAGIC
# MAGIC You will find the following in the root folder:
# MAGIC
# MAGIC   - **src**
# MAGIC   - **resources**
# MAGIC   - **databricks.yml**
# MAGIC   - **tests**
# MAGIC
# MAGIC 1. In a new tab, open the **databricks.yml**.
# MAGIC
# MAGIC     - It begins by defining the bundle name under the **bundle** mapping.
# MAGIC     
# MAGIC     - Then it defines the resources to include under the **include** mapping. All the YAML configuration files are in the **resources** folder.
# MAGIC
# MAGIC     - Under the **targets** top-level mapping, you will see three defined targets and a variety of configuration specifics for each: development, stage, and production.
# MAGIC         - All three targets have a `root_path`.
# MAGIC         - All three targets have specific configurations.
# MAGIC         - The target environments **stage** and **production** will have additional variables that we need to configure, such as `target_catalog` and `raw_data_path` to specify the correct data.
# MAGIC
# MAGIC
# MAGIC 2. In the new tab open **resources**. 
# MAGIC
# MAGIC     - Here, you will find two folders: **job** and **pipeline**.
# MAGIC
# MAGIC     - Click on the YAML file named **variables.yml**. You will find a pre-defined variables for the resources to be deployed when deploying the bundle. These include things like the job name, notebook paths, and parameters to pass to the notebooks.
# MAGIC
# MAGIC     - The **health_etl_pipeline.pipeline.yml** file (located in the **pipeline** folder) describes the Declarative Pipeline configuration.
# MAGIC
# MAGIC     - The **dabs_workflow.job.yml** file (located in the **job** folder) describes the different tasks that will be created. Notice that there are 3 tasks: **Unit_Tests**, **Visualization**, and **Health_ETL**. While **Health_ETL** is listed after **Visualization**, it depends on **Unit_Tests**. The order of the tasks doesn't matter, as the `depends_on` key configures the dependencies.
# MAGIC
# MAGIC 3. In the new tab navigate back to **src** in the root folder. This folder contains other folders and notebooks that are called from the YAML files you inspected in the previous steps. These notebooks will be chained together as part of the workflow we will deploy below.
# MAGIC
# MAGIC     - **dlt_pipelines**: This folder contains two Declarative Pipeline notebooks (**gold_tables_dlt** and **ingests-bronze-silver_dlt**). You can inspect these notebooks to understand their role in the **Health_ETL** workflow.
# MAGIC     
# MAGIC     - **Final Visualization**: This notebook will be the final task in our workflow. It creates a stacked bar chart of cholesterol distribution by age group.
# MAGIC
# MAGIC     - **helpers**: Contains a .py file with custom python methods for the transformation of the data in the Pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Explore and Update YAML Configuration Files
# MAGIC We will update our YAML files to better understand how to point to the assets and variables needed to configure the bundle before validation using the Databricks CLI.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1: Explore the databricks.yml Configuration
# MAGIC
# MAGIC Recall that to use a variable called `my_variable` in a bundle, refer to it using `${var.my_variable}`.
# MAGIC
# MAGIC #### Instructions
# MAGIC
# MAGIC 1. Navigate to the folder named **Full Project**.
# MAGIC    
# MAGIC 2. Click on the file **databricks.yml** and explore the bundle configuration.
# MAGIC
# MAGIC 3. Locate the mapping **targets**. 
# MAGIC    - Each target is a unique collection of artifacts, Databricks workspace settings, and Databricks job or pipeline details.
# MAGIC    - The targets mapping consists of one or more target mappings, which must each have a unique programmatic (or logical) name.
# MAGIC
# MAGIC 4. Locate the **development** target and examine the configuration. Notice the following:
# MAGIC    - The value for `default` is set to `True`.
# MAGIC    - The value for `existing_cluster_id` uses the variable `cluster_id`.
# MAGIC    - The **tasks** are set to use our lab compute cluster.
# MAGIC
# MAGIC 5. Locate the **stage** target and examine the configuration. Notice the following:
# MAGIC    - The `target_catalog` variable uses the variable `catalog_stage`.
# MAGIC    - The `raw_data_path` variable uses the volume `health` in `catalog_stage`.
# MAGIC    - The **tasks** are set to use our lab compute cluster.
# MAGIC
# MAGIC 6. Locate the **production** target and examine the configuration. Notice the following:
# MAGIC    - The `target_catalog` variable uses the variable `catalog_prod`.
# MAGIC    - The `raw_data_path` variable uses the volume `health` in `catalog_prod`.
# MAGIC    - No compute cluster is specified for the job. The default compute will use Serverless in production.
# MAGIC

# COMMAND ----------

print(f'Your user name: {DA.catalog_name}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2: Update **variables.yml**
# MAGIC
# MAGIC Next, we will update the file **variables.yml**.
# MAGIC
# MAGIC #### Instructions
# MAGIC
# MAGIC 1. Navigate to the **resources** folder.
# MAGIC
# MAGIC 2. Click on the file **variables.yml**.
# MAGIC
# MAGIC 3. Fill in the following details for the variables:
# MAGIC
# MAGIC    - **TO DO**: `username`: Add your username here. Your username can be found in the cell above for your lab.
# MAGIC       - Use `${workspace.current_user.short_name}`
# MAGIC
# MAGIC    - **TO DO**: `my_email`: Enter your email address here. This will be used to send notifications.
# MAGIC       - Use your email address
# MAGIC
# MAGIC    - **TO DO**: `cluster_id`: 
# MAGIC       - You can use the `lookup` function with your username to obtain the cluster ID value.
# MAGIC       - Paste the value from cell 13 for the lookup cluster id variable
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **NOTES:**
# MAGIC 1. The file **variables_solution.yml** contains an example solution in case you need help.
# MAGIC
# MAGIC 2. In the Vocareum environment, all catalogs, clusters, and usernames match and have no spaces by default. If using this method outside the Vocareum environment, be cautious.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D. Visualizing the Bundle's Assets
# MAGIC
# MAGIC Here we will look at how to manually update our YAML files to help get acquainted with the setup. Since we are bringing in a pre-configured bundle, it's worth looking at the structure of files we'll be interacting with. Below is a diagram representing how the variables for the development catalog will be used. 
# MAGIC
# MAGIC ![Full Pipeline](./images/06_img1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Notebook Execution
# MAGIC
# MAGIC Now that we are familiar with the various folders and files that make up our bundle, let's make sure the CLI is installed by authenticating.

# COMMAND ----------

# DBTITLE 0,--i18n-d0b9eb16-9b01-11ee-b9d1-0242ac120002
# MAGIC %md
# MAGIC
# MAGIC ### Databricks CLI Authentication
# MAGIC
# MAGIC Usually, you would need to set up authentication for the CLI. However, in this training environment, that has already been taken care of for you.
# MAGIC
# MAGIC Verify the credentials by executing the following cell, which displays the contents of the `/Users` workspace directory.

# COMMAND ----------

# MAGIC %sh databricks workspace list /Users

# COMMAND ----------

# MAGIC %md
# MAGIC ### E1. Development Bundle
# MAGIC
# MAGIC Here is what the configuration of our target mapping for development looks like in the databricks.yml file. 
# MAGIC ```YAML
# MAGIC targets:
# MAGIC
# MAGIC   development:
# MAGIC     mode: development
# MAGIC     default: true
# MAGIC     # In Development, we will use classic compute for our tasks 
# MAGIC     resources:
# MAGIC       jobs:
# MAGIC         health_etl_workflow:    
# MAGIC           name: health_etl_workflow_${bundle.target} 
# MAGIC           tasks:
# MAGIC             - task_key: Unit_Tests
# MAGIC               existing_cluster_id: ${var.cluster_id}
# MAGIC             - task_key: Visualization
# MAGIC               existing_cluster_id: ${var.cluster_id}
# MAGIC     workspace:
# MAGIC       root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
# MAGIC ...
# MAGIC ```
# MAGIC
# MAGIC **NOTE:** Recall that with **development** and **stage** target environments we are using a mix of serverless and classic compute at the task level.

# COMMAND ----------

# MAGIC %md
# MAGIC To validate the bundle, run the following cell. This uses all the default values from **variables.yml** (see diagram above).

# COMMAND ----------

# DBTITLE 1,Validate Development
# MAGIC %sh 
# MAGIC cd "Full Project" 
# MAGIC pwd;
# MAGIC databricks bundle validate -t development

# COMMAND ----------

# MAGIC %md
# MAGIC After the development target validates, deploy the bundle to the development environment.

# COMMAND ----------

# DBTITLE 1,Deploy to Development
# MAGIC %sh
# MAGIC cd "Full Project" 
# MAGIC databricks bundle deploy -t development

# COMMAND ----------

# MAGIC %md
# MAGIC To run the bundle, using the Databricks CLI, run the following cell. This is the name of the job. Note that the job will show `[dev <username>] health_etl_workflow_<target>` within **Jobs and Pipelines**. 
# MAGIC
# MAGIC This makes sense when you refer back to the structure of the **dabs_workflow.job.yml** file located in **resources**: 
# MAGIC ```YAML
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     health_etl_workflow: # <----- Name of job to run
# MAGIC       name: health_etl_workflow${bundle.target}
# MAGIC       description: Final Workflow SDK
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Run Bundle in Development
# MAGIC %sh
# MAGIC cd "Full Project" 
# MAGIC databricks bundle run health_etl_workflow

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary - Development
# MAGIC While the job is running, examine the tasks when using the **development** target. Note the following:
# MAGIC - Unit tests passed
# MAGIC - Lakeflow Declarative Pipeline ETL and integration tests passed on a small sample of 7,500 rows of dev data.
# MAGIC - Visualization was created using the small sample of 7,500 rows of dev data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2. Staging Bundle
# MAGIC
# MAGIC Here is what the configuration of our target mapping for stage looks like in the databricks.yml file.
# MAGIC ```YAML
# MAGIC   ...
# MAGIC
# MAGIC   stage:
# MAGIC     mode: development
# MAGIC       # In stage, we will use classic compute for our tasks 
# MAGIC     resources:
# MAGIC       jobs:
# MAGIC         health_etl_workflow:   
# MAGIC           name: health_etl_workflow_${bundle.target}  
# MAGIC           tasks:
# MAGIC             - task_key: Unit_Tests
# MAGIC               existing_cluster_id: ${var.cluster_id}
# MAGIC             - task_key: Visualization
# MAGIC               existing_cluster_id: ${var.cluster_id}
# MAGIC     workspace:
# MAGIC       root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
# MAGIC     variables:
# MAGIC       target_catalog: ${var.catalog_stage}
# MAGIC       raw_data_path: /Volumes/${var.catalog_stage}/default/health
# MAGIC ```
# MAGIC
# MAGIC </br>
# MAGIC
# MAGIC Imagine you've reviewed your code, analyzed coverage, etc., and are ready to deploy and test in a staging environment. DABs simplifies this by adjusting a few parameter values. Run the following cells to validate, deploy, and run with `stage` as the target.
# MAGIC
# MAGIC </br>
# MAGIC
# MAGIC In this example, since `target_catalog` and `raw_data_path` have default values, we can override them when deploying to other targets like stage within the **targets** mapping. This specifies reading data from the staging catalog.
# MAGIC
# MAGIC </br>
# MAGIC
# MAGIC #### BONUS
# MAGIC You can also override variable values through the Databricks CLI for DABs. For example, you can run `databricks bundle validate --var="target_catalog=<username>_2_stage" -t stage`. Keep in mind this will just reproduce the same job you went through a moment ago.

# COMMAND ----------

# DBTITLE 1,Validate Staging
# MAGIC %sh
# MAGIC cd "Full Project" 
# MAGIC databricks bundle validate -t stage

# COMMAND ----------

# DBTITLE 1,Deploy to Staging
# MAGIC %sh
# MAGIC cd "Full Project" 
# MAGIC databricks bundle deploy -t stage

# COMMAND ----------

# DBTITLE 1,Run Bundle in Staging
# MAGIC %sh
# MAGIC cd "Full Project" 
# MAGIC databricks bundle run health_etl_workflow -t stage

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary - Stage
# MAGIC While the job is running, examine the tasks when using the **stage** target. Note the following:
# MAGIC - Unit tests passed
# MAGIC - Lakeflow Declarative pipeline ETL and integration tests passed on a small sample of 35,000 rows of dev data.
# MAGIC - Visualization was created using the small sample of 35,000 rows of dev data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E3. Production
# MAGIC Here is what the configuration of our target mapping for production looks like in the databricks.yml file.
# MAGIC ```YAML
# MAGIC   production:
# MAGIC     mode: production
# MAGIC     workspace:
# MAGIC       # host: can change host if isolating by workspace
# MAGIC       root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
# MAGIC     variables:
# MAGIC       target_catalog: ${var.catalog_prod}
# MAGIC       raw_data_path: /Volumes/${var.catalog_prod}/default/health
# MAGIC ```
# MAGIC
# MAGIC Here, we'll repeat the same bash commands using `%sh`. However, note that all production compute will run on serverless instead of classic compute, as we're not overriding the default compute.
# MAGIC
# MAGIC You can verify this by deploying the job and inspecting the tasks.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Validate Production
# MAGIC %sh
# MAGIC cd "Full Project" 
# MAGIC databricks bundle validate -t production

# COMMAND ----------

# DBTITLE 1,Deploy to Production
# MAGIC %sh
# MAGIC cd "Full Project" 
# MAGIC databricks bundle deploy -t production

# COMMAND ----------

# DBTITLE 1,Run Bundle in Production
# MAGIC %sh
# MAGIC cd "Full Project" 
# MAGIC databricks bundle run health_etl_workflow -t production

# COMMAND ----------

# MAGIC %md
# MAGIC ## E4. Destroy all bundles

# COMMAND ----------

# MAGIC %sh
# MAGIC cd "Full Project";
# MAGIC databricks bundle destroy -t development --auto-approve;
# MAGIC databricks bundle destroy -t stage --auto-approve;
# MAGIC databricks bundle destroy -t production --auto-approve;

# COMMAND ----------

# MAGIC %md
# MAGIC # Next Steps
# MAGIC
# MAGIC ![ci_cd](./images/ci_cd_overview.png)
# MAGIC Think about how you can use DABs to accelerate development by designing programmatic management of your workflows. With DABs you can create, manage, and deploy your different assets and artifacts in a consistent and repeatable manner for CI/CD workflows. You can continue to learn about DABs by completing the next lab, where you will have a chance to practice attaching a machine learning model to your workflow as a part of the Dev workflow.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
# MAGIC