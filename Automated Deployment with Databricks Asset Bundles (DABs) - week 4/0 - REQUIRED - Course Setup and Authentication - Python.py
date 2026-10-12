# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 0 - REQUIRED - Course Setup and Authentication
# MAGIC
# MAGIC This notebook will set up your lab environment for this course. You must run this notebook to create the necessary catalogs, data and authentication for the Databricks CLI.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Note**: If you see the warning `Warn: Failed to load git info from /api/2.0/workspace/get-status` while **validating**, **deploying**, or **running** a Databricks Asset Bundle (DAB) during this course, you can safely ignore it and proceed.

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
# MAGIC It will create the following catalogs, volumes and files:
# MAGIC
# MAGIC - Catalog: **username_1_dev**
# MAGIC   - Schema: **default**
# MAGIC     - Volume: **health**
# MAGIC       - *dev_health.csv* : Small subset of prod data, anonymized *PII*, 7,500 rows
# MAGIC
# MAGIC - Catalog: **username_2_stage**
# MAGIC   - Schema: **default**
# MAGIC     - Volume: **health**
# MAGIC       - *stage_health.csv* : Subset of prod data, 35,000 rows
# MAGIC
# MAGIC - Catalog: **username_3_prod**
# MAGIC   - Schema: **default**
# MAGIC     - Volume: **health**
# MAGIC       - *2025-01-01_health.csv*
# MAGIC       - *2025-01-02_health.csv*
# MAGIC       - *2025-01-03_health.csv*
# MAGIC       - CSV files are added to this cloud storage location daily.
# MAGIC
# MAGIC
# MAGIC
# MAGIC **NOTES:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-0-catalog-setup-REQUIRED

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore Your Environment
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Let's quickly explore the course folder structure and files.
# MAGIC
# MAGIC     a. In the left navigation bar, select the folder icon.
# MAGIC
# MAGIC     b. Ensure you are in the main course folder named **Automated Deployment with Databricks Asset Bundles**.
# MAGIC     
# MAGIC     c. In the main course folder, you will see various folders, each folder contains specific files for each demonstration or lab.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Manually view your catalogs and data for this course.
# MAGIC
# MAGIC     a. In the navigation bar on the left, select the catalog icon.
# MAGIC
# MAGIC     b. Confirm the classroom setup script has created three new catalogs for you:
# MAGIC       - **unique_name_1_dev**
# MAGIC       - **unique_name_2_stage**
# MAGIC       - **unique_name_3_prod**
# MAGIC
# MAGIC     c. Expand each catalog and notice a volume named **health** was created and contains one or more CSV files for the specific environment (development, stage, and production).

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Throughout the course, the following Python variables will be used to dynamically reference your course catalogs:
# MAGIC     - `DA.catalog_dev`
# MAGIC     - `DA.catalog_stage`
# MAGIC     - `DA.catalog_prod`
# MAGIC
# MAGIC     Run the code below and confirm the variables refer to your catalog names.
# MAGIC

# COMMAND ----------

print(f'DA.catalog_dev: {DA.catalog_dev}')
print(f'DA.catalog_stage: {DA.catalog_stage}')
print(f'DA.catalog_prod: {DA.catalog_prod}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Generate Tokens for Authentication to the Databricks CLI
# MAGIC In this section, you will generate credentials for working with the Databricks CLI for this course.
# MAGIC
# MAGIC Throughout the course, we will use the Databricks CLI from within a notebook. Since we are in a learning environment, we will save a credentials file in the workspace within a folder called **var_your_user_name**.
# MAGIC
# MAGIC **NOTE: In the "real world," we recommend that you follow your organization's security policies for storing credentials. Do not store your credentials in a file.**
# MAGIC
# MAGIC View the [Secret management](https://docs.databricks.com/en/security/secrets/index.html) documentation for more information on using secrets.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### C1. Store the Databricks CLI Credentials
# MAGIC
# MAGIC A token is just like a username and password, so you should treat it with the same level of security as your own credentials. If you ever suspect a token has leaked, you should delete it immediately.
# MAGIC
# MAGIC For the purpose of this training, we are going to create a landing pad in this notebook to record and store the credentials within the workspace. When using credentials in production, you will want to follow the security practices of your organization. 
# MAGIC
# MAGIC Run the following cell. Notice that it creates two text fields which you will populate in the next section.
# MAGIC
# MAGIC **NOTES:** 
# MAGIC - In your organization, you do not want to store your PAT in a file. Instead, you want to use secret scopes. For the purpose of the lab, we are storing the PAT in a folder called **var_your_user_name** for demonstration purposes. Please follow all company security best practices.

# COMMAND ----------

DA.get_credentials()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### C2. Generate Credentials
# MAGIC Create an personal access token (PAT) for use with the Databricks CLI in the Databricks Academy Lab. 
# MAGIC
# MAGIC 1. Click on your username in the top bar, right-click on **User Settings** from the drop-down menu, and select *Open in a New Tab*.
# MAGIC
# MAGIC 2. In **Settings**, select **Developer**, then to the left of **Access tokens**, select **Manage**.
# MAGIC
# MAGIC 3. Click **Generate new token**.
# MAGIC
# MAGIC 4. Specify the following:
# MAGIC    - A comment describing the purpose of the token (e.g., *CLI Demo*).
# MAGIC    - The lifetime of the token; estimate the number of days you anticipate needing to complete this module.
# MAGIC
# MAGIC 5. Click **Generate**.
# MAGIC
# MAGIC 6. Copy the displayed token to the clipboard. You will not be able to view the token again. If you lose the token, you will need to delete it and create a new one.
# MAGIC
# MAGIC **NOTE:** Sometimes vocareum has issues with the copy button. So highlight your PAT and copy it. Confirm it was copied successfully.
# MAGIC
# MAGIC 7. Paste the token into the **Token** field above.
# MAGIC
# MAGIC 8. The host value should already be populated for you.
# MAGIC
# MAGIC 9. Click outside of the cell. You should see the result *Credentials stored (132 bytes written).*
# MAGIC
# MAGIC 10. In the main course folder **Automated Deployment with Databricks Asset Bundles** you should see a new folder called **var_your_user_name**.
# MAGIC - **NOTE:** You might have to refresh the folder. You can do that by selecting any folder and then navigating back to the main course folder.
# MAGIC
# MAGIC
# MAGIC **NOTES:** 
# MAGIC - Personal Access Token (PAT) must be enabled in the workspace.
# MAGIC - Typically, use secret scopes. For the purpose of the course, we are storing the PAT in a folder called **var_your_user_name** for demonstration purposes.

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to load the credentials. These values will be recorded as follows:
# MAGIC - In the environment variables **`DATABRICKS_HOST`** and **`DATABRICKS_TOKEN`** for [authentication](https://docs.databricks.com/en/dev-tools/auth/index.html) by the Databricks CLI used in subsequent notebooks.
# MAGIC - Since environment variables are limited in scope to the current execution context, the values are persisted to a [file in your workspace](https://docs.databricks.com/en/files/workspace.html#) for use by subsequent notebooks.

# COMMAND ----------

creds = DA.load_credentials()

# COMMAND ----------

# MAGIC %md
# MAGIC Install the CLI to use with a Databricks Notebook. For this course, we will install it using a notebook we setup for you.
# MAGIC
# MAGIC For more information to install the Databricks CLI, view the [Install or update the Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) documentation.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-Common-Install-CLI

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to view the version of the CLI. You should see that you are using **Databricks CLI v0.257.0**.
# MAGIC
# MAGIC
# MAGIC **NOTE:** View the documentation on running shell commands in Databricks [Code languages in notebooks](https://docs.databricks.com/aws/en/notebooks/notebooks-code#code-languages-in-notebooks).

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks -v

# COMMAND ----------

# MAGIC %md
# MAGIC Run the `--help` command to view documentation for the Databrick CLI.
# MAGIC
# MAGIC [Databricks CLI commands](https://docs.databricks.com/en/dev-tools/cli/commands.html#databricks-cli-commands) documentation.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks --help

# COMMAND ----------

# MAGIC %md
# MAGIC View help for commands to manage catalogs.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks catalogs --help

# COMMAND ----------

# MAGIC %md
# MAGIC View available catalogs using the Databricks CLI.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks catalogs list

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
# MAGIC

# COMMAND ----------

