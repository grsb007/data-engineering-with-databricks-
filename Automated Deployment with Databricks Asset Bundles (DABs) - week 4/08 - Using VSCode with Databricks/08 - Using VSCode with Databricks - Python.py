# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 08 Bonus - Using VSCode with Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Open a simple text editor on your computer. Paste the Databricks URL and Personal Access Token (PAT) in the text editor to use for authentication with VSCode.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the following cell to display your Databricks lab URL. Leave this cell open as you will need it to authenticate to Databricks with VSCode.
# MAGIC
# MAGIC     Paste the URL into your text editor.

# COMMAND ----------

lab_databricks_url = f'{spark.conf.get("spark.databricks.workspaceUrl")}/'
print(lab_databricks_url)

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Complete the following steps to create a PAT in Databricks for use with VSCode.
# MAGIC
# MAGIC    a. First, create a personal access token (PAT) for use with the Databricks CLI in the Databricks Academy Lab.
# MAGIC
# MAGIC    b. Click on your username in the top bar, right-click on **User Settings** from the drop-down menu and select *Open in a New Tab*.
# MAGIC
# MAGIC    c. In **Settings**, select **Developer**, then, to the left of **Access tokens**, select **Manage**.
# MAGIC
# MAGIC    d. Click **Generate new token**.
# MAGIC
# MAGIC    e. Specify the following:
# MAGIC       - A comment describing the purpose of the token (e.g., *CLI Demo*).
# MAGIC       - The lifetime of the token; estimate the number of days you anticipate needing to complete this module.
# MAGIC
# MAGIC    f. Click **Generate**.
# MAGIC
# MAGIC    g. Copy the displayed token to the clipboard. You will not be able to view the token again. If you lose the token, you will need to delete it and create a new one.
# MAGIC
# MAGIC    h. Paste the PAT in your text editor below the Databricks URL.
# MAGIC
# MAGIC    **NOTE:** Sometimes, Vocareum has issues with the copy button. Highlight your PAT and copy it manually. Confirm it was copied successfully.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### **NOTE:** Make sure to open the VSCode lab in a new tab. You will need both the Workspace and VSCode open.
# MAGIC
# MAGIC 4. To access VSCode in your lab environment, navigate to the drop-down menu, right-click on **VSCode**, and select *Open in a New Tab*. Leave your text editor open, as you will need the URL and PAT.
# MAGIC
# MAGIC    Follow the provided instructions in VSCode.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ![VSCode Access](./images/vscode_lab.png)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
# MAGIC