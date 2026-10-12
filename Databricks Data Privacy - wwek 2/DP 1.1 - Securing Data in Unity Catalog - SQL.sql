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
-- MAGIC # Securing Data in Unity Catalog
-- MAGIC
-- MAGIC In this demo you will learn how to hide sensitive data using 3 different approaches:
-- MAGIC * Views 
-- MAGIC * Dynamic Views
-- MAGIC * Row Filter and Column Masks on Tables (introduced in 2024)
-- MAGIC
-- MAGIC Further, you will also learn data governance features of Unity Catalog
-- MAGIC * Introduction to Catalog Explorer 
-- MAGIC * Enable data access to users using inherited and explicit privileges 
-- MAGIC * Tagging + AI generated Documentation
-- MAGIC * Use Lineage and Insight features in Unity Catalog to understand data flow and access patterns.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REQUIRED - SELECT SERVERLESS
-- MAGIC
-- MAGIC Before executing cells in this notebook, please select Serverless cluster in the lab. Be aware that **Serverless** is enabled by default.
-- MAGIC
-- MAGIC Follow these steps to select the Serverless cluster:
-- MAGIC
-- MAGIC - Navigate to the top-right of this notebook and click the drop-down menu to select Serverless. By default, the notebook will use **Serverless**.

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

-- MAGIC %run ./Includes/Classroom-Setup-1.1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the code below to view your current default catalog and schema. Confirm that they have the same name as the cell above.
-- MAGIC

-- COMMAND ----------

SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Setting up PII data
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B1. Explore the customers_silver table
-- MAGIC
-- MAGIC In the classroom setup above, we created a table named **customers_silver** within the **pii_data** schema in Unity Catalog. This table contains PII data, such as individuals names, addresses, and loyalty scores.
-- MAGIC
-- MAGIC 1. Let's review the customer data in the next cell using the default catalog and schema by specifying just the table name.

-- COMMAND ----------

SELECT * 
FROM customers_silver
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B2. Create the View customers_gold_view
-- MAGIC
-- MAGIC 1. Let's create a view named **customers_gold_view** that presents a processed view of the **customers_silver** table data by averaging the units purchased by each customer.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE VIEW customers_gold_view AS
SELECT 
  customer_id, 
  state, 
  avg(units_purchased) as average_units_purchased, 
  loyalty_segment
FROM customers_silver
GROUP BY customer_id, state, loyalty_segment;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Let's display the view **customers_gold_view**. Confirm it contains aggregated information for each **customer_id**.

-- COMMAND ----------

SELECT * 
FROM customers_gold_view
ORDER BY customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B3. View the Catalog Explorer
-- MAGIC
-- MAGIC Catalog Explorer is a user interface tool that allows users to browse, explore, and manage data assets such as schemas, tables, models, and functions within a data catalog. It provides functionality for data discovery, including viewing schema details, previewing sample data, and exploring entity relationships, as well as management capabilities for catalogs, permissions, and data sharing
-- MAGIC
-- MAGIC 1. Run the below cell to print out the name of your catalog in Unity Catalog.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"Your Catalog Name: {DA.catalog_name}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC To open the Catalog Explorer, complete the following:
-- MAGIC
-- MAGIC 1. Click the **Catalog** icon in the sidebar directly to the left of this notebook. It is the third icon from the top.
-- MAGIC
-- MAGIC 2. Find and expand your catalog name using the information from above.
-- MAGIC
-- MAGIC 3. Expand the schema **pii_data**. This will display a list of available objects, such as tables, views, volumes, and functions.
-- MAGIC
-- MAGIC 4. Expand the **Tables** option if it's available; otherwise, you'll directly see the available tables.
-- MAGIC
-- MAGIC 5. Select the schema **pii_data**, right-click on it, and select *Open in Catalog Explorer* to see detailed information regarding the schema. This will open in a new tab.
-- MAGIC
-- MAGIC 6. In the Catalog Explorer, you will see the available objects in the schema. Select **customers_gold_view**.
-- MAGIC
-- MAGIC 7. In the view, you can see tabs such as **Sample Data**, **Details**, and **Permissions** (which we'll cover shortly).
-- MAGIC
-- MAGIC 8. Leave the Catalog Explorer tab open and return back to this notebook.
-- MAGIC
-- MAGIC
-- MAGIC <br></br>
-- MAGIC **Example**
-- MAGIC
-- MAGIC ![View Catalog Explorer](./Includes/images/view_catalog_explorer.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Controlling access to data
-- MAGIC
-- MAGIC In this section we're going to configure permissions on data objects we created. To keep things simple, we will show you how to grant privileges to everyone. 
-- MAGIC
-- MAGIC If you're working with a group, you can have others in the group test your work by attempting to access your data objects.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C1. Three Level Namespace Query for Your View
-- MAGIC
-- MAGIC The output of the following cell represents a query you could have others in your group run to attempt to access your **customers_gold_view**, using the three-level namespace: **Catalog.Schema.View**.
-- MAGIC
-- MAGIC 1. Run the code below to view the dynamically set `DA.catalog_name` course variable value. Remember, the `DA` object is a Databricks Academy variable set dynamically during the classroom setup scripts for the labs.

-- COMMAND ----------

SELECT DA.catalog_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the query below using just the view name **customers_gold_view**. Notice that this works as well. 
-- MAGIC
-- MAGIC     This is because the classroom setup script sets the default catalog to your catalog, and the default schema to **pii_data** and enables you to simply reference the table name without the catalog and schema.

-- COMMAND ----------

SELECT *
FROM customers_gold_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### C1.1 View Privileges Notes
-- MAGIC If someone else were to run this query, this would currently fail since no privileges have been granted yet. Only you (the owner) can access the view at the current time. **By default, only data owners (and admins) can see the data objects that were just created**. 
-- MAGIC
-- MAGIC In order to access any data objects, users need appropriate permissions for the data object in question (a view, in this case), as well as all containing elements (the schema and catalog).
-- MAGIC
-- MAGIC Unity catalog's security model accommodates two distinct patterns for managing data access permissions:
-- MAGIC
-- MAGIC 1. Granting permissions en masse by taking advantage of Unity Catalog's privilege inheritance.
-- MAGIC
-- MAGIC 1. Explicitly granting permissions to specific objects. This pattern is quite secure, but involves more work to set up and administer.
-- MAGIC
-- MAGIC We'll explore both approaches to provide an understanding of how each one works.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C2. Inherited privileges
-- MAGIC
-- MAGIC As we've seen, securable objects in Unity Catalog are hierarchical and follow a Three Level Namespace. Privileges are inherited downward and using this feature makes it easy to set up default access rules for your data. 
-- MAGIC
-- MAGIC Using privilege inheritance, let's build a permission chain that will allow anyone to access the view **customers_gold_view** and other objects in the same catalog and schema.
-- MAGIC <br></br>
-- MAGIC ```
-- MAGIC
-- MAGIC   GRANT USE CATALOG ON CATALOG ${DA.catalog} TO `account users`;
-- MAGIC
-- MAGIC   GRANT USE SCHEMA,SELECT ON CATALOG ${DA.catalog}.example TO `account users`
-- MAGIC
-- MAGIC ```
-- MAGIC All of these permissions were granted at the catalog level with one single statement. As convenient as this is, there are some very important things to keep in mind with this approach:
-- MAGIC
-- MAGIC * The grantee (everyone, in this case) now has the **`SELECT`** privilege on **all** applicable objects (that is, tables and views) in **all** schemas within the catalog
-- MAGIC
-- MAGIC * This privilege will also be extended to any future tables/views, as well as any future schemas that appear within the catalog
-- MAGIC
-- MAGIC While this can be very convenient for granting access to hundreds or thousands of tables, we must be very careful how we set this up when using privilege inheritance because it's much easier to grant permissions to the wrong things accidentally. Also keep in mind the above approach is extreme. A slightly less permissive compromise can be made, while still leveraging privilege inheritance, with the following two grants. Note, you don't need to run these statements; they're merely provided as an example to illustrate the different types of privilege structures you can create that take advantage of inheritance.
-- MAGIC
-- MAGIC Basically, this pushes the `USE SCHEMA` and `SELECT` down a level, so that grantees only have access to all applicable objects in the newly created schema.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 1. Below is an *example* query to grant permissions to do the following:
-- MAGIC
-- MAGIC   - The ability to use/access your catalog specified by **${DA.catalog_name}**.
-- MAGIC
-- MAGIC   - The ability to use/access any schemas within your catalog.
-- MAGIC
-- MAGIC   - The ability to perform SELECT operations (read data) in your catalog.
-- MAGIC
-- MAGIC   - This allows members of the *account users* group to browse and query data within the catalog and its schemas.
-- MAGIC
-- MAGIC **NOTE: The query result will return an UNAUTHORIZED_ACCESS error. Why? You must be the catalog's owner to grant such privileges, and you are not the owner of the catalog. This shared training workspace and your catalog has been created for you by the admin. Depending on your organization's permissions, you may also encounter this scenario in your environment.**
-- MAGIC
-- MAGIC **With the correct permissions this would enable all account users to access the specified catalog.**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f'GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG {DA.catalog_name} TO `account users`')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the `DESCRIBE CATALOG` statement to view information about the catalog. View the results. Notice that you can view the *Owner* of a catalog. In this example, you are not the owner of the catalog and do not have the ability to grant permissions.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC r = spark.sql(f'DESCRIBE CATALOG {DA.catalog_name}')
-- MAGIC display(r)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C3. Check Privileges in Unity Catalog
-- MAGIC
-- MAGIC There are two ways to check the applied permissions on objects: 
-- MAGIC
-- MAGIC - Catalog Explorer
-- MAGIC - Code

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C3.1 Check Permissions in Catalog Explorer
-- MAGIC
-- MAGIC Check privileges on a catalog using the Catalog Explorer by completing the following steps:
-- MAGIC
-- MAGIC 1. Click the **Catalog** icon in the sidebar, directly to the left of this notebook. It is the third icon from the top.
-- MAGIC
-- MAGIC 2. Find and expand your unique catalog name.
-- MAGIC
-- MAGIC 3. Right-click on your catalog name and select *Open in Catalog Explorer* to view detailed information about the catalog. This will open in a new tab.
-- MAGIC
-- MAGIC 4. In the Catalog Explorer, you will see the available schemas in the catalog.
-- MAGIC
-- MAGIC 5. In the top navigation bar, you will see **Overview**, **Details**, **Permissions**, **Policies**.
-- MAGIC
-- MAGIC 6. Select **Permissions**.
-- MAGIC
-- MAGIC 7. Notice that in **Permissions** you see that you have *ALL PRIVILEGES* on this catalog. *ALL PRIVILEGES* current provides the following:
-- MAGIC
-- MAGIC     - **Prerequisite:** *USE CATALOG, USE SCHEMA*
-- MAGIC     - **Metadata:** *APPLY TAG, BROWSE*
-- MAGIC     - **Read:** *EXECUTE, READ VOLUME, SELECT*
-- MAGIC     - **Edit:** *MODIFY, REFRESH, WRITE VOLUME*
-- MAGIC     - **Create:** *CREATE FUNCTION, CREATE MATERIALIZED VIEW, CREATE MODEL, CREATE SCHEMA, CREATE TABLE, CREATE VOLUME*
-- MAGIC
-- MAGIC For more information view the [Privilege types by securable object in Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html) documentation.
-- MAGIC
-- MAGIC **Example Catalog Explorer Image**
-- MAGIC
-- MAGIC ![Unity Catalog Privileges](./Includes/images/uc_permissions.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C3.2 Check Permissions with Code
-- MAGIC 1. You can also use the [`SHOW GRANTS command`](https://docs.databricks.com/en/sql/language-manual/security-show-grant.html) to check the grants for your catalog. This command is a versatile SQL statement used to display grants about various database objects. In this case, we'll apply it to a catalog following the next syntax: 
-- MAGIC ```
-- MAGIC
-- MAGIC SHOW GRANTS [ principal ] ON securable_object
-- MAGIC
-- MAGIC
-- MAGIC ```
-- MAGIC
-- MAGIC Execute the cell below and view the results.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC r = spark.sql(f'SHOW GRANTS ON CATALOG {DA.catalog_name}')
-- MAGIC display(r)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C4. Grant Explicit privileges on Schema or Objects
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Show privileges on the **pii_data** schema within your catalog. Notice that only you have access to that schema.

-- COMMAND ----------

SHOW GRANTS ON SCHEMA pii_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Show privileges on the **customers_gold_view** view within your catalog's **pii_data** schema. Notice that only you have access to that view.

-- COMMAND ----------

SHOW GRANTS ON VIEW customers_gold_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Using explicit privilege grants on Schema level, let's build a permission chain that will allow anyone to access the **customers_gold_view** view.
-- MAGIC
-- MAGIC **NOTE:** This code will execute successfully because you created the schema and view within the catalog. However, you will need to provide access to the catalog for other users to access the objects within it. In this lab environment, we have restricted you from granting privileges on your catalog.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## This permission grants users the ability to access and interact with the specified catalog, allowing them to see and query schemas within it.
-- MAGIC ## Granting privileges on your catalog will not work in this lab - commented out
-- MAGIC ##
-- MAGIC ## spark.sql(f'GRANT USE CATALOG ON CATALOG {DA.catalog_name} TO `account users`')
-- MAGIC ##
-- MAGIC
-- MAGIC ## This permission grants the account users group the ability to access and interact with the pii_data schema in the specified catalog.
-- MAGIC spark.sql(f'GRANT USE SCHEMA ON SCHEMA {DA.catalog_name}.pii_data TO `account users`')
-- MAGIC
-- MAGIC ## This permission grants the account users group the ability to query (select data from) the customers_gold_view view in the pii_data schema of the specified catalog.
-- MAGIC spark.sql(f'GRANT SELECT ON VIEW {DA.catalog_name}.pii_data.customers_gold_view TO `account users`')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC With these grants in place (if you had permission to grant all access), if anyone else were to query the view again, the query still succeeds because all the appropriate permissions are in place; we've just taken a very different approach to establishing them.
-- MAGIC
-- MAGIC This seems more complicated. One statement from earlier has been replaced with three, and this only provides access to a single view. 
-- MAGIC
-- MAGIC Following this pattern, we'd have to do an additional `SELECT` grant for each additional table or view we wanted to permit. But this complication comes with the benefit of security. Now, user can only read the *gold* view, but nothing else. There's no chance they could accidentally get access to some other object. So this is very explicit and secure, but one can imagine it would be very cumbersome when dealing with lots of tables and views.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C5. Check Explicit privileges

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### C5.1 Show Schema Privileges

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Similarly to previous steps, we'll leverage the `SHOW GRANTS` statement again to check the explicit privileges granted to your schema **pii_data**.
-- MAGIC
-- MAGIC     Notice that in the results `account users` have been added to the **pii_data** schema.

-- COMMAND ----------

SHOW GRANTS ON SCHEMA pii_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C5.2 Show View Privileges

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. View the privileges on the **customers_gold_view**. Notice that in the results, `account users` have been granted access to the view **customers_gold_view**.
-- MAGIC

-- COMMAND ----------

SHOW GRANTS ON VIEW pii_data.customers_gold_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. View the grants on your catalog. Notice that `account users` does not have access to grant access to the catalog.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC r = spark.sql(f'SHOW GRANTS ON CATALOG {DA.catalog_name}')
-- MAGIC display(r)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C6. Revoking Privileges
-- MAGIC
-- MAGIC No data governance platform would be complete without the ability to revoke previously issued grants. In preparation for testing the next approach to granting privileges, let's unwind what we just did using `REVOKE`.
-- MAGIC
-- MAGIC 1. Revoke the usage of `account users` on your schema.

-- COMMAND ----------

REVOKE USAGE ON SCHEMA pii_data FROM `account users`;
REVOKE SELECT ON VIEW pii_data.customers_gold_view FROM `account users`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. After revoking the access to our schema, you check the permissions again by running the query below.
-- MAGIC
-- MAGIC

-- COMMAND ----------

SHOW GRANTS ON SCHEMA pii_data;

-- COMMAND ----------

SHOW GRANTS ON VIEW pii_data.customers_gold_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C7. Views versus tables
-- MAGIC
-- MAGIC We've explored two different approaches to managing permissions, and we have now shown how permissions can configured such that anyone can access the **customers_gold_view** view, which processes and displays data from the **customers_silver** table. 
-- MAGIC
-- MAGIC But suppose someone else were to try to directly access the **customers_silver** table. This could be accomplished by replacing **customers_gold_view** in the previous query with **customers_silver**.
-- MAGIC
-- MAGIC With explicit privileges in place, the query would fail. How then, does the query against the **customers_gold_view** view work? Because the view's **owner** has appropriate privileges on the **customers_silver** table (through ownership). This property gives rise to interesting applications of views in table security, which we cover in the next section.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Protecting columns and rows
-- MAGIC
-- MAGIC Databricks provides several options for protecting columns and rows, in this section we'll use the **customers_silver** table as a source for creating **dynamic views** and applying **row filtering and column masks** to protect sensitive data:
-- MAGIC - Dynamic View: **customers_gold_dynamic_view**
-- MAGIC - Table: **customers_silver_with_row_filter_and_column_masks** with `ROW_FILTER` and `MASK COLUMN`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D1. Preview the customers_silver table
-- MAGIC
-- MAGIC 1. Run the query and view the **customers_silver** table.
-- MAGIC
-- MAGIC     Notice that the **customers_silver** table contains detailed customer information, including various attributes such as customer ID, name, contact details, and loyalty segment.

-- COMMAND ----------

SELECT *
FROM customers_silver
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. We'll use the **loyalty_segment** column to filter rows in our dynamic view and table with ROW_FILTER. 
-- MAGIC
-- MAGIC     Run the query below to view distinct values in the **loyalty_segment** column. Notice that currently, four values are shown in the cell below.

-- COMMAND ----------

SELECT DISTINCT(loyalty_segment)
FROM customers_silver
ORDER BY loyalty_segment DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Count the number of rows in the **customers_silver** table. Confirm the table contains *28,813* rows.

-- COMMAND ----------

SELECT count(*) as TotalRows
FROM customers_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D2. Dynamic Views
-- MAGIC
-- MAGIC We have seen that Unity Catalog's treatment of views provides the ability for views to protect access to tables; users can be granted access to views that manipulate, transform, or obscure data from a source table, without needing to provide direct access to the source table.
-- MAGIC
-- MAGIC Dynamic views provide the ability to do fine-grained access control of columns and rows within a table, conditional on the principal running the query. Dynamic views are an extension to standard views that allow us to do things like:
-- MAGIC * partially obscure column values or redact them entirely
-- MAGIC * omit rows based on specific criteria
-- MAGIC
-- MAGIC Access control with dynamic views is achieved through the use of functions within the definition of the view. These functions include:
-- MAGIC * `current_user()`: returns the email address of the user querying the view
-- MAGIC * `is_account_group_member()`: returns TRUE if the user querying the view is a member of the specified group
-- MAGIC * `is_member()`: returns TRUE if the user querying the view is a member of the specified workspace-local group
-- MAGIC
-- MAGIC **NOTE:** Databricks generally advises against using the `is_member()` function in production, since it references workspace-local groups and hence introduces a workspace dependency into a metastore that potentially spans multiple workspaces.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below to use the `is_account_group_member()` function to check if you are part of the `supervisors` group.
-- MAGIC
-- MAGIC     Notice that it returns the value *false*, indicating that you are not part of the `supervisors` group.

-- COMMAND ----------

SELECT is_account_group_member('supervisors')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D3. Redacting 'Customer ID' and hiding 'Loyalty > 2' customers
-- MAGIC
-- MAGIC Suppose we want everyone to be able to see aggregated data trends from the **customers_silver** table, but we don't want to disclose customer PII to everyone. 
-- MAGIC
-- MAGIC Let's create a view to redact the **customer_id** column and hide high loyalty customers, so that only members of `supervisors` group can see it using the `is_account_group_member()` function.
-- MAGIC
-- MAGIC Column redactions are performed using `CASE` statements and row filtering is done by applying the conditional as a `WHERE` clause.
-- MAGIC
-- MAGIC This view performs the following:
-- MAGIC - This view dynamically redacts **customer_id** based on the user's group membership.
-- MAGIC - `Supervisors` see the actual customer_id, others see 9999999.
-- MAGIC - Non-supervisors only see customers with a **loyalty_segment** less than *3*.
-- MAGIC - The view calculates the average units purchased and includes **state** and **loyalty_segment**.

-- COMMAND ----------

CREATE OR REPLACE VIEW customers_gold_dynamic_view AS
SELECT 
  CASE WHEN        -- Redact customer_id column if user is not a supervisor
    is_account_group_member('supervisors') THEN customer_id 
    ELSE 9999999
  END AS customer_id,
  state, 
  avg(units_purchased) as average_units_purchased, 
  loyalty_segment
FROM customers_silver
WHERE
  CASE WHEN          -- Redact rows where loyalty_segment 3 or above if the user is not a supervisor
    is_account_group_member('supervisors') THEN TRUE  -- When true, return all rows
    ELSE loyalty_segment < 3                          -- When false, return rows less than 3
  END
GROUP BY customer_id, state, loyalty_segment
ORDER BY customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### D4. Dynamic View Results
-- MAGIC
-- MAGIC 1. Now, let's query the view. Notice the **customer_id** column is now *9999999* and displaying only rows with **loyalty_segment** below 3

-- COMMAND ----------

SELECT * 
FROM customers_gold_dynamic_view
ORDER BY loyalty_segment DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Count the number of rows in the **customers_gold_dynamic_view** dynamic view. Confirm the table contains 19,176 rows (original table had 28,813), filtering out rows that you are not allowed to view because you are not part of the `supervisors` group.

-- COMMAND ----------

SELECT count(*) as TotalRows
FROM customers_gold_dynamic_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Does this output surprise you?**
-- MAGIC
-- MAGIC As the owner of the view and table, you do not need any privileges to access these objects, yet when querying the view, we see redacted columns. This is because of the way the view is defined. As a regular user (one who is not a member of the `supervisors` group), the **customer_id** column is redacted and the high loyalty customer records are hidden.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## E. Row Filters and Column Masks (Introduced in 2024) 
-- MAGIC
-- MAGIC This newly introduced feature enables data owners to mask columns and hide rows in similar ways to Dynamic Views - without having to create another data object.
-- MAGIC
-- MAGIC - **Row filters** allow you to apply a filter to a table so that queries return only rows that meet the filter criteria. You implement a row filter as a SQL user-defined function (UDF). Python and Scala UDFs are also supported, but only when they are wrapped in a SQL UDF.
-- MAGIC
-- MAGIC - **Column masks** let you apply a masking function to a table column. The masking function gets evaluated at query runtime, substituting each reference of the target column with the results of the masking function. For most use cases, column masks determine whether to return the original column value or redact it based on the identity of the invoking user. Column masks are expressions written as SQL UDFs or as Python or Scala UDFs that are wrapped in a SQL UDF.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E1. Creating a Row Filter
-- MAGIC
-- MAGIC 1. To create a row filter, you need to write a UDF to define the filter policy and then apply it to a table with an ALTER TABLE statement. Alternatively, you can specify a row filter for a table in the initial CREATE TABLE statement. Each table can have only one row filter. A row filter accepts zero or more input parameters where each input parameter binds to one column of the corresponding table.
-- MAGIC
-- MAGIC     Our UDF will leverage `is_account_group_member()` function to evaluate if the current user is a member of the 'supervisors' account group. Run the code and view the output. Again, you are not part of the group.

-- COMMAND ----------

SELECT is_account_group_member('supervisors')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Let's create the UDF `loyalty_row_filter` using the `is_account_group_member` function to drive the behavior for row filtering: if the user is not a member of the group, it will filter the **loyalty_segment** column for values less than 3.

-- COMMAND ----------

DROP FUNCTION IF EXISTS loyalty_row_filter;

CREATE OR REPLACE FUNCTION loyalty_row_filter(loyalty_segment STRING)
RETURNS BOOLEAN
RETURN IF(is_account_group_member('supervisors'), true, loyalty_segment < 3);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Now lets create the table **customers_silver_with_row_filter_and_column_masks** from the **customers_silver** table and view it's results. 
-- MAGIC
-- MAGIC     Notice since the `ROW_FILTER` from above is not applied yet, the query below will retrieve and include all rows for a total of 28,670 rows.

-- COMMAND ----------

-- Drop the table if it exists for demo purposes
DROP TABLE IF EXISTS customers_silver_with_row_filter_and_column_masks;

-- Create a new table and apply the row filter
CREATE OR REPLACE TABLE customers_silver_with_row_filter_and_column_masks AS 
SELECT 
  customer_id,
  state, 
  avg(units_purchased) as average_units_purchased, 
  loyalty_segment
FROM customers_silver
GROUP BY customer_id, state, loyalty_segment
ORDER BY customer_id;


-- View the new table
SELECT count(*) AS TotalRows
FROM customers_silver_with_row_filter_and_column_masks;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Now let's assign the `loyalty_row_filter` function with `WITH ROW FILTER` using an `ALTER TABLE` statement on the table `customers_silver_with_row_filter_and_column_masks` to the **loyalty_segment** column.
-- MAGIC
-- MAGIC     [ROW FILTER clause](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-row-filter.html#row-filter-clause) documentation.

-- COMMAND ----------

ALTER TABLE customers_silver_with_row_filter_and_column_masks 
SET ROW FILTER loyalty_row_filter ON (loyalty_segment);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Let's confirm the `ROW FILTER` has been applied to the table with a [`INFORMATION_SCHEMA.ROW_FILTERS`](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/row_filters) Clause.
-- MAGIC
-- MAGIC     Run the query and review the results. Scroll to the right to the **Row Filter** row in the **target_columns** column and verify that a row filter has been applied.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC     SELECT *
-- MAGIC     FROM information_schema.row_filters
-- MAGIC     WHERE table_catalog = '{DA.catalog_name}'
-- MAGIC     AND table_schema    = 'pii_data'
-- MAGIC     AND table_name      = 'customers_silver_with_row_filter_and_column_masks'
-- MAGIC     """
-- MAGIC ).display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. Let's confirm that the `ROW_FILTER` is working by applying a `DISTINCT` on the **loyalty_segment** column in the table. The results should be between *0* and *2*.

-- COMMAND ----------

SELECT distinct(loyalty_segment)
FROM customers_silver_with_row_filter_and_column_masks
ORDER BY loyalty_segment DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Let's count the total number of rows in the **customers_silver_with_row_filter_and_column_masks** table. Notice that the table now contains *19,176* rows instead of the original *28,670*, with rows filtered out because you are not part of the assigned `supervisors` group.
-- MAGIC

-- COMMAND ----------

SELECT count(*) AS TotalRows
FROM customers_silver_with_row_filter_and_column_masks;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E2. Create a Column Mask
-- MAGIC
-- MAGIC To apply column masks, create a UDF and apply it to a table column using an `ALTER TABLE` statement.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Let's create the function `redact_customer_id` and use the `is_account_group_member` function to control its behavior. If `is_account_group_member` returns `true`, it returns the actual **customer_id**. Otherwise, it returns the value *9999999*.
-- MAGIC

-- COMMAND ----------

DROP FUNCTION IF EXISTS redact_customer_id;

CREATE OR REPLACE FUNCTION redact_customer_id(customer_id BIGINT)
RETURN CASE WHEN is_account_group_member('supervisors') 
  THEN customer_id 
  ELSE 9999999
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Now, let's assign the `redacted_customer_id` function using `SET MASK` in an `ALTER TABLE` statement for the table **customers_silver_with_row_filter_and_column_masks**.
-- MAGIC
-- MAGIC     [Column mask clause](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-column-mask.html#column-mask-clause) documentation.
-- MAGIC
-- MAGIC     [ALTER COLUMN clause](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-alter-table-manage-column.html#alter-column-clause) documentation.
-- MAGIC

-- COMMAND ----------

ALTER TABLE customers_silver_with_row_filter_and_column_masks
  ALTER COLUMN customer_id 
  SET MASK redact_customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Let's confirm the `COLUMN MASK` has been applied to the table with a [`INFORMATION_SCHEMA.COLUMN_MASKS`](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/column_masks) Clause.
-- MAGIC
-- MAGIC     Run the query and view the results. Scroll to the right **Column Mask** row in the **column_name** and **mask_name**. You should notice that a column mask has been applied.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC     SELECT *
-- MAGIC     FROM information_schema.column_masks
-- MAGIC     WHERE table_catalog = '{DA.catalog_name}'
-- MAGIC     AND table_schema    = 'pii_data'
-- MAGIC     AND table_name      = 'customers_silver_with_row_filter_and_column_masks'
-- MAGIC """).display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E3. Table with Row Filtering and Column Mask Results
-- MAGIC
-- MAGIC 1. Now, let's query the table. You should notice that the column masking in **customer_id** shows the value *9999999*, and the row filtering displays only rows where the **loyalty_segment** is below 3.
-- MAGIC

-- COMMAND ----------

SELECT * 
FROM customers_silver_with_row_filter_and_column_masks
ORDER BY loyalty_segment DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## F. Compare Results of Dynamic Views with Row Filters and Column Masks
-- MAGIC
-- MAGIC The query below provides a quick row count comparison between the two tables we've created, with matching values, to demonstrate that both can achieve the same results: **customers_gold_dynamic_view** and **customers_silver_with_row_filter_and_column_masks**.
-- MAGIC
-- MAGIC 1. Run the query and view the results. You should notice that the view and table return the same row count.

-- COMMAND ----------

SELECT 
  (SELECT count(*) FROM customers_silver_with_row_filter_and_column_masks) = 
  (SELECT count(*) FROM customers_gold_dynamic_view) AS equal_row_count

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### F1. Dynamic Views vs Row Filters - Discuss
-- MAGIC
-- MAGIC - Dynamic views, row filters, and column masks all let you apply complex logic to tables and process their filtering decisions at query runtime.
-- MAGIC
-- MAGIC - Use dynamic views if you need to apply transformation logic such as filters and masks to read-only tables, and if it is acceptable for users to refer to the dynamic views using different names. 
-- MAGIC
-- MAGIC - Use row filters and column masks if you want to filter or compute expressions over specific data but still provide users access to the tables using their original names.
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## G. Tagging 
-- MAGIC Tags are attributes with keys and optional values that can be applied to securable objects in Unity Catalog to organize and categorize them.
-- MAGIC
-- MAGIC - Supported objects for tagging include catalogs, schemas, tables, columns, volumes, views, registered models, and model versions.
-- MAGIC
-- MAGIC - Tags simplify search and discovery of tables and views using workspace search functionality.
-- MAGIC
-- MAGIC - You can assign up to 20 tags per object, with key length up to 255 characters and value length up to 1000 characters.
-- MAGIC
-- MAGIC - Tags can be added and managed through Catalog Explorer UI or SQL commands (for Databricks Runtime 13.3+).
-- MAGIC
-- MAGIC - Tags can be used for data classification, security, lifecycle management, compliance, and project management.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### G.1 Set Tags to Table and Column
-- MAGIC Let's set a tag to the **customers_silver** table, this can be achieved using an `ALTER TABLE` statement or via the UI in Catalog Explorer. 
-- MAGIC
-- MAGIC **NOTE:** Remember this can be set to other objects stated above (catalogs, schemas, columns, etc):

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### G2. Tagging via ALTER TABLE

-- COMMAND ----------

-- TABLE TAGS
ALTER TABLE customers_silver 
SET TAGS (
  'quality'='silver',
  'domain'='customer'
  );


-- COLUMN TAGS
ALTER TABLE customers_silver 
  ALTER COLUMN customer_id SET TAGS ("compliance" = "GDPR");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### G3. Tagging via Catalog Explorer
-- MAGIC
-- MAGIC 1. Using the Catalog Explorer, navigate to the **customers_silver** table in your catalog within the **pii_schema**. 
-- MAGIC
-- MAGIC     In the **Overview** tab, you will find the "Tagging" section on the right side of the panel. Notice the tags we defined in the previous cell
-- MAGIC
-- MAGIC **Example Tag in Catalog Explorer**
-- MAGIC
-- MAGIC ![Tagging in Catalog Explorer](./Includes/images/uc_tag.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## H. Discoverability
-- MAGIC
-- MAGIC Unity Catalog offers robust data discovery capabilities, allowing users to easily search for and locate data assets across their organization. The platform provides a structured way to tag, document, and manage metadata, enabling comprehensive search functionality that utilizes lineage information and enforces security based on user permissions
-- MAGIC
-- MAGIC There are two ways to leverage tags for discoverability:

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### H1. Search Bar
-- MAGIC
-- MAGIC 1. Using the syntax such as `tag:value`. In our example should be `domain:customer`. The more tags added, the finer the results.
-- MAGIC
-- MAGIC     Run the query below and copy the values and paste them into the search bar on top and press enter to see the results. 
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC **Example Results**
-- MAGIC
-- MAGIC ![Discoverability](./Includes/images/search_bar.png)
-- MAGIC
-- MAGIC **NOTE:** Please take into consideration the image may have a different catalog name from yours.

-- COMMAND ----------

SELECT concat('catalog:',DA.catalog_name,' ','domain:customer') as use_in_search_bar

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### H2. Queries
-- MAGIC
-- MAGIC Alternatively, run the query below leveraging the `INFORMATION_SCHEMA.TABLE_TAGS` filtering with the `customers_silver` table.
-- MAGIC Be aware of the tables below to retrieve tags from the different objects:
-- MAGIC
-- MAGIC - `INFORMATION_SCHEMA.CATALOG_TAGS`
-- MAGIC - `INFORMATION_SCHEMA.SCHEMA_TAGS`
-- MAGIC - `INFORMATION_SCHEMA.TABLE_TAGS`
-- MAGIC - `INFORMATION_SCHEMA.COLUMN_TAGS`
-- MAGIC - `INFORMATION_SCHEMA.VOLUME_TAGS`
-- MAGIC

-- COMMAND ----------

SELECT * 
FROM INFORMATION_SCHEMA.TABLE_TAGS
WHERE TABLE_NAME = 'customers_silver'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## I. Lineage
-- MAGIC
-- MAGIC 1. Data lineage is a key pillar of any data governance solution. In the **Lineage** tab, we can identify elements that are related to the selected object:
-- MAGIC * With **Upstream** selected, we see objects that gave rise to this object, or that this object uses. This is useful for tracing the source of your data.
-- MAGIC * With **Downstream** selected, we see objects that are using this object. This is useful for performing impact analyses.
-- MAGIC * The lineage graph provides a visualization of the lineage relationships.
-- MAGIC
-- MAGIC You can access the lineage of a table in Catalog Explorer by selecting your table, in this case **customers_silver**, in the _"lineage"_ tab there is a button _"see lineage graph"_ to display the results shown below. 
-- MAGIC
-- MAGIC **Example**
-- MAGIC
-- MAGIC **Note:** Take into consideration some information such as catalog won't match your view.
-- MAGIC
-- MAGIC ![Lineage](./Includes/images/lineage.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## J. AI generated Documentation
-- MAGIC
-- MAGIC AI-generated documentation for Unity Catalog enables automatically generate descriptions for tables and columns. The feature uses a custom-built large language model (LLM) to generate metadata based on table schemas and column names.
-- MAGIC
-- MAGIC - Available for catalogs, schemas, tables, columns, functions, models, and volumes.
-- MAGIC - Saves time and reduces manual effort in documenting data assets.
-- MAGIC - Improves search functionality within Databricks workspaces.
-- MAGIC - Users need appropriate permissions (object owner or MODIFY privilege) to view, edit, and save AI-generated comments.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### J1. Tables
-- MAGIC 1. We'll auto-generate concise and informative table and column comments for Unity Catalog, leveraging DatabricksIQ. 
-- MAGIC
-- MAGIC     In the Catalog Explorer search for the "customers_silver" table and in the "Overview" Tab, DatabricksIQ will suggest a "_AI Suggested Description_", you can edit and adjust it as per your needs or accept such a recommendation. Also is possible to update the description later if needed.
-- MAGIC
-- MAGIC **Example**
-- MAGIC
-- MAGIC ![AI Generated Table Description](./Includes/images/table_ai_desc.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### J2. Columns
-- MAGIC
-- MAGIC 1. In the same manner with the table description, under the "Overview" tab, below where the schema is presented. There is a button "AI Generate", after clicked, DatabricksIQ will generate a description for each column as shown in the image below, you can also revert or adjust as needed.
-- MAGIC
-- MAGIC **Example**
-- MAGIC
-- MAGIC
-- MAGIC ![AI Generated Column Description](./Includes/images/table_column_ai_desc.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## K. Insights
-- MAGIC You can use the Insights tab in Catalog Explorer to view the most frequent recent queries and users of any table registered in Unity Catalog. The Insights tab reports on frequent queries and user access for the past 30 days.
-- MAGIC
-- MAGIC You must have the following permissions to view frequent queries and user data on the Insights tab.
-- MAGIC * SELECT privilege on the table.
-- MAGIC * USE SCHEMA privilege on the table’s parent schema.
-- MAGIC * USE CATALOG privilege on the table’s parent catalog.
-- MAGIC
-- MAGIC Metastore admins have these privileges by default.
-- MAGIC
-- MAGIC In the Insights tab for a table, you can view 
-- MAGIC 1. Frequently used queries and notebooks
-- MAGIC 1. Frequently used dashboards
-- MAGIC 1. Frequent Users
-- MAGIC 1. Other tables frequently joined with the table in question
-- MAGIC
-- MAGIC The Insights tab can also help identify tables that are no longer used by applications. These tables can then be tagged for future cleanup.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>

-- COMMAND ----------

