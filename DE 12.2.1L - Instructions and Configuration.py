# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## End-to-End ETL in the Lakehouse
# MAGIC 
# MAGIC In this notebook, you will pull together concepts learned throughout the course to complete an example data pipeline.
# MAGIC 
# MAGIC The following is a non-exhaustive list of skills and tasks necessary to successfully complete this exercise:
# MAGIC * Using Databricks notebooks to write queries in SQL and Python
# MAGIC * Creating and modifying databases, tables, and views
# MAGIC * Using Auto Loader and Spark Structured Streaming for incremental data processing in a multi-hop architecture
# MAGIC * Using Delta Live Table SQL syntax
# MAGIC * Configuring a Delta Live Table pipeline for continuous processing
# MAGIC * Using Databricks Jobs to orchestrate tasks from notebooks stored in Repos
# MAGIC * Setting chronological scheduling for Databricks Jobs
# MAGIC * Defining queries in Databricks SQL
# MAGIC * Creating visualizations in Databricks SQL
# MAGIC * Defining Databricks SQL dashboards to review metrics and results

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Run Setup
# MAGIC Run the following cell to reset all the databases and directories associated with this lab.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-12.2.1L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Land Initial Data
# MAGIC Seed the landing zone with some data before proceeding.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Create and Configure a DLT Pipeline
# MAGIC **NOTE**: The main difference between the instructions here and in previous labs with DLT is that in this instance, we will be setting up our pipeline for **Continuous** execution in **Production** mode.

# COMMAND ----------

print_pipeline_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Click the **Jobs** button on the sidebar.
# MAGIC 1. Select the **Delta Live Tables** tab.
# MAGIC 1. Click **Create Pipeline**.
# MAGIC 1. Fill in a **Pipeline Name** - because these names must be unique, we suggest using the **Pipline Name** provided in the cell above.
# MAGIC 1. For **Notebook Libraries**, use the navigator to locate and select the notebook **DE 12.2.2L - DLT Task**.
# MAGIC     * Alternatively, you can copy the **Notebook Path** specified above and paste it into the field provided.
# MAGIC 1. Configure the Source
# MAGIC     * Click **`Add configuration`**
# MAGIC     * Enter the word **`source`** in the **Key** field
# MAGIC     * Enter the **Source** value specified above to the **`Value`** field
# MAGIC 1. In the **Target** field, specify the database name printed out next to **Target** in the cell above.<br/>
# MAGIC This should follow the pattern **`dbacademy_<username>_dewd_cap_12`**
# MAGIC 1. In the **Storage location** field, copy the directory as printed above.
# MAGIC 1. For **Pipeline Mode**, select **Continuous**
# MAGIC 1. Uncheck the **Enable autoscaling** box
# MAGIC 1. Set the number of workers to **`1`** (one)
# MAGIC 1. Click **Create**.
# MAGIC 1. After the UI updates, change from **Development** to **Production** mode
# MAGIC 
# MAGIC This should begin the deployment of infrastructure.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Schedule a Notebook Job
# MAGIC 
# MAGIC Our DLT pipeline is setup to process data as soon as it arrives. 
# MAGIC 
# MAGIC We'll schedule a notebook to land a new batch of data each minute so we can see this functionality in action.
# MAGIC 
# MAGIC Before we start run the following cell to get the values used in this step.

# COMMAND ----------

print_job_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Steps:
# MAGIC 1. Navigate to the Jobs UI using the Databricks left side navigation bar.
# MAGIC 1. Click the blue **Create Job** button
# MAGIC 1. Configure the task:
# MAGIC     1. Enter **Land-Data** for the task name
# MAGIC     1. Select the notebook **DE 12.2.3L - Land New Data** using the notebook picker
# MAGIC     1. From the **Cluster** dropdown, under **Existing All Purpose Cluster**, select your cluster
# MAGIC     1. Click **Create**
# MAGIC 1. In the top-left of the screen rename the job (not the task) from **`Land-Data`** (the defaulted value) to the **Job Name** provided for you in the previous cell.    
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Note**: When selecting your all purpose cluster, you will get a warning about how this will be billed as all purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Set a Chronological Schedule for your Job
# MAGIC Steps:
# MAGIC 1. Navigate to the **Jobs UI** and click on the job you just created.
# MAGIC 1. Locate the **Schedule** section in the side panel on the right.
# MAGIC 1. Click on the **Edit schedule** button to explore scheduling options.
# MAGIC 1. Change the **Schedule type** field from **Manual** to **Scheduled**, which will bring up a chron scheduling UI.
# MAGIC 1. Set the schedule to update **Every 2**, **Minutes** from **00** 
# MAGIC 1. Click **Save**
# MAGIC 
# MAGIC **NOTE**: If you wish, you can click **Run now** to trigger the first run, or wait until the top of the next minute to make sure your scheduling has worked successfully.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Register DLT Event Metrics for Querying with DBSQL
# MAGIC 
# MAGIC The following cell prints out SQL statements to register the DLT event logs to your target database for querying in DBSQL.
# MAGIC 
# MAGIC Execute the output code with the DBSQL Query Editor to register these tables and views. 
# MAGIC 
# MAGIC Explore each and make note of the logged event metrics.

# COMMAND ----------

DA.generate_register_dlt_event_metrics_sql()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Define a Query on the Gold Table
# MAGIC 
# MAGIC The **daily_patient_avg** table is automatically updated each time a new batch of data is processed through the DLT pipeline. Each time a query is executed against this table, DBSQL will confirm if there is a newer version and then materialize results from the newest available version.
# MAGIC 
# MAGIC Run the following cell to print out a query with your database name. Save this as a DBSQL query.

# COMMAND ----------

DA.generate_daily_patient_avg()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Add a Line Plot Visualization
# MAGIC 
# MAGIC To track trends in patient averages over time, create a line plot and add it to a new dashboard.
# MAGIC 
# MAGIC Create a line plot with the following settings:
# MAGIC * **X Column**: **`date`**
# MAGIC * **Y Column**: **`avg_heartrate`**
# MAGIC * **Group By**: **`name`**
# MAGIC 
# MAGIC Add this visualization to a dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Track Data Processing Progress
# MAGIC 
# MAGIC The code below extracts the **`flow_name`**, **`timestamp`**, and **`num_output_rows`** from the DLT event logs.
# MAGIC 
# MAGIC Save this query in DBSQL, then define a bar plot visualization that shows:
# MAGIC * **X Column**: **`timestamp`**
# MAGIC * **Y Column**: **`num_output_rows`**
# MAGIC * **Group By**: **`flow_name`**
# MAGIC 
# MAGIC Add your visualization to your dashboard.

# COMMAND ----------

DA.generate_visualization_query()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Refresh your Dashboard and Track Results
# MAGIC 
# MAGIC The **Land-Data** notebook scheduled with Jobs above has 12 batches of data, each representing a month of recordings for our small sampling of patients. As configured per our instructions, it should take just over 20 minutes for all of these batches of data to be triggered and processed (we scheduled the Databricks Job to run every 2 minutes, and batches of data will process through our pipeline very quickly after initial ingestion).
# MAGIC 
# MAGIC Refresh your dashboard and review your visualizations to see how many batches of data have been processed. (If you followed the instructions as outlined here, there should be 12 distinct flow updates tracked by your DLT metrics.) If all source data has not yet been processed, you can go back to the Databricks Jobs UI and manually trigger additional batches.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC With everything configured, you can now continue to the final part of your lab in the notebook [DE 12.2.4L - Final Steps]($./DE 12.2.4L - Final Steps)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>


### DLT Task

-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE recordings_bronze
AS SELECT current_timestamp() receipt_time, input_file_name() source_file, *
  FROM cloud_files("${source}", "json", map("cloudFiles.schemaHints", "time DOUBLE"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE pii
AS SELECT *
  FROM cloud_files("/mnt/training/healthcare/patient", "csv", map("header", "true", "cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE recordings_enriched
  (CONSTRAINT positive_heartrate EXPECT (heartrate > 0) ON VIOLATION DROP ROW)
AS SELECT 
  CAST(a.device_id AS INTEGER) device_id, 
  CAST(a.mrn AS LONG) mrn, 
  CAST(a.heartrate AS DOUBLE) heartrate, 
  CAST(from_unixtime(a.time, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) time,
  b.name
  FROM STREAM(live.recordings_bronze) a
  INNER JOIN STREAM(live.pii) b
  ON a.mrn = b.mrn

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE daily_patient_avg
  COMMENT "Daily mean heartrates by patient"
AS SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE(time) `date`
  FROM STREAM(live.recordings_enriched)
  GROUP BY mrn, name, DATE(time)

### Land New Data

# Databricks notebook source
# MAGIC %run ../../Includes/Classroom-Setup-12.2.3L

# COMMAND ----------

DA.data_factory.load()


### Final steps

# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # End-to-End ETL in the Lakehouse
# MAGIC ## Final Steps
# MAGIC 
# MAGIC We are picking up from the first notebook in this lab, [DE 12.2.1L - Instructions and Configuration]($./DE 12.2.1L - Instructions and Configuration)
# MAGIC 
# MAGIC If everything is setup correctly, you should have:
# MAGIC * A DLT Pipeline running in **Continuous** mode
# MAGIC * A job that is feeding that pipline new data every 2 minutes
# MAGIC * A series of Databricks SQL Queries analysing the outputs of that pipeline

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-12.2.4L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Execute a Query to Repair Broken Data
# MAGIC 
# MAGIC Review the code that defined the **`recordings_enriched`** table to identify the filter applied for the quality check.
# MAGIC 
# MAGIC In the cell below, write a query that returns all the records from the **`recordings_bronze`** table that were refused by this quality check.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC SELECT * FROM ${da.db_name}.recordings_bronze WHERE heartrate <= 0

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC For the purposes of our demo, let's assume that thorough manual review of our data and systems has demonstrated that occasionally otherwise valid heartrate recordings are returned as negative values.
# MAGIC 
# MAGIC Run the following query to examine these same rows with the negative sign removed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT abs(heartrate), * FROM ${da.db_name}.recordings_bronze WHERE heartrate <= 0

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC To complete our dataset, we wish to insert these fixed records into the silver **`recordings_enriched`** table.
# MAGIC 
# MAGIC Use the cell below to update the query used in the DLT pipeline to execute this repair.
# MAGIC 
# MAGIC **NOTE**: Make sure you update the code to only process those records that were previously rejected due to the quality check.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ANSWER
# MAGIC 
# MAGIC MERGE INTO ${da.db_name}.recordings_enriched t
# MAGIC USING (SELECT
# MAGIC   CAST(a.device_id AS INTEGER) device_id, 
# MAGIC   CAST(a.mrn AS LONG) mrn, 
# MAGIC   abs(CAST(a.heartrate AS DOUBLE)) heartrate, 
# MAGIC   CAST(from_unixtime(a.time, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) time,
# MAGIC   b.name
# MAGIC   FROM ${da.db_name}.recordings_bronze a
# MAGIC   INNER JOIN ${da.db_name}.pii b
# MAGIC   ON a.mrn = b.mrn
# MAGIC   WHERE heartrate <= 0) v
# MAGIC ON t.mrn=v.mrn AND t.time=v.time
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Use the cell below to manually or programmatically confirm that this update has been successful.
# MAGIC 
# MAGIC (The total number of records in the **`recordings_bronze`** should now be equal to the total records in **`recordings_enriched`**).

# COMMAND ----------

# ANSWER
assert spark.table(f"{DA.db_name}.recordings_bronze").count() == spark.table(f"{DA.db_name}.recordings_enriched").count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Consider Production Data Permissions
# MAGIC 
# MAGIC Note that while our manual repair of the data was successful, as the owner of these datasets, by default we have permissions to modify or delete these data from any location we're executing code.
# MAGIC 
# MAGIC To put this another way: our current permissions would allow us to change or drop our production tables permanently if an errant SQL query is accidentally executed with the current user's permissions (or if other users are granted similar permissions).
# MAGIC 
# MAGIC While for the purposes of this lab, we desired to have full permissions on our data, as we move code from development to production, it is safer to leverage <a href="https://docs.databricks.com/administration-guide/users-groups/service-principals.html" target="_blank">service principals</a> when scheduling Jobs and DLT Pipelines to avoid accidental data modifications.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Shut Down Production Infrastructure
# MAGIC 
# MAGIC Note that Databricks Jobs, DLT Pipelines, and scheduled DBSQL queries and dashboards are all designed to provide sustained execution of production code. In this end-to-end demo, you were instructed to configure a Job and Pipeline for continuous data processing. To prevent these workloads from continuing to execute, you should **Pause** your Databricks Job and **Stop** your DLT pipeline. Deleting these assets will also ensure that production infrastructure is terminated.
# MAGIC 
# MAGIC **NOTE**: All instructions for DBSQL asset scheduling in previous lessons instructed users to set the update schedule to end tomorrow. You may choose to go back and also cancel these updates to prevent DBSQL endpoints from staying on until that time.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
