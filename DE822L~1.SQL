### Lab Instructions

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
# MAGIC # Lab: Migrating SQL Notebooks to Delta Live Tables
# MAGIC 
# MAGIC This notebook describes the overall structure for the lab exercise, configures the environment for the lab, provides simulated data streaming, and performs cleanup once you are done. A notebook like this is not typically needed in a production pipeline scenario.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Convert existing data pipelines to Delta Live Tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC 
# MAGIC This demo uses simplified artificially generated medical data. The schema of our two datasets is represented below. Note that we will be manipulating these schema during various steps.
# MAGIC 
# MAGIC #### Recordings
# MAGIC The main dataset uses heart rate recordings from medical devices delivered in the JSON format. 
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | device_id | int |
# MAGIC | mrn | long |
# MAGIC | time | double |
# MAGIC | heartrate | double |
# MAGIC 
# MAGIC #### PII
# MAGIC These data will later be joined with a static table of patient information stored in an external system to identify patients by name.
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | mrn | long |
# MAGIC | name | string |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Getting Started
# MAGIC 
# MAGIC Begin by running the following cell to configure the lab environment.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.2.1L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Land Initial Data
# MAGIC Seed the landing zone with some data before proceeding. You will re-run this command to land additional data later.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Execute the following cell to print out values that will be used during the following configuration steps.

# COMMAND ----------

DA.print_pipeline_config()    

# COMMAND ----------

# ANSWER

# This function is provided for those students that do not 
# want to work through the exercise of creating the pipline.
DA.create_pipeline()

# This function is provided to start the pipline and block 
# until it has comleted, is canceled or failed
DA.start_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Create and Configure a Pipeline
# MAGIC 
# MAGIC 1. Click the **Jobs** button on the sidebar, then select the **Delta Live Tables** tab.
# MAGIC 1. Click **Create Pipeline**.
# MAGIC 1. Leave **Product Edition** as **Advanced**.
# MAGIC 1. Fill in a **Pipeline Name** - because these names must be unique, we suggest using the **Pipline Name** provided in the cell above.
# MAGIC 1. For **Notebook Libraries**, use the navigator to locate and select the notebook **`DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab`**.
# MAGIC 1. Configure the Source
# MAGIC     * Click **`Add configuration`**
# MAGIC     * Enter the word **`source`** in the **Key** field
# MAGIC     * Enter the **Source** value specified above to the **`Value`** field
# MAGIC 1. Enter the database name printed next to **`Target`** below in the **Target** field.
# MAGIC 1. Enter the location printed next to **`Storage Location`** below in the **Storage Location** field.
# MAGIC 1. Set **Pipeline Mode** to **Triggered**.
# MAGIC 1. Disable autoscaling.
# MAGIC 1. Set the number of **`workers`** to **`1`** (one).
# MAGIC 1. Click **Create**.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Open and Complete DLT Pipeline Notebook
# MAGIC 
# MAGIC You will perform your work in the companion notebook [DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab]($./DE 8.2.2L - Migrating a SQL Pipeline to DLT Lab),<br/>
# MAGIC which you will ultimately deploy as a pipeline.
# MAGIC 
# MAGIC Open the Notebook and, following the guidelines provided therein, fill in the cells where prompted to<br/>
# MAGIC implement a multi-hop architecture similar to the one we worked with in the previous section.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Run your Pipeline
# MAGIC 
# MAGIC Select **Development** mode, which accelerates the development lifecycle by reusing the same cluster across runs.<br/>
# MAGIC It will also turn off automatic retries when jobs fail.
# MAGIC 
# MAGIC Click **Start** to begin the first update to your table.
# MAGIC 
# MAGIC Delta Live Tables will automatically deploy all the necessary infrastructure and resolve the dependencies between all datasets.
# MAGIC 
# MAGIC **NOTE**: The first table update may take several minutes as relationships are resolved and infrastructure deploys.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Troubleshooting Code in Development Mode
# MAGIC 
# MAGIC Don't despair if your pipeline fails the first time. Delta Live Tables is in active development, and error messages are improving all the time.
# MAGIC 
# MAGIC Because relationships between tables are mapped as a DAG, error messages will often indicate that a dataset isn't found.
# MAGIC 
# MAGIC Let's consider our DAG below:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/dlt-dag.png">
# MAGIC 
# MAGIC If the error message **`Dataset not found: 'recordings_parsed'`** is raised, there may be several culprits:
# MAGIC 1. The logic defining **`recordings_parsed`** is invalid
# MAGIC 1. There is an error reading from **`recordings_bronze`**
# MAGIC 1. A typo exists in either **`recordings_parsed`** or **`recordings_bronze`**
# MAGIC 
# MAGIC The safest way to identify the culprit is to iteratively add table/view definitions back into your DAG starting from your initial ingestion tables. You can simply comment out later table/view definitions and uncomment these between runs.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>



-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC # Lab: Migrating a SQL Pipeline to Delta Live Tables
-- MAGIC 
-- MAGIC This notebook will be completed by you to implement a DLT pipeline using SQL. 
-- MAGIC 
-- MAGIC It is **not intended** to be executed interactively, but rather to be deployed as a pipeline once you have completed your changes.
-- MAGIC 
-- MAGIC To aid in completion of this Notebook, please refer to the <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-language-ref.html#sql" target="_blank">DLT syntax documentation</a>.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Declare Bronze Table
-- MAGIC 
-- MAGIC Declare a bronze table that ingests JSON data incrementally (using Auto Loader) from the simulated cloud source. The source location is already supplied as an argument; using this value is illustrated in the cell below.
-- MAGIC 
-- MAGIC As we did previously, include two additional columns:
-- MAGIC * **`receipt_time`** that records a timestamp as returned by **`current_timestamp()`** 
-- MAGIC * **`source_file`** that is obtained by **`input_file_name()`**

-- COMMAND ----------

-- ANSWER
CREATE OR REFRESH STREAMING LIVE TABLE recordings_bronze
AS SELECT current_timestamp() receipt_time, input_file_name() source_file, *
  FROM cloud_files("${source}", "json", map("cloudFiles.schemaHints", "time DOUBLE"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ### PII File
-- MAGIC 
-- MAGIC Using a similar CTAS syntax, create a live **table** into the CSV data found at */mnt/training/healthcare/patient*.
-- MAGIC 
-- MAGIC To properly configure Auto Loader for this source, you will need to specify the following additional parameters:
-- MAGIC 
-- MAGIC | option | value |
-- MAGIC | --- | --- |
-- MAGIC | **`header`** | **`true`** |
-- MAGIC | **`cloudFiles.inferColumnTypes`** | **`true`** |
-- MAGIC 
-- MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"/> Auto Loader configurations for CSV can be found <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-csv.html" target="_blank">here</a>.

-- COMMAND ----------

-- ANSWER
CREATE OR REFRESH STREAMING LIVE TABLE pii
AS SELECT *
  FROM cloud_files("/mnt/training/healthcare/patient", "csv", map("header", "true", "cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Declare Silver Tables
-- MAGIC 
-- MAGIC Our silver table, **`recordings_parsed`**, will consist of the following fields:
-- MAGIC 
-- MAGIC | Field | Type |
-- MAGIC | --- | --- |
-- MAGIC | **`device_id`** | **`INTEGER`** |
-- MAGIC | **`mrn`** | **`LONG`** |
-- MAGIC | **`heartrate`** | **`DOUBLE`** |
-- MAGIC | **`time`** | **`TIMESTAMP`** (example provided below) |
-- MAGIC | **`name`** | **`STRING`** |
-- MAGIC 
-- MAGIC This query should also enrich the data through an inner join with the **`pii`** table on the common **`mrn`** field to obtain the name.
-- MAGIC 
-- MAGIC Implement quality control by applying a constraint to drop records with an invalid **`heartrate`** (that is, not greater than zero).

-- COMMAND ----------

-- ANSWER

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

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## Gold Table
-- MAGIC 
-- MAGIC Create a gold table, **`daily_patient_avg`**, that aggregates **`recordings_enriched`** by **`mrn`**, **`name`**, and **`date`** and delivers the following columns:
-- MAGIC 
-- MAGIC | Column name | Value |
-- MAGIC | --- | --- |
-- MAGIC | **`mrn`** | **`mrn`** from source |
-- MAGIC | **`name`** | **`name`** from source |
-- MAGIC | **`avg_heartrate`** | Average **`heartrate`** from the grouping |
-- MAGIC | **`date`** | Date extracted from **`time`** |

-- COMMAND ----------

-- ANSWER

CREATE OR REFRESH STREAMING LIVE TABLE daily_patient_avg
  COMMENT "Daily mean heartrates by patient"
  AS SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE(time) `date`
    FROM STREAM(live.recordings_enriched)
    GROUP BY mrn, name, DATE(time)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>


### Lab Conclusion

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
# MAGIC # Lab: Conclusion
# MAGIC Running the following cell to configure the lab environment:

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-8.2.3L

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Display Results
# MAGIC 
# MAGIC Assuming your pipeline runs successfully, display the contents of the gold table.
# MAGIC 
# MAGIC **NOTE**: Because we specified a value for **Target**, tables are published to the specified database. Without a **Target** specification, we would need to query the table based on its underlying location in DBFS (relative to the **Storage Location**).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${da.db_name}.daily_patient_avg

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Trigger another file arrival with the following cell. 
# MAGIC 
# MAGIC Feel free to run it a couple more times if desired. 
# MAGIC 
# MAGIC Following this, run the pipeline again and view the results. 
# MAGIC 
# MAGIC Feel free to re-run the cell above to gain an updated view of the **`daily_patient_avg`** table.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC Ensure that you delete your pipeline from the DLT UI, and run the following cell to clean up the files and tables that were created as part of the lab setup and execution.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Summary
# MAGIC 
# MAGIC In this lab, you learned to convert an existing data pipeline to a Delta Live Tables SQL pipeline, and deployed that pipeline using the DLT UI.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/data-engineering/delta-live-tables/index.html" target="_blank">Delta Live Tables Documentation</a>
# MAGIC * <a href="https://youtu.be/6Q8qPZ7c1O0" target="_blank">Delta Live Tables Demo</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
