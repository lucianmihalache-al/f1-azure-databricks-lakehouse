-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Configure Access to Cloud Storage via Unity Catalog
-- MAGIC
-- MAGIC
-- MAGIC ### Access Cloud Storage

-- COMMAND ----------

-- MAGIC
-- MAGIC %fs ls 'abfss://demo@databrickscoursedl1611.dfs.core.windows.net/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create External Location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dl1611_demo
URL 'abfss://demo@databrickscoursedl1611.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databricks-course-sc`)
COMMENT 'External location for the demo container';
