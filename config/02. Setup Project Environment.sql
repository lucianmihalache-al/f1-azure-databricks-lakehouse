-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Set-up the project environment for Formula1 Project
-- MAGIC 1. Create External Location databricks_course_ext_dl1611_formula1
-- MAGIC 2. Create Catalog formula1
-- MAGIC 3. Create Schemas landing, bronze, silver and gold
-- MAGIC 4. Create Volume Files in the landing schema
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Access Cloud Storage

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1@databrickscoursedl1611.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create External Location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dl1611_formula1
URL 'abfss://formula1@databrickscoursedl1611.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databricks-course-sc`)
COMMENT 'External location for the formula1 container';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Catalog Formula1

-- COMMAND ----------

show catalogs

-- COMMAND ----------

create catalog if not exists formula1
  managed location 'abfss://formula1@databrickscoursedl1611.dfs.core.windows.net/'
  comment 'This is the main catalog for formula1 project';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Schemas landing, bronze, silver, gold

-- COMMAND ----------

create schema if not exists formula1.landing;
create schema if not exists formula1.bronze
   managed location 'abfss://formula1@databrickscoursedl1611.dfs.core.windows.net/bronze';
create schema if not exists formula1.silver
   managed location 'abfss://formula1@databrickscoursedl1611.dfs.core.windows.net/silver';
create schema if not exists formula1.gold
   managed location 'abfss://formula1@databrickscoursedl1611.dfs.core.windows.net/gold';     

-- COMMAND ----------

select current_catalog()

-- COMMAND ----------

use catalog formula1

-- COMMAND ----------

show schemas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Volume Files

-- COMMAND ----------

create external volume formula1.landing.files
location 'abfss://formula1@databrickscoursedl1611.dfs.core.windows.net/landing';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1/landing/files

-- COMMAND ----------
