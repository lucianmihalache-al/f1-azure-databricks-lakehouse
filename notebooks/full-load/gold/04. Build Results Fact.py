# Databricks notebook source
# MAGIC %md
# MAGIC # Build Results Fact
# MAGIC
# MAGIC  1. Read silver `results` table
# MAGIC  2. Read silver `sprints` table
# MAGIC  3. Add new column `session_type` with values `RACE` or `SPRINT`
# MAGIC  4. UNION `results` and `sprints`
# MAGIC  5. Derive additional columns
# MAGIC      - is_win -> Indicates that the driver own the race
# MAGIC      - is_podium -> Indicates that the driver scored a podium result (1, 2, 3)
# MAGIC      - has_points -> Indicates that the driver has scored points
# MAGIC  6. Write the transformed data to gold `fact_session_results` table      

# COMMAND ----------

#### Entity Relationship Diagram - Formula1 Silver Schema

![Formula1 Silver Data.png](../../z-course-images/formula1-silver-data-erd.png "Formula1 Silver Data.png")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Steps 1 & 2 - Read source tables
# MAGIC - `silver.results`
# MAGIC - `silver.sprints`

# COMMAND ----------

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
         .withColumn("session_type", F.lit("RACE"))
         .drop("race_date", "race_name", "ingestion_timestamp", "source_file")
)



# COMMAND ----------

sprints_df = (
    spark.table(f"{catalog_name}.{silver_schema}.sprints")
         .withColumn("session_type", F.lit("SPRINT"))
         .drop("race_date", "race_name", "ingestion_timestamp", "source_file")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Union `results` with `sprints`
# MAGIC

# COMMAND ----------


results_sprints_df = results_df.unionByName(sprints_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Add dervied columns
# MAGIC  1. is_win -> Indicates that the driver own the race
# MAGIC  2. is_podium -> Indicates that the driver scored a podium result (1, 2, 3)
# MAGIC  3. has_points -> Indicates that the driver has scored points

# COMMAND ----------

fact_session_results_df =(results_sprints_df
        .withColumn("is_win", F.col("finish_position") == 1)
        .withColumn("is_podium", F.col("finish_position").between(1, 3))
        .withColumn("has_points", F.col("points") > 0)
)

# COMMAND ----------

display(fact_session_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write the transformed data to the `gold` `fact_session_results` table

# COMMAND ----------

fact_session_results_df.write.format('delta').mode('overwrite').saveAsTable(target_table)

# COMMAND ----------

display(spark.table(target_table))

# COMMAND ----------

