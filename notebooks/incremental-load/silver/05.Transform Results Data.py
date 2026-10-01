# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Results Data
# MAGIC 1. Read bronze results table
# MAGIC 2. Drop 'url' column
# MAGIC 3. Standardize column names using snake_case(constructor_id, driver_id, race_name, finish_position_text)
# MAGIC 4. Rename columns (race_date, grid_position, completed_laps, car_number, finish_position)
# MAGIC 5. Filter out rows where 'season', 'round', 'constructor_id' or 'driver_id' is null (business key validation)
# MAGIC 6. Removes duplicate records
# MAGIC 7. Trnsform values of columns 'race_name' to Title Case
# MAGIC 8. Write the transformed data to silver results table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Steps 1 to 4 - Read and transform bronze results table

# COMMAND ----------

results_df = (
     spark.table(bronze_table)
          .drop(F.col("url"))
          .withColumnsRenamed({
                        "date": "race_date",
                        "raceName": "race_name",
                        "constructorId": "constructor_id",
                        "driverId": "driver_id",
                        "grid": "grid_position",
                        "laps": "completed_laps",
                        "number": "car_number",
                        "position": "finish_position",
                        "positionText": "finish_position_text"})

)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Steps 5 & 6 - Apply data quality checks
# MAGIC - Filter out rows where 'season', 'round', 'constructor_id' or 'driver_id' is null
# MAGIC - Remove duplicates

# COMMAND ----------

results_valid_df = (
    results_df.filter(
                    F.col("season").isNotNull()&
                    F.col("round").isNotNull()&
                    F.col("constructor_id").isNotNull()&
                    F.col("driver_id").isNotNull())
              .dropDuplicates(['constructor_id', 'driver_id', 'round', 'season'])
)
 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Transform column values to Title Case

# COMMAND ----------

results_final_df = (results_valid_df
                   .withColumn("race_name", F.initcap(F.col("race_name"))))


# COMMAND ----------

# MAGIC %md
# MAGIC #### 8. Write the transformed data to silver circuits table

# COMMAND ----------

results_final_df.write.format('delta').mode('overwrite').saveAsTable(silver_table)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

