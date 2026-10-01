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
# MAGIC #### Steps 1 - 7. Read, transform and perform quality checks

# COMMAND ----------

results_df = (
              spark.table(bronze_table)
                   .drop(F.col("url"))
                   .withColumnRenamed("date", "race_date")
                   .withColumnRenamed("raceName", "race_name")
                   .withColumnRenamed("constructorId", "constructor_id")
                   .withColumnRenamed("driverId", "driver_id")
                   .withColumnRenamed("grid", "grid_position")
                   .withColumnRenamed("laps", "completed_laps")
                   .withColumnRenamed("number", "car_number")
                   .withColumnRenamed("position", "finish_position")
                   .withColumnRenamed("positionText", "finish_position_text")
                   .filter(
                          F.col("season").isNotNull()&
                          F.col("round").isNotNull()&
                          F.col("constructor_id").isNotNull()&
                          F.col("driver_id").isNotNull())
                   .dropDuplicates(['constructor_id', 'driver_id', 'round', 'season'])
                   .withColumn("race_name", F.initcap(F.col("race_name")))
)



# COMMAND ----------

# MAGIC %md
# MAGIC #### 8. Write the transformed data to silver circuits table

# COMMAND ----------

results_df.write.format('delta').mode('overwrite').saveAsTable(silver_table)

# COMMAND ----------

display(spark.table(silver_table))