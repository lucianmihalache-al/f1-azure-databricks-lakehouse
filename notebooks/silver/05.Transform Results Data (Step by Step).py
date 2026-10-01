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
# MAGIC #### 1. Read bronze results table

# COMMAND ----------

results_df = spark.table(bronze_table)

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Keep only the columns required for analisys (drop url column)

# COMMAND ----------

results_dropped_df = results_df.drop(F.col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. & 4. Standardise columns names

# COMMAND ----------

results__renamed_df = (results_dropped_df
                      .withColumnRenamed("date", "race_date")
                      .withColumnRenamed("raceName", "race_name")
                      .withColumnRenamed("constructorId", "constructor_id")
                      .withColumnRenamed("driverId", "driver_id")
                      .withColumnRenamed("grid", "grid_position")
                      .withColumnRenamed("laps", "completed_laps")
                      .withColumnRenamed("number", "car_number")
                      .withColumnRenamed("position", "finish_position")
                      .withColumnRenamed("positionText", "finish_position_text")
                    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Filter out rows where 'season', 'round', 'constructor_id' or 'driver_id' is null

# COMMAND ----------

results_valid_df = results__renamed_df.filter(
                   F.col("season").isNotNull()&
                   F.col("round").isNotNull()&
                   F.col("constructor_id").isNotNull()&
                   F.col("driver_id").isNotNull()
) 

# COMMAND ----------

display(results__renamed_df.count() - results_valid_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Remove Duplicates

# COMMAND ----------

results_distinct_df = results_valid_df.dropDuplicates(['constructor_id', 'driver_id', 'round', 'season'])

# COMMAND ----------

display(results_valid_df.count() - results_distinct_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Transform column values to Title Case

# COMMAND ----------

results_final_df = (results_distinct_df
                   .withColumn("race_name", F.initcap(F.col("race_name"))))


# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Write the transformed data to silver circuits table

# COMMAND ----------

results_final_df.write.format('delta').mode('overwrite').saveAsTable(silver_table)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

