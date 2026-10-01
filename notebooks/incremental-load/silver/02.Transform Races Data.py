# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Races Data
# MAGIC 1. Read bronze races table
# MAGIC 2. Keep only the columns required for analitycs (drop 'url' column)
# MAGIC 3. Standardize column names using snake_case(circuitsid -> circuits_id, raceName -> race_name)
# MAGIC 4. Rename columns to make them more meaningfull (date -> race_date)
# MAGIC 5. Filter out rows where circuit_id is null
# MAGIC 6. Removes duplicate records
# MAGIC 7. Trnsform values of columns race_name to Title Case
# MAGIC 8. Write the transformed data to silver circuits table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read bronze races table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

races_df = spark.table(bronze_table)

# COMMAND ----------

races_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Keep only the columns required for analisys (drop url column)

# COMMAND ----------

races_selected_df = races_df.select([
                   F.col("season"),
                   F.col("round"),
                   F.col("raceName"),
                   F.col("date"),
                   F.col("circuitId"),
                   F.col("ingestion_timestamp"),
                   F.col("source_file"),
]) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3 & 4 Standardise columns names

# COMMAND ----------

races_renamed_df = (races_selected_df
                 .withColumnRenamed("raceName", "race_name")
                 .withColumnRenamed("date", "race_date")
                 .withColumnRenamed("circuitId", "circuit_id")) 

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Filter out rows where circuit_id is null

# COMMAND ----------

races_valid_df = races_renamed_df.filter(
                      F.col("circuit_id").isNotNull()    # there are no nulls in circuit_id column
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Remove Duplicates

# COMMAND ----------

races_distinct_df = races_valid_df.dropDuplicates(["season", "round"])

# COMMAND ----------

display(races_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Transform column values to Title Case

# COMMAND ----------

races_final_df = (races_distinct_df
                 .withColumn("race_name", F.initcap(F.col("race_name"))))


# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8. Write the transformed data to silver circuits table

# COMMAND ----------

races_final_df.write.format('delta').mode('overwrite').saveAsTable(silver_table)

# COMMAND ----------

display(spark.table(silver_table))