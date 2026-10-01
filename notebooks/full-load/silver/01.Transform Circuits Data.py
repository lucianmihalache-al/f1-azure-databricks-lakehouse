# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Circuits Data
# MAGIC 1. Read bronze circuits table
# MAGIC 2. Keep only the columns required for analitycs (drop 'url' column)
# MAGIC 3. Standardize column names using snake_case(circuitsid -> circuits_id)
# MAGIC 4. Rename columns to make them more meaningfull
# MAGIC 5. Filter out rows where circuit_id is null
# MAGIC 6. Removes duplicate records
# MAGIC 7. Trnsform values of columns circuit_name and locality to Title Case
# MAGIC 8. Write the transformed data to silver circuits table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read bronze circuits table

# COMMAND ----------

# circuits_df = spark.read.option('versionAsOf', 0).table(bronze_table)  - used to load a certain version of the table

circuits_df = spark.table(bronze_table)

# COMMAND ----------

circuits_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Keep only the columns required for analisys (drop url column)

# COMMAND ----------

    #circuits_selected_df = circuits_df.select(
     #       'circuitId',
      #      'circuitName',
       #     'lat',
        #    'long',
         #   'locality',
          #  'country',
           # 'ingestion_timestamp',
            #'source_file')

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

circuits_selected_df = circuits_df.select(
            F.col("circuitId"),   #.alias("circuit_id"), -> used to rename columns
            F.col("circuitName"),
            F.col("lat"),
            F.col("long"),
            F.col("locality"),
            F.col("country"),
            F.col("ingestion_timestamp"),
            F.col("source_file"),

)

# COMMAND ----------

circuits_selected_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3 & 4 Standardise columns names

# COMMAND ----------

circuits_renamed_df = (
    circuits_selected_df
          .withColumnRenamed("circuitId", "circuit_id")
          .withColumnRenamed("circuitName", "circuit_name")
          .withColumnRenamed("lat", "latitude")
          .withColumnRenamed("long", "longitude")
          
             
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Filter out rows where circuit_id is null

# COMMAND ----------

#circuits_valid_df = circuits_renamed_df.filter(
 #       "circuit_id is not null"
#)

# COMMAND ----------

circuits_valid_df = circuits_renamed_df.filter(
           F.col("circuit_id").isNotNull())

# COMMAND ----------

circuits_valid_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Remove Duplicates

# COMMAND ----------

#circuits_distinct_df = circuits_valid_df.distinct()

# COMMAND ----------

circuits_distinct_df = circuits_valid_df.dropDuplicates(["circuit_id"])

# COMMAND ----------

circuits_distinct_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Transform column values to Title Case

# COMMAND ----------

circuits_final_df = (
    circuits_distinct_df
             .withColumn("circuit_name", F.initcap(F.col("circuit_name")))
             .withColumn("locality", F.initcap(F.col("locality")))
)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8. Write the transformed data to silver circuits table

# COMMAND ----------

circuits_final_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)

# COMMAND ----------

display(spark.table(silver_table))