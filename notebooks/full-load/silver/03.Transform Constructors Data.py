# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Constructors Data
# MAGIC 1. Read bronze constructors table
# MAGIC 2. Drop 'url' column
# MAGIC 3. Standardize column names using snake_case(constructorId -> constructor_id)
# MAGIC 4. Rename columns to make them more meaningfull (name -> constructor_name)
# MAGIC 5. Removes duplicate records
# MAGIC 6. Trnsform values of columns nationality to Title Case
# MAGIC 7. Write the transformed data to silver constructors table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read bronze constructors table

# COMMAND ----------

constructors_df = spark.table(bronze_table)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Keep only the columns required for analisys (drop url column)

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url") 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3 & 4 Standardise columns names

# COMMAND ----------

costructors_renamed_df = (constructors_dropped_df
                        .withColumnRenamed("constructorId", "constructor_id")
                        .withColumnRenamed("name", "constructor_name"))
                     


# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Remove Duplicates

# COMMAND ----------

constructor_distinct_df = costructors_renamed_df.dropDuplicates(["constructor_id"])  # no duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Transform column values to Title Case

# COMMAND ----------

constructors_final_df = (constructor_distinct_df
                    .withColumn("nationality", F.initcap(F.col("nationality"))))


# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Write the transformed data to silver circuits table

# COMMAND ----------

constructors_final_df.write.format('delta').mode('overwrite').saveAsTable(silver_table)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

