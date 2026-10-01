# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Drivers Data
# MAGIC 1. Read bronze drivers table
# MAGIC 2. Drop 'url' column
# MAGIC 3. Standardize column names using snake_case(driverId -> driver_id, dateofbirth -> date_of_birth)
# MAGIC 4. Concatenate 'givenName' and 'familyName' to create a new column 'driver_name' and transform the value to Title Case
# MAGIC 5. Removes duplicate records
# MAGIC 6. Trnsform values of columns nationality to Title Case
# MAGIC 7. Write the transformed data to silver drivers table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Read bronze drivers table

# COMMAND ----------

drivers_df = spark.table(bronze_table)

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Keep only the columns required for analisys (drop url column)

# COMMAND ----------

drivers_dropped_df = drivers_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Standardise columns names

# COMMAND ----------

drivers__renamed_df = (drivers_dropped_df
                     .withColumnRenamed("driverId", "driver_id") 
                     .withColumnRenamed("dateOfBirth", "date_of_birth")) 


# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Concatenate 'givenName' and 'familyName' to create a new column 'driver_name'

# COMMAND ----------

drivers_conca_df = (drivers__renamed_df
                   .withColumn("driver_name",
                      F.initcap(F.concat_ws(" ", F.col("name.givenName"), F.col("name.familyName"))))
                   .drop("name")
                   )

# COMMAND ----------

display(drivers_conca_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Remove Duplicates

# COMMAND ----------

drivers_distinct_df = drivers_conca_df.dropDuplicates(["driver_id"])  # no duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Transform column values to Title Case

# COMMAND ----------

drivers_final_df = (drivers_distinct_df
                   .withColumn("nationality", F.initcap(F.col("nationality"))))


# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Write the transformed data to silver circuits table

# COMMAND ----------

drivers_final_df.write.format('delta').mode('overwrite').saveAsTable(silver_table)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

