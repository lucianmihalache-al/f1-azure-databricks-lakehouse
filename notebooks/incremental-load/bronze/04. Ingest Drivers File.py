# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest drivers.json file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Define and enforce schema (preserve the nested structure)
# MAGIC 3. Add Metadata Columns
# MAGIC     - Source File
# MAGIC     - Ingestion Timestamp
# MAGIC 4. Write to bronze delta table    

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

# Define source file and table name
source_file = f"{landing_folder_path}/drivers.json"
table_name = f"{catalog_name}.{bronze_schema}.drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using dataframe reader API

# COMMAND ----------

# Define the schema
from pyspark.sql.types import StructField, StructType, StringType, DateType

name_schema = StructType([
              StructField('givenName', StringType()),
              StructField('familyName', StringType())
])
        
drivers_schema = StructType([
                 StructField('driverId', StringType()),
                 StructField('name', name_schema),
                 StructField('dateOfBirth', DateType()),
                 StructField('nationality', StringType()),
                 StructField('url', StringType())
])        




# COMMAND ----------

# Read data from the drivers file
drivers_df = (spark.read
             .format('json')
             .option('mode', 'FAILFAST')
             .schema(drivers_schema)
             .load(source_file))
      

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Add Matadata columns

# COMMAND ----------


drivers_final_df = add_ingestion_metadata(drivers_df)                
                                                          
            

# COMMAND ----------

drivers_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write to bronze delta table

# COMMAND ----------

drivers_final_df.write.format('delta').mode('overwrite').saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.bronze.drivers

# COMMAND ----------

display(spark.table(table_name))