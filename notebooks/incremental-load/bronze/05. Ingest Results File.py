# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest results.json file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Define and enforce schema
# MAGIC 3. Add Metadata Columns
# MAGIC     - Source File
# MAGIC     - Ingestion Timestamp
# MAGIC 4. Write to bronze delta table    

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists formula1.bronze.results

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

# Define source file and table name
source_file = f"{landing_folder_path}/results"
table_name = f"{catalog_name}.{bronze_schema}.results"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using dataframe reader API

# COMMAND ----------

# Define the schema
from pyspark.sql.types import StructField, StructType, DateType, StringType, IntegerType, FloatType

results_schema = StructType([
            StructField('date', DateType()),
            StructField('raceName', StringType()),
            StructField('round', IntegerType()),
            StructField('season', IntegerType()),
            StructField('url', StringType()),
            StructField('constructorId', StringType()),
            StructField('driverId', StringType()),
            StructField('grid', IntegerType()),
            StructField('laps', IntegerType()),
            StructField('number', IntegerType()),
            StructField('points', FloatType()),
            StructField('position', IntegerType()),
            StructField('positionText', StringType()),
            StructField('status', StringType())
               
])
   




# COMMAND ----------

# Read data from the drivers file
results_df = (spark.read
                .format('json')
                .schema(results_schema)
                .option('mode', 'FASTFAIL')
                .load(source_file))
        

      

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Add Matadata columns

# COMMAND ----------

results_final_df = add_ingestion_metadata(results_df)
             
                                                          
            

# COMMAND ----------

results_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write to bronze delta table

# COMMAND ----------

results_final_df.write.format('delta').mode('overwrite').saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.bronze.results

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC select season, count(*)
# MAGIC from formula1.bronze.results
# MAGIC group by season
# MAGIC order by season