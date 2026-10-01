# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest sprints.json file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Define and enforce schema
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
source_file = f"{landing_folder_path}/sprints"
table_name = f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using dataframe reader API

# COMMAND ----------

# Define the schema
from pyspark.sql.types import StructType,StructField, DateType, StringType, IntegerType, FloatType

sprints_schema = StructType([
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
                StructField('status', StringType()),
])
   




# COMMAND ----------

# Read data from the sprints file
sprints_df = (spark.read
                  .format('json')
                  .option('mode', 'FASTFAIL')
                  .schema(sprints_schema)
                  .option('multiLine', 'TRUE')
                  .load(source_file))
        

      

# COMMAND ----------

display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Add Matadata columns

# COMMAND ----------

sprints_final_df = add_ingestion_metadata(sprints_df)
             
                                                          
            

# COMMAND ----------

sprints_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write to bronze delta table

# COMMAND ----------

sprints_final_df.write.format('delta').mode('overwrite').saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.bronze.sprints

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC select season, count(*)
# MAGIC from formula1.bronze.sprints
# MAGIC group by season
# MAGIC order by season;