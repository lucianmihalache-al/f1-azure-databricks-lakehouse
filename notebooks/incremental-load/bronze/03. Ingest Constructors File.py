# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest constructors.json file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Add Metadata Columns
# MAGIC     - Source File
# MAGIC     - Ingestion Timestamp
# MAGIC 3. Write to bronze delta table    

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/constructors.json"
table_name = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using dataframe reader API

# COMMAND ----------

# Define the schema

constructors_schema = "constructorId STRING, name STRING, nationality STRING, url STRING"
        


# COMMAND ----------


constructors_df = (spark.read
                        .format('json')
                        .option('mode', 'FAILFAST')
                        .schema(constructors_schema)
                        .load(source_file)
          )
      

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Add Matadata columns

# COMMAND ----------


constructors_final_df = add_ingestion_metadata(constructors_df)                
                                                          
            

# COMMAND ----------

constructors_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write to bronze delta table

# COMMAND ----------

constructors_final_df.write.format('delta').mode('overwrite').saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.bronze.constructors

# COMMAND ----------

display(spark.table(table_name))