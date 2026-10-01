# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuit.csv file
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Add Metadata Columns
# MAGIC     - Source File
# MAGIC     - Ingestion Timestamp
# MAGIC 3. Write to bronze delta table    

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/circuits.csv"
table_name = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

source_file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the CSV file using dataframe reader API

# COMMAND ----------

# Create schema variable to manually modify the columns datatype before importing the CSV file
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

circuit_schema = StructType([
    StructField('circuitId',    StringType()),
    StructField('url',          StringType()),
    StructField('circuitName',  StringType()),
    StructField('lat',          DoubleType()),
    StructField('long',         DoubleType()),
    StructField('locality',     StringType()),
    StructField('country',      StringType()),

])

# COMMAND ----------

circuits_df = (
    spark.read
        .format('csv')
        .option('header', 'true')          # add the columns names to the header
       # .option('inferSchema', 'true')      asks spark to check and correct the datatypes (lat and long)
        .option('mode', 'FAILFAST')        # choose this import mode to check and notify any bad datatypes
        .schema(circuit_schema)            # using our manually modified datatype
        .load (source_file)
)        

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Add Matadata columns

# COMMAND ----------

# Import functions and create new variable when adding new columns
#from pyspark.sql import functions as F

#circuits_final_df = (
    #circuits_df
         #.withColumn('ingestion_timestamp', F.current_timestamp())
         #.withColumn('source_file', F.col('_metadata.file_path')))

         # Replace the above code with the new function created in 'the bronze helpers' (refactor logic)


circuits_final_df = add_ingestion_metadata(circuits_df)




# COMMAND ----------

circuits_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write to bronze delta table

# COMMAND ----------

#circuits_final_df = circuits_final_df.withColumn("batch_id", F.lit(v_batch_id))

# COMMAND ----------

#(
#circuits_final_df.write.format('delta').mode('overwrite').partitionBy('batch_id').option('replaceWhere', 3f"batch_id = '{v_batch_id}'").option('mergeSchema', 'true').saveAsTable(table_name)
3)

# COMMAND ----------

write_to_bronze(
    input_df = circuits_final_df,
    target_table = table_name,
    batch_id = v_batch_id

)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

