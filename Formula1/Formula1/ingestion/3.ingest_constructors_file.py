# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest Constructors.json file

# COMMAND ----------

dbutils.widgets.text("p_source_data", "")
v_source_data = dbutils.widgets.get("p_source_data")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

consdtructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = add_ingestion_date(consdtructor_dropped_df).withColumnRenamed("constructorId","constructor_Id") \
                                              .withColumnRenamed("constructorRef","constructor_ref") \
                                              .withColumn("data_source",lit(v_source_data)) \
                                              .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 Write output to parquet file

# COMMAND ----------

#constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")