# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest pit_stops.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stop_schema = StructType(fields=[ StructField("raceId", IntegerType(), False), \
                                      StructField("driverId", IntegerType(), True), \
                                      StructField("stop", StringType(), True), \
                                      StructField("lap", IntegerType(), True), \
                                      StructField("time", StringType(), True), \
                                      StructField("duration", StringType(), True), \
                                      StructField("milliseconds", IntegerType(), True)    
                            
                            ])

# COMMAND ----------

pit_stop_df = spark.read \
                   .schema(pit_stop_schema) \
                   .option("multiline", True) \
                   .json(f'{raw_folder_path}/{v_file_date}/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = add_ingestion_date(pit_stop_df.withColumnRenamed("raceId","race_Id") \
                      .withColumnRenamed("driverId","driver_Id") \
                      .withColumn("data_source", lit(v_data_source)) \
                      .withColumn("file_date",lit(v_file_date)))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

display(final_df)

# COMMAND ----------

#overwrite_partition(final_df,'f1_processed','pit_stops',"race_Id")
merge_condition = "tgt.race_Id = src.race_Id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_Id = src.race_Id";
merge_delta_data(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, "race_Id" )


# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success")