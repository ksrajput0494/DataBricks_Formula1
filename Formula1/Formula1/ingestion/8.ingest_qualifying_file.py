# Databricks notebook source
# MAGIC %md
# MAGIC ## 

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

from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, FloatType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_schema =  StructType(fields=[ StructField("qualifyId",IntegerType(),False), \
                                      StructField("raceId", IntegerType(), True), \
                                      StructField("driverId", IntegerType(), True), \
                                      StructField("constructorId", IntegerType(), True), \
                                      StructField("number", IntegerType(), True), \
                                      StructField("Position", IntegerType(), True), \
                                      StructField("q1", StringType(), True), \
                                      StructField("q2", StringType(), True), \
                                      StructField("q3",StringType(), True)
    ])

# COMMAND ----------

qualifying_df = spark.read \
                  .schema(qualifying_schema) \
                  .option("multiline",True) \
                  .json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_df.withColumnRenamed("qualifyId","qualify_Id") \
                             .withColumnRenamed("raceId","race_Id") \
                             .withColumnRenamed("driverId","driver_Id") \
                             .withColumnRenamed("constructorId","constructor_Id") \
                             .withColumn("data_source", lit(v_data_source)) \
                             .withColumn("file_date",lit(v_file_date)))
                             

# COMMAND ----------

#overwrite_partition(qualifying_final_df,'f1_processed','qualifying',"race_Id")

# COMMAND ----------

merge_condition = "tgt.qualify_Id = src.qualify_Id AND tgt.race_Id = src.race_Id";
merge_delta_data(qualifying_final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, "race_Id" )

# COMMAND ----------

#qualifying_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")
#qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")