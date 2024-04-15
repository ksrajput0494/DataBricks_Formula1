# Databricks notebook source
# MAGIC %md
# MAGIC ## 

# COMMAND ----------

dbutils.widgets.text("p_source_data", "")
v_source_data = dbutils.widgets.get("p_source_data")


# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, FloatType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_schema =  StructType(fields=[ StructField("resultId",IntegerType(),False), \
                                      StructField("raceId", IntegerType(), True), \
                                      StructField("driverId", IntegerType(), True), \
                                      StructField("constructorId", IntegerType(), True), \
                                      StructField("number", IntegerType(), True), \
                                      StructField("grid", IntegerType(), True), \
                                      StructField("position", IntegerType(), True), \
                                      StructField("positionText", StringType(), True), \
                                      StructField("positionOrder", IntegerType(), True), \
                                      StructField("points",FloatType(), True), \
                                      StructField("laps", IntegerType(), True), \
                                      StructField("time", StringType(), True), \
                                      StructField("milliseconds", IntegerType(), True), \
                                      StructField("fastestLap", IntegerType(), True), \
                                      StructField("rank", IntegerType(), True), \
                                      StructField("fatestLapTime", StringType(), True), \
                                      StructField("fastestLapSpeed", StringType(), True), \
                                      StructField("statusId", StringType(), True) \
    ])

# COMMAND ----------

results_df = spark.read \
                  .schema(results_schema) \
                  .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

results_rename_column_df = add_ingestion_date(results_df.withColumnRenamed("resultId","result_Id") \
                             .withColumnRenamed("raceId","race_Id") \
                             .withColumnRenamed("driverId","driver_Id") \
                             .withColumnRenamed("constructorId","constructor_Id") \
                             .withColumnRenamed("positionText","position_Text") \
                             .withColumnRenamed("positionOrder","position_Order") \
                             .withColumnRenamed("fastestLap","fastest_Lap") \
                             .withColumnRenamed("fatestLapTime","fatest_Lap_Time") \
                             .withColumnRenamed("fastestLapSpeed","fastest_Lap_Speed") \
                             .withColumn("data_source",lit(v_source_data)) \
                             .withColumn("file_date",lit(v_file_date)))
                              
                             

# COMMAND ----------

results_final_df = results_rename_column_df.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC De-dupe the dataframe

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_Id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect() :
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

#results_final_df.write.mode("overwrite").partitionBy("race_Id").parquet(f"{processed_folder_path}/results") old
# results_final_df.write.mode("append").partitionBy("race_Id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2 

# COMMAND ----------

#overwrite_partition(results_final_df,'f1_processed','results', 'race_Id')
merge_condition = "tgt.result_id = src.result_id AND tgt.race_Id = src.race_Id";
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, "race_Id" )

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.results

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_Id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_Id
# MAGIC ORDER BY race_Id DESC