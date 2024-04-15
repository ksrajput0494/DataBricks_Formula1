# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_source_data","")
v_source_data = dbutils.widgets.get("p_source_data")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId",IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round",IntegerType(),True),
                                  StructField("circuitid", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date",DateType(), True),
                                  StructField("time",StringType(), True),
                                  StructField("url",StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
    .option("header",True) \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

races_selected_df = races_df.select(col("raceId"),col("year"),col("round"),col("circuitid"),col("name"),col("date"),col("time"))

# COMMAND ----------

Races_renamed_df = races_selected_df.withColumnRenamed("raceId", "races_Id") \
    .withColumnRenamed("year","race_year") \
    .withColumnRenamed("circuitId","circuit_id")
    

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_pre_final_df = add_ingestion_date(Races_renamed_df) \
                                 .withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '), col('time')),'yyyy-MM-dd HH:mm:ss')) \
                                 .withColumn("data_source", lit(v_source_data)) \
                                 .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

races_final_df = races_pre_final_df.select(col("races_Id"),col("race_year"),col("round"),col("circuit_id"),col("name"),col("race_timestamp"),col("ingestion_date"), col("data_source"), col("file_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

#races_final_df.write.mode("overwrite").partitionBy('race_year').parquet(f"{processed_folder_path}/races")
races_final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")