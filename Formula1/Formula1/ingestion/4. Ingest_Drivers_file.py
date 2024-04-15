# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest drivers.json file

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
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[ StructField("forename", StringType(),True),
                                  StructField("surname", StringType(), True)
])

# COMMAND ----------

driver_schema = StructType(fields=[ StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number",IntegerType(), True),
                                    StructField("code",StringType(), True),
                                    StructField("name", name_schema, True),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)

])

# COMMAND ----------

drivers_df = spark.read \
            .schema(driver_schema) \
            .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename Columns and add new columns
# MAGIC 1. driveId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation of forename and surname 

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId","driver_id") \
                                    .withColumnRenamed("driverRef","driver_ref") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("name",concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                    .withColumn("data_source", lit(v_source_data)) \
                                    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop the unwanted columns
# MAGIC 1. name.forename
# MAGIC 2. name.surname
# MAGIC 3. url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - write to output to processed container in parquet format

# COMMAND ----------

#drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")