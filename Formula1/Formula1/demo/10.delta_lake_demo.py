# Databricks notebook source
# MAGIC %md
# MAGIC #### 
# MAGIC 1) Write data to delta lake (managed table)
# MAGIC 2) Write data to delta lake (external table)
# MAGIC 3) Read data from delta lake (Table)
# MAGIC 4) Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formula1dlkd/demo'

# COMMAND ----------

results_df = spark.read \
    .option('inferSchema', True) \
    .json("/mnt/formula1dlkd/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable('f1_demo.results_managed')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save('/mnt/formula1dlkd/demo/results_external')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formula1dlkd/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/formula1dlkd/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable('f1_demo.results_partitioned')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC 1) Update Delta Table
# MAGIC 2) Delete FROM Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dlkd/demo/results_managed")

deltaTable.update("position <= 10", {"points": "11 - position"} )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dlkd/demo/results_managed")

deltaTable.delete("points = 0" )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upsert using merge

# COMMAND ----------

drivers_dqy1_df = spark.read \
    .option("inferSchema", True) \
        .json('/mnt/formula1dlkd/raw/2021-03-28/drivers.json') \
            .filter("driverId <= 10") \
                .select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_dqy1_df)

# COMMAND ----------

drivers_dqy1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_dqy2_df = spark.read \
    .option("inferSchema", True) \
        .json('/mnt/formula1dlkd/raw/2021-03-28/drivers.json') \
            .filter("driverId BETWEEN 6 AND 15") \
                .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_dqy2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_dqy2_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_dqy3_df = spark.read \
    .option("inferSchema", True) \
        .json('/mnt/formula1dlkd/raw/2021-03-28/drivers.json') \
            .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
                .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET tgt.dob = upd.dob,
# MAGIC            tgt.forename = upd.forename,
# MAGIC            tgt.surname = upd.surname,
# MAGIC            tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverID, dob, forename, surname, createdDate) VALUES (driverID, dob, forename, surname, current_timestamp)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Day 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET tgt.dob = upd.dob,
# MAGIC            tgt.forename = upd.forename,
# MAGIC            tgt.surname = upd.surname,
# MAGIC            tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverID, dob, forename, surname, createdDate) VALUES (driverID, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dlkd/demo/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_dqy3_df.alias("upd"),
    "tgt.driverId = upd.driverId" ) \
        .whenMatchedUpdate(set= {"dob" : "upd.dob" ,"forename" : "upd.forename","surname" : "upd.surname","updatedDate" : "current_timestamp()"}) \
        .whenNotMatchedInsert(values = {
            "driverId" : "upd.driverId",
            "dob" : "upd.dob" ,
            "forename" : "upd.forename",
            "surname" : "upd.surname",
            "createdDate" : "current_timestamp()"
        }) \
        .execute()




# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC #####
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-04-01T10:38:54Z';

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", '2024-04-01T10:38:54Z').load("/mnt/formula1dlkd/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-04-01T10:38:54Z';

# COMMAND ----------

spark.databricks

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = False;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-04-01T10:38:54Z';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 9;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_Merge VERSION AS OF 9 src
# MAGIC ON (tgt.driverID = src.driverID)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transaction Logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createDate DATE,
# MAGIC updateDate DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_txn
# MAGIC WHERE driverId = 1

# COMMAND ----------

for driver_id in range(3, 20):
  spark.sql(f"""INSERT INTO f1_demo.drivers_txn
                SELECT * FROM f1_demo.drivers_merge
                WHERE driverId = {driver_id}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/formula1dlkd/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/formula1dlkd/demo/drivers_convert_to_delta_new`