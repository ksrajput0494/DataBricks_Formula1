-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Lesson Objectives
-- MAGIC 1. Spark SQL Documentation
-- MAGIC 2. Create Database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESCRIBE DATABASE DEMO

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED DEMO

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Lesson Objectives
-- MAGIC 1. Create managed table using Python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESCRIBE EXTENDED race_results_python

-- COMMAND ----------

select * from demo.race_results_python where race_year = 2020;

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
SELECT * FROM demo.race_results_python WHERE race_year=2020;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

select * from race_results_sql

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Lesson Objectives
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping a external table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_number INT,
driver_name STRING,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/formula1dlkd/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

select count(*) from race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Views on tables
-- MAGIC #### Lesson Objectives
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Global View
-- MAGIC 3. Create Permananet View
-- MAGIC

-- COMMAND ----------

select current_database()

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT * 
    FROM demo.race_results_python
  WHERE race_year = 2018;

-- COMMAND ----------

select * from v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2012;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

SHOW TABLES IN global_temp

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2000;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

select * from demo.pv_race_results;