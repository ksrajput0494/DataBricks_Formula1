-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

USE f1_processed

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * from drivers LIMIT 10;

-- COMMAND ----------

DESC drivers;

-- COMMAND ----------

SELECT * from drivers where nationality = 'British' and dob >= '1990-01-01'

-- COMMAND ----------

SELECT name,dob AS date_of_birth from drivers where nationality = 'British' and dob >= '1990-01-01' order by dob DESC

-- COMMAND ----------

select * from drivers order by nationality ASC, dob DESC

-- COMMAND ----------

SELECT * from drivers where (nationality = 'British' and dob >= '1990-01-01') OR nationality = 'Indian' ORDER BY dob DESC;

-- COMMAND ----------

SELECT name,nationality, dob from drivers where (nationality = 'British' and dob >= '1990-01-01') OR nationality = 'India' ORDER BY dob DESC;