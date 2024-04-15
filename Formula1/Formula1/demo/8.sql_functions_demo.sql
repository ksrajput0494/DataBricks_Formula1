-- Databricks notebook source
use f1_processed;

-- COMMAND ----------

select *, CONCAT(driver_ref, '_', code) AS new_driver_ref from drivers

-- COMMAND ----------

select *, SPLIT(name,' ')[0] forename,SPLIT(name,' ')[1] surname from drivers

-- COMMAND ----------

select *, current_timestamp from drivers

-- COMMAND ----------

select *, date_format(dob, 'dd-MM-yyyy' ) from drivers

-- COMMAND ----------

select *, date_add(dob, 1 ) from drivers

-- COMMAND ----------

SELECT COUNT(*) FROM drivers

-- COMMAND ----------

SELECT MAX(dob) from drivers

-- COMMAND ----------

SELECT * from drivers where dob = '2000-05-11'

-- COMMAND ----------

SELECT COUNT(*) FROM drivers where nationality = 'British'

-- COMMAND ----------

SELECT nationality, COUNT(*) FROM drivers where GROUP by nationality order by nationality

-- COMMAND ----------

SELECT nationality, COUNT(*) FROM drivers where group by nationality HAVING count(*) > 100 order by nationality

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob DESC) As age_rank From drivers
ORDER BY nationality, age_rank