-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create circuits tables

-- COMMAND ----------

use f1_raw

-- COMMAND ----------



-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits ;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitID INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  COUNTRY STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  URL STRING
)
USING csv
OPTIONS (path "/mnt/formula1dlkd/raw/circuits.csv", header true)

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
create table f1_raw.races(
raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date date,
time STRING,
url STRING
) USING csv
OPTIONS (path = "/mnt/formula1dlkd/raw/races.csv", header = True)

-- COMMAND ----------

select * from f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create constructors table
-- MAGIC * Single Line JSON
-- MAGIC * Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT, 
constructorRef STRING, 
name STRING, 
nationality STRING, 
url STRING
) USING JSON
OPTIONS (path "/mnt/formula1dlkd/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create drivers table
-- MAGIC * Single Line JSON
-- MAGIC * Complex Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT, 
driverRef STRING, 
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE, 
nationality STRING, 
url STRING
) USING JSON
OPTIONS (path "/mnt/formula1dlkd/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create result table
-- MAGIC * Single Line JSON
-- MAGIC * Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,  
raceId  INT,  
driverId  INT,  
constructorId  INT,  
number  INT,  
grid  INT,  
position  INT,  
positionText  String,  
positionOrder  INT,  
points Float,  
laps  INT,  
time  String,  
milliseconds  INT,  
fastestLap  INT,  
rank  INT,  
fatestLapTime  String,  
fastestLapSpeed  String,  
statusId  String  
) USING JSON
OPTIONS (path "/mnt/formula1dlkd/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create pit stops table
-- MAGIC * Multi Line JSON
-- MAGIC * Simple Structure

-- COMMAND ----------


DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
raceId Integer, 
driverId Integer, 
stop String, 
lap Integer, 
time String, 
duration String, 
milliseconds Integer 
) USING JSON
OPTIONS (path "/mnt/formula1dlkd/raw/pit_stops.json", multiLine true)


-- COMMAND ----------

select * from pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Lap Times Table
-- MAGIC * CSV file
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT, 
driverId INT,  
lap INT,
position INT, 
time STRING,
milliseconds Integer 
) USING csv
OPTIONS (path "/mnt/formula1dlkd/raw/lap_times")

-- COMMAND ----------

select count(*) from lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Qualifying table
-- MAGIC * JSON file
-- MAGIC * MultiLine JSON
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
qualifyId INT,
raceId INT,
driverId INT,  
constructorId INT,
number INT,
position INT, 
q1 STRING,
q2 STRING,
q3 STRING
) USING JSON
OPTIONS (path "/mnt/formula1dlkd/raw/qualifying", multiLine true)

-- COMMAND ----------

select * from qualifying

-- COMMAND ----------

DESC EXTENDED qualifying