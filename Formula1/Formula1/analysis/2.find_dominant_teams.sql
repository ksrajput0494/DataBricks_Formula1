-- Databricks notebook source
select team_name, SUM(calculated_points) AS total_points, count(1) as total_races , AVG(calculated_points) AS avg_points
from f1_presentation.calculated_race_results
group by team_name
HAVING count(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

select team_name, SUM(calculated_points) AS total_points, count(1) as total_races , AVG(calculated_points) AS avg_points
from f1_presentation.calculated_race_results
WHERE race_year between 2011 and 2020
group by team_name
HAVING count(1) >= 50
ORDER BY avg_points DESC