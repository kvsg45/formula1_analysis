-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from calculates_race_results;

-- COMMAND ----------

select team_name, 
count(*) as total_races,
sum(calc_points) as total_points,
avg(calc_points) as avg_points
from calculates_race_results
where race_year between 2011 and 2020
group by team_name
having total_races>=100
order by avg_points desc;

-- COMMAND ----------


