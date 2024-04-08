-- Databricks notebook source
use f1_presentation;

-- COMMAND ----------

select * from calculates_race_results;

-- COMMAND ----------

select driver_name, 
count(*) as total_races,
sum(calc_points) as total_points,
avg(calc_points) as avg_points
from calculates_race_results
group by driver_name
having total_races>=50
order by avg_points desc;

-- COMMAND ----------

select count(distinct driver_name) from calculates_race_results;

-- COMMAND ----------

select driver_name, 
count(*) as total_races,
sum(calc_points) as total_points,
avg(calc_points) as avg_points
from calculates_race_results
where race_year between 2011 and 2020
group by driver_name
having total_races>=50
order by avg_points desc;

-- COMMAND ----------

select driver_name, 
count(*) as total_races,
sum(calc_points) as total_points,
avg(calc_points) as avg_points
from calculates_race_results
where race_year between 2001 and 2010
group by driver_name
having total_races>=50
order by avg_points desc;

-- COMMAND ----------


