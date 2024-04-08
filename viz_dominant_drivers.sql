-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

use f1_presentation;

-- COMMAND ----------

create or replace temp view v_dominant_drivers
as
select driver_name, 
count(*) as total_races,
sum(calc_points) as total_points,
avg(calc_points) as avg_points,
rank() over(order by avg(calc_points) desc) driver_rank
from calculates_race_results
group by driver_name
having total_races>=50
order by avg_points desc;

-- COMMAND ----------

select * from calculates_race_results;

-- COMMAND ----------

select race_year,
driver_name,
count(*) as total_races,
sum(calc_points) as total_points,
avg(calc_points) as avg_points
from calculates_race_results
where driver_name in (select driver_name from v_dominant_drivers where driver_rank<=10)
group by race_year, driver_name
order by race_year, avg_points desc;

-- COMMAND ----------

select race_year,
driver_name,
count(*) as total_races,
sum(calc_points) as total_points,
avg(calc_points) as avg_points
from calculates_race_results
where driver_name in (select driver_name from v_dominant_drivers where driver_rank<=10)
group by race_year, driver_name
order by race_year, avg_points desc;

-- COMMAND ----------


