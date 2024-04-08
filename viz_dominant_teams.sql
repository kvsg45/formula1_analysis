-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

use f1_presentation;

-- COMMAND ----------

select * from calculates_race_results;

-- COMMAND ----------

create or replace temp view v_dominant_teams
as
select team_name,
count(*) as total_races,
sum(calc_points) as total_points,
avg(calc_points) as avg_points,
rank() over(order by avg(calc_points) desc) as team_rank
from calculates_race_results
group by team_name
having total_races>=100;

-- COMMAND ----------

select * from v_dominant_teams;

-- COMMAND ----------

select race_year,
team_name,
count(*) as total_races,
sum(calc_points) as total_points,
avg(calc_points) as avg_points
from calculates_race_results
where team_name in (select team_name from v_dominant_teams where team_rank<=5)
group by race_year, team_name
order by race_year, avg_points desc;

-- COMMAND ----------

select race_year,
team_name,
count(*) as total_races,
sum(calc_points) as total_points,
avg(calc_points) as avg_points
from calculates_race_results
where team_name in (select team_name from v_dominant_teams where team_rank<=5)
group by race_year, team_name
order by race_year, avg_points desc;
