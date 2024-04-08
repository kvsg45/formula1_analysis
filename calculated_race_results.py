# Databricks notebook source
dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

spark.sql(f"""
              CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
              (
              race_year INT,
              team_name STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
              )
              USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
            create or replace temp view race_result_updated
            as
            select r.race_year,r.race_id,c.name as team_name, d.name as driver_name,d.driver_id,re.position,re.points,
            11 - re.position as calculated_points
            from f1_processed.results re
            join f1_processed.drivers d on (re.driver_id=d.driver_id)
            join f1_processed.constructors c on (re.constructor_id = c.constructor_id)
            join f1_processed.races r on (re.race_id = r.race_id)
            where re.position<=10 and re.file_date = '{v_file_date}' 
""")

# COMMAND ----------

spark.sql(f"""
              MERGE INTO f1_presentation.calculated_race_results tgt
              USING race_result_updated upd
              ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
              WHEN MATCHED THEN
                UPDATE SET tgt.position = upd.position,
                           tgt.points = upd.points,
                           tgt.calculated_points = upd.calculated_points,
                           tgt.updated_date = current_timestamp
              WHEN NOT MATCHED
                THEN INSERT (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, created_date ) 
                     VALUES (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, current_timestamp)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from f1_presentation.calculated_race_results

# COMMAND ----------


