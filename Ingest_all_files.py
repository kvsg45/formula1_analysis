# Databricks notebook source
circuits_result = dbutils.notebook.run("ingest_circuits_file",0,{"p_data_source": "Ergast_API","p_file_date": "2021-04-18"})

# COMMAND ----------

constructors_result = dbutils.notebook.run("ingest_constructors_file",0,{"p_data_source": "Ergast_API","p_file_date": "2021-04-18"})

# COMMAND ----------

drivers_result = dbutils.notebook.run("ingest_drivers_file",0,{"p_data_source": "Ergast_API","p_file_date": "2021-04-18"})

# COMMAND ----------

Laptimes_result = dbutils.notebook.run("ingest_Laptimes_file",0,{"p_data_source": "Ergast_API","p_file_date": "2021-04-18"})

# COMMAND ----------

pit_stops_result = dbutils.notebook.run("ingest_pit_stops_file",0,{"p_data_source": "Ergast_API","p_file_date": "2021-04-18"})

# COMMAND ----------

qualifying_result = dbutils.notebook.run("ingest_qualifying_file",0,{"p_data_source": "Ergast_API","p_file_date": "2021-04-18"})

# COMMAND ----------

races_result = dbutils.notebook.run("ingest_races_file",0,{"p_data_source": "Ergast_API","p_file_date": "2021-04-18"})

# COMMAND ----------

results_result = dbutils.notebook.run("ingest_results_file",0,{"p_data_source": "Ergast_API","p_file_date": "2021-04-18"})

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(*)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(*)
# MAGIC from f1_processed.lap_times
# MAGIC group by race_id
# MAGIC order by race_id desc;

# COMMAND ----------


