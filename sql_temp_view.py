# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results").withColumnRenamed("constructor_name","team")

# COMMAND ----------

race_results_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results limit 10;

# COMMAND ----------

race_results_2019_df = spark.sql("select * from v_race_results where race_year=2019")

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("v_race_results")

# COMMAND ----------

display(spark.sql("select * from v_race_results limit 10"))

# COMMAND ----------


