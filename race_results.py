# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
    .withColumnRenamed("name","race_name") \
    .withColumnRenamed("race_id","races_race_id") \
    .withColumnRenamed("circuit_id","races_circuit_id")
circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("name","circuit_name") \
    .withColumnRenamed("circuit_id","circuits_circuit_id")
drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("name","driver_name") \
    .withColumnRenamed("number","driver_number") \
    .withColumnRenamed("driver_id","drivers_driver_id") \
    .withColumnRenamed("nationality","driver_nationality")
constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name","constructor_name") \
    .withColumnRenamed("constructor_id","constructors_constructor_id")
results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
    .withColumnRenamed("number","result_number") \
    .withColumnRenamed("race_id","results_race_id") \
    .withColumnRenamed("driver_id","results_driver_id") \
    .withColumnRenamed("constructor_id","results_constructor_id") \
    .withColumnRenamed("file_date","results_file_date") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

display(results_df)

# COMMAND ----------

from pyspark.sql.functions import col, max, min
max_race_id = races_df.select(max(races_df.races_race_id)).collect()[0][0]
min_race_id = races_df.select(min(races_df.races_race_id)).collect()[0][0]
results_df1 = results_df.filter((results_df.results_race_id >= min_race_id) & (results_df.results_race_id <= max_race_id))
display(results_df1)

# COMMAND ----------

display(races_df)
races_df.count()

# COMMAND ----------

worker1_df = races_df.join(circuits_df,races_df.races_circuit_id==circuits_df.circuits_circuit_id,"left") \
    .select("races_race_id","race_year","race_name","race_timestamp","location")
display(worker1_df)

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
worker2_df = results_df1.join(drivers_df,results_df1["results_driver_id"]==drivers_df["drivers_driver_id"],"left") \
    .join(constructors_df, results_df1["results_constructor_id"]==constructors_df["constructors_constructor_id"],"left") \
    .select("results_race_id","results_driver_id","results_constructor_id","driver_name","driver_number","driver_nationality","constructor_name","grid","fastest_lap","time" \
        ,"points","position","results_file_date").withColumn("created_date",current_timestamp())
display(worker2_df)
worker2_df.count()


# COMMAND ----------

display(worker1_df)

# COMMAND ----------

final_df = worker2_df.join(worker1_df,worker2_df.results_race_id==worker1_df.races_race_id,"left") \
    .select("results_race_id" \
        ,"race_year" \
        ,"race_name" \
        ,"race_timestamp" \
        ,"location" \
        ,"driver_name" \
        ,"driver_number" \
        ,"driver_nationality" \
        ,"constructor_name" \
        ,"grid" \
        ,"fastest_lap" \
        ,"time" \
        ,"points" \
        ,"position" \
        ,"results_file_date") \
        .withColumn("created_date",current_timestamp()) \
        .withColumnRenamed("race_timestamp","race_date") \
        .withColumnRenamed("results_file_date","file_date")
display(final_df)

# COMMAND ----------

# # final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")
# overwrite_records(final_df, 'results_race_id', 'f1_presentation', 'race_results')

# COMMAND ----------

merge_condition = 'tgt.results_race_id = src.results_race_id and tgt.driver_name = src.driver_name'
merge_delta_data(final_df, 'results_race_id', presentation_folder_path, merge_condition, 'f1_presentation','race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC use f1_presentation

# COMMAND ----------

# MAGIC %sql
# MAGIC select results_race_id, count(*) 
# MAGIC from f1_presentation.race_results
# MAGIC group by results_race_id
# MAGIC order by results_race_id desc;

# COMMAND ----------

# %sql
# drop table f1_presentation.race_results;

# COMMAND ----------


