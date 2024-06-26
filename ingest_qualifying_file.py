# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Lap_times JSON

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import current_timestamp, col, concat, lit

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("position", IntegerType(), True), \
                                    StructField("q1", StringType(), True), \
                                    StructField("q2", StringType(), True), \
                                    StructField("q3", StringType(), True)])



# COMMAND ----------

qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .option("multiLine",True) \
    .json(f"{raw_folder_path}/{v_file_date}/qualifying")
qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id") \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("constructorId","constructor_id") \
    .withColumn("ingestion_date",current_timestamp()) \
    .withColumn("data_source",lit(v_data_source)) \
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# overwrite_records(qualifying_final_df, 'race_id', 'f1_processed', 'qualifying')

# COMMAND ----------

merge_condition = 'tgt.qualify_id = src.qualify_id'
merge_delta_data(qualifying_final_df, 'race_id', processed_folder_path, merge_condition, 'f1_processed','qualifying')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(*)
# MAGIC from f1_processed.qualifying
# MAGIC group by race_id
# MAGIC order by race_id desc;

# COMMAND ----------


