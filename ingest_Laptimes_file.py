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

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)])



# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/lap_times")


# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source",lit(v_data_source)) \
.withColumn("file_date",lit(v_file_date))


# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

# overwrite_records(lap_times_final_df, 'race_id', 'f1_processed', 'lap_times')

# COMMAND ----------

merge_condition = 'tgt.race_id = src.race_id and tgt.driver_id = src.driver_id and tgt.lap = src.lap'
merge_delta_data(lap_times_final_df, 'race_id', processed_folder_path, merge_condition, 'f1_processed','lap_times')

# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(*)
# MAGIC from f1_processed.lap_times
# MAGIC group by race_id
# MAGIC order by race_id desc;

# COMMAND ----------


