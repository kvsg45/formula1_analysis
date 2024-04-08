# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Drivers JSON

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import current_timestamp, col, concat, lit

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])



# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/results.json")
results_final_df = results_df.withColumnRenamed("resultId","result_id") \
    .withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("driverId","driver_id") \
    .withColumnRenamed("constructorId","constructor_id") \
    .withColumnRenamed("positionText","position_text") \
    .withColumnRenamed("positionOrder","position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
    .withColumn("ingestion_date",current_timestamp()) \
    .withColumn("data_source",lit(v_data_source)) \
    .withColumn("file_date",lit(v_file_date)) \
    .drop("statusId")


# COMMAND ----------

result_deduped_df = results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method - 1

# COMMAND ----------

# for x in results_final_df.select("race_id").distinct().collect():
#     if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"alter table f1_processed.results drop if exists partition (race_id = {x.race_id})")


# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method - 2

# COMMAND ----------

# overwrite_records(results_final_df, 'race_id', 'f1_processed', 'results')

# COMMAND ----------

# merge_delta_data(df, partition_col, folder_path, merge_condition, db_name, table_name)

# COMMAND ----------

merge_condition = 'tgt.result_id=src.result_id and tgt.race_id = src.race_id'
merge_delta_data(result_deduped_df, 'race_id', processed_folder_path, merge_condition, 'f1_processed','results')

# COMMAND ----------

# spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true") # useful for large data projects

# from delta.tables import DeltaTable
# if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     deltaTable = DeltaTable.forPath(spark, f"{processed_folder_path}/results")
#     deltaTable.alias("tgt").merge(
#       results_final_df.alias("src"),
#       "tgt.result_id=src.result_id and tgt.race_id = src.race_id") \
#         .whenMatchedUpdateAll()\
#         .whenNotMatchedInsertAll()\
#         .execute()
# else:
#     results_final_df.write.mode("append").partitionBy("race_id").format("delta").saveAsTable("f1_processed.results")


# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# results_final_df = results_final_df.select('result_id', 'driver_id', 'constructor_id', 'number', 'grid', 'position', 'position_text', 'position_order', 'points', 'laps', 'time', 'milliseconds', 'fastest_lap', 'rank', 'fastest_lap_time', 'fastest_lap_speed', 'ingestion_date', 'data_source', 'file_date','race_id')

# COMMAND ----------

# if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(*)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, driver_id, count(*) as total
# MAGIC from f1_processed.results
# MAGIC group by race_id, driver_id
# MAGIC having count(*)>1
# MAGIC order by race_id, driver_id desc;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table f1_processed.results;

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

x = results_final_df.select('number')
display(x)


# COMMAND ----------

cols = results_final_df.columns
print(cols)
for x in cols:
    if x == 'number':
        cols.remove(x)
cols.append('number')
modified_df = results_final_df.select(cols)
display(modified_df)

# COMMAND ----------


