# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest races.csv file
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

print(v_file_date)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                     StructField("year",IntegerType(),True),
                                     StructField("round",IntegerType(),True),
                                     StructField("circuitId",IntegerType(),True),
                                     StructField("name",StringType(),True),
                                     StructField("date",DateType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("url",StringType(),True)


])

# COMMAND ----------

races_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/races.csv",header=True)
display(races_df)
#races_df.printSchema()
display(races_schema)

# COMMAND ----------

races_df = spark.read \
    .option("header",True) \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Selecting req columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_df.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),col("date"),col("time"))
display(races_selected_df)

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("year","race_year") \
    .withColumnRenamed("circuitId","circuit_id")
display(races_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit
races_final_df = add_ingestion_date(races_renamed_df.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss')))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

races_final_selected_df = races_final_df.select(col("race_id"),col("race_year"),col("round"),col("circuit_id"),col("name"),col("race_timestamp"),col("ingestion_date")).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))
display(races_final_selected_df)

# COMMAND ----------

# races_final_selected_df.write.mode("overwrite").partitionBy("race_year").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

races_final_selected_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races;

# COMMAND ----------


