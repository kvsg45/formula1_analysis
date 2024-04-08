# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date = '{v_file_date}'") \
    .select("race_year").distinct().collect()

# COMMAND ----------

race_years_list = []
for x in race_results_list:
    race_years_list.append(x[0])

# COMMAND ----------

from pyspark.sql.functions import col
race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .withColumnRenamed("constructor_name","team") \
    .filter(col('race_year').isin(race_years_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col
driver_standings_df = race_results_df \
    .groupBy("race_year","driver_name","driver_nationality","team") \
    .agg(sum("points").alias("total_points"),count(when(col("position")==1, True)).alias("wins"))
display(driver_standings_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = driver_standings_df.withColumn("rank",rank().over(driver_rank_spec))
display(final_df)

# COMMAND ----------

constructor_standings_df = race_results_df \
    .groupBy("race_year","team") \
    .agg(sum("points").alias("total_points"),count(when(col("position")==1, True)).alias("wins"))
display(constructor_standings_df)

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_constructor_df = constructor_standings_df.withColumn("rank",rank().over(constructor_rank_spec))
display(final_constructor_df)

# COMMAND ----------

final_deduped_df = final_df.dropDuplicates(['race_year','driver_name'])

# COMMAND ----------

final_constructor_deduped_df = final_constructor_df.dropDuplicates(['race_year','team'])

# COMMAND ----------

# demo_df_group = final_df.groupBy("race_year","driver_name").agg(count("*").alias("total")).sort("race_year",ascending=True)
# display(demo_df_group)

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")
# overwrite_records(final_df, 'race_year', 'f1_presentation', 'driver_standings')


# COMMAND ----------

merge_condition = 'tgt.driver_name = src.driver_name and tgt.race_year = src.race_year'
merge_delta_data(final_deduped_df, 'race_year', presentation_folder_path, merge_condition, 'f1_presentation','driver_standings')

# COMMAND ----------

merge_condition = 'tgt.team = src.team and tgt.race_year = src.race_year'
merge_delta_data(final_constructor_deduped_df, 'race_year', presentation_folder_path, merge_condition, 'f1_presentation','constructor_standings')

# COMMAND ----------

# final_constructor_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")
# overwrite_records(final_constructor_df, 'race_year', 'f1_presentation', 'constructor_standings')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_year, count(*)
# MAGIC from f1_presentation.driver_standings
# MAGIC group by race_year
# MAGIC order by race_year desc;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select race_year, count(*)
# MAGIC from f1_presentation.constructor_standings
# MAGIC group by race_year
# MAGIC order by race_year desc;
