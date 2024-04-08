# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date",current_timestamp())
    return output_df

# COMMAND ----------

def overwrite_records(df,partition_col,db_name, table_name):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    modified_df = rearrange_columns(df,partition_col)
    if(spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        modified_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        modified_df.write.mode("append").partitionBy(f"{partition_col}").format("parquet").saveAsTable(f"{db_name}.{table_name}")



# COMMAND ----------

def rearrange_columns(df, partition_col):
    cols = df.columns
    for x in cols:
        if x == partition_col:
            cols.remove(f'{partition_col}')
    cols.append(f'{partition_col}')
    return df.select(cols)


# COMMAND ----------

def merge_delta_data(df, partition_col, folder_path, merge_condition, db_name, table_name):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true") # useful for large data projects

    from delta.tables import DeltaTable
    if(spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(df.alias("src"),merge_condition) \
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        df.write.mode("append").partitionBy(f"{partition_col}").format("delta").saveAsTable(f"{db_name}.{table_name}")
