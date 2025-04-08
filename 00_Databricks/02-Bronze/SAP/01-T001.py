# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook Definition  
# MAGIC
# MAGIC **Name**: T001
# MAGIC </br> **Description**: t001
# MAGIC </br> **Created By** : Andy Cumbicus amcumbicus@gmail.com
# MAGIC </br> **Created Date** : 2025-04-07
# MAGIC </br> **App**: [bronze]
# MAGIC </br>
# MAGIC </br> **is_incremental**: false
# MAGIC </br>**Pipeline that execute it**: p-SAP_bronze_to_silver
# MAGIC </br> **Cluster**: Cluster ID
# MAGIC </br> **Source Files/Tables**: bronze/SAP/T001/T001.parquet
# MAGIC </br> **Target Files/Tables**: ctl_river_dev.bronze.t001
# MAGIC </br>
# MAGIC ## Control Version
# MAGIC
# MAGIC | **Version** | **Date**            | **Modified Name**           | **Description**           |
# MAGIC |-------------|---------------------|-----------------------------|---------------------------|
# MAGIC |1.0|2025-04-07|Andy Cumbicus amcumbicus@gmail.com|Extract table|
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import functions

# COMMAND ----------


import os
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC #### Params

# COMMAND ----------

#Global Params
datalake = os.environ.get("DATALAKE")
env = os.environ.get("ENVIRONMENT")
project = "river"

#Table details
source = "SAP"
tableName = "T001"
adlsPath = f"abfss://{project}@{env}{datalake}.dfs.core.windows.net/{source}/{tableName}"
format = "*.parquet"

#PCatalog
catalogName = f"ctl_{project}_{env}"
spark.catalog.setCurrentCatalog(catalogName)

#Path
databaseName = "bronze"
adlsPath_ods = f"abfss://{container}@{datalake}.dfs.core.windows.net/{databaseName}/{source}/{tableName}"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load data

# COMMAND ----------

parquet_df = spark.read.parquet(f"{adlsPath}/{format}") 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Control Fields

# COMMAND ----------

ods_df = parquet_df.selectExpr("*", "_metadata.file_path as _tmp") \
    .withColumn("LoadDate", to_date(regexp_extract(col("_tmp"), datePattern, 1),'yyyyMMdd').cast("date")) \
    .withColumn("CurrentUser", lit(current_user())) \
    .drop("_tmp")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data

# COMMAND ----------

unionDf \
    .write \
	.format("delta") \
	.mode("overwrite") \
 	.option("mergeSchema", "true") \
    .option("path",adlsPath_ods)\
	.saveAsTable(f"{databaseName}.{tableName}") 