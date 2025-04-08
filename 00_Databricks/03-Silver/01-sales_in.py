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

#Source details
sourceCatalogName = f"ctl_{project}_{env}"
sourceTableName = "T001"
sourceDatabaseName = "bronze"
sourceTable = f"{sourceCatalogName}.{sourceDatabaseName}.{sourceTableName}"

#Target source
targetCatalogName = f"ctl_{project}_{env}"
targetDatabaseName = "silver"
targetTableName = "sales_in"
targetTable = f"{targetCatalogName}.{targetDatabaseName}.{targetTableName}"
targertPath = f"abfss://{container}@{datalake}.dfs.core.windows.net/{targetDatabaseName}/{targetTableNamesor}"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load data

# COMMAND ----------

bronze_df = spark.read(sourceTable) 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transformations

# COMMAND ----------

#Cast Value
union_df = ods_df \
    .withColumn(
        "Value", 
        regexp_replace(col("Value"), ",", "").cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Data

# COMMAND ----------

unionDf \
  .write \
	.format("delta") \
	.mode("overwrite") \
 	.option("mergeSchema", "true") \
  .option("path",targertPath)\
	.saveAsTable(targetTable) 