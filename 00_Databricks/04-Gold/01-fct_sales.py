# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook Definition  
# MAGIC
# MAGIC **Name**: 01-Fct_sales
# MAGIC </br> **Description**: Fct_sales
# MAGIC </br> **Created By** : Andy Cumbicus amcumbicus@gmail.com
# MAGIC </br> **Created Date** : 2025-04-07
# MAGIC </br> **App**: [bronze]
# MAGIC </br>
# MAGIC </br> **is_incremental**: false
# MAGIC </br>**Pipeline that execute it**: p-SAP_silver_to_gold
# MAGIC </br> **Cluster**: Cluster ID
# MAGIC </br> **Source Files/Tables**: ctl_river_dev.silver.sales_in
# MAGIC </br> **Source Files/Tables**: ctl_river_dev.silver.sales_out
# MAGIC </br> **Target Files/Tables**: ctl_river_dev.gold.fct_sales
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
sourceDatabaseName = "silver"
sourceTableName = "sales_in"
sourceTable = f"{sourceCatalogName}.{sourceDatabaseName}.{sourceTableName}"
sourceTableName1 = "sales_out"
sourceTable1 = f"{sourceCatalogName}.{sourceDatabaseName}.{sourceTableName1}"

#Target source
targetCatalogName = f"ctl_{project}_{env}"
targetDatabaseName = "gold"
targetTableName = "fct_sales"
targetTable = f"{targetCatalogName}.{targetDatabaseName}.{targetTableName}"
targertPath = f"abfss://{container}@{datalake}.dfs.core.windows.net/{targetDatabaseName}/{targetTableNamesor}"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load data

# COMMAND ----------

bronze_df = spark.read(sourceTable) 
bronze_df1 = spark.read(sourceTable1) 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transformations

# COMMAND ----------

#Cast Value
union_df = bronze_df.union(bronze_df1) \
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