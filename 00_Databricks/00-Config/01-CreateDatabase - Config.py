# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook Definition  
# MAGIC
# MAGIC **Name**: 01 - CreateDatabase - Config
# MAGIC </br> **Created By** : Andy Cumbicus amcumbicus@gmail.com
# MAGIC </br> **Created Date** : 2025-03-04
# MAGIC
# MAGIC </br>
# MAGIC ## Control Version
# MAGIC
# MAGIC | **Version** | **Date**            | **Modified Name**           | **Description**           |
# MAGIC |-------------|---------------------|-----------------------------|---------------------------|
# MAGIC |1.0|2025-02-04|Andy Cumbicus amcumbicus@gmail.com| Schema Config Setup |

# COMMAND ----------

# MAGIC %md
# MAGIC ##Parameters

# COMMAND ----------

# Global Params
datalake = os.environ.get("DATALAKE")
env = os.environ.get("ENVIRONMENT")  

#DB Params
databaseName = "config"
projectName = "river"
catalogName = f"ctl_{env}_{projectName}"

# Path
adlsPath = f"abfss://{projectName}@{env}{datalake}.dfs.core.windows.net/{catalogFolder}"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Schema

# COMMAND ----------

spark.catalog.createDatabase(
    name=schemaName,
    catalog=catalogName,
    location=f"{adlsPath}",
    ifNotExists=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Config Tables

# COMMAND ----------

tableFolder = "SourcesConfig"

spark.sql(f"""
CREATE OR REPLACE TABLE {databaseName}.{tableFolder} (
    Id INT,
    Name STRING,
    SourceSystem STRING,
    ConnectionString STRING,
    Query STRING,
    Comments STRING,
    IsActive INT)
LOCATION '{adlsPath}/{tableFolder}'
""")

# COMMAND ----------

tableFolder = "ExtractionConfig"

spark.sql(f"""
CREATE OR REPLACE TABLE {databaseName}.{tableFolder} (
    Id INT,
    Name STRING,
    Source_id INT,
    ExecutionDays STRING,
    ExecutionWeekDay STRING,
    Periodicidad INT,
    IsActive INT)
LOCATION '{adlsPath}/{tableFolder}'
""")

# COMMAND ----------

tableFolder = "ExecutionLogs"

spark.sql(f"""
CREATE OR REPLACE TABLE {databaseName}.{tableFolder} (
    LogId INT,
    Name STRING,
    PipelineBronze ,
    Stage STRING,
    NotebookPath STRING,
    Status STRING,
    ErrorMessage STRING,
    ExecutionDate DATE)
LOCATION '{adlsPath}/{tableFolder}'
""")