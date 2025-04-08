# Databricks notebook source
# MAGIC %md
# MAGIC ### Import Functions
# MAGIC

# COMMAND ----------

import os
from pyspark.sql import functions 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Config Excel 

# COMMAND ----------

#Global Params
datalake = os.environ.get("DATALAKE")
env = os.environ.get("ENVIROMENT")

#Path
container = "formacion"
source_folder = "Config"
file_folder = "ConfigTables"
file_name = "ConfigTables.xlsx"
adls_path = f"abfss://{container}@{datalake}.dfs.core.windows.net/{source_folder}/{file_folder}/{file_name}"

#Select Catalog
catalog = f"`ctl-{container}-{env}`"
spark.sql(f"use catalog {catalog}")

# COMMAND ----------

# MAGIC %scala
# MAGIC //Load Configurations from Excel  
# MAGIC
# MAGIC //Periodicidad
# MAGIC var configtable_Periodicidad = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").option("dataAddress", "'Sources'!A1").load(adlsPath)
# MAGIC configtable_Periodicidad.createOrReplaceTempView("VSourcesConfig")
# MAGIC
# MAGIC //ExtractionConfig
# MAGIC var configtable_ExtractionConfig = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").option("dataAddress", "'ExtractionConfig'!A1").load(adlsPath)
# MAGIC configtable_ExtractionConfig.createOrReplaceTempView("VExtractionConfig")

# COMMAND ----------

# MAGIC %sql 
# MAGIC TRUNCATE TABLE config.SourcesConfig;
# MAGIC INSERT INTO confign.SourcesConfig (Id, Descripcion, IsActive)
# MAGIC SELECT
# MAGIC     Id INT,
# MAGIC     Name STRING,
# MAGIC     SourceSystem STRING,
# MAGIC     ConnectionString STRING,
# MAGIC     Query STRING,
# MAGIC     Comments STRING,
# MAGIC     IsActive INT
# MAGIC FROM VSourcesConfig
# MAGIC where Id is not null;
# MAGIC
# MAGIC TRUNCATE TABLE config.ExtractionConfig;
# MAGIC INSERT INTO config.ExtractionConfig (Id, Name, ExecutionDays, ExecutionWeekDay, PeriodicidadId, IsActive)
# MAGIC SELECT 
# MAGIC     Id INT,
# MAGIC     Name STRING,
# MAGIC     Source_id INT,
# MAGIC     ExecutionDays STRING,
# MAGIC     ExecutionWeekDay STRING,
# MAGIC     Periodicidad INT,
# MAGIC     IsActive INT
# MAGIC
# MAGIC FROM VExtractionConfig
# MAGIC where Id is not null;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution calendar

# COMMAND ----------

# MAGIC %sql
# MAGIC --Vista temporal con los campos del excel exploded
# MAGIC create or replace temp view exploded_table as
# MAGIC SELECT 
# MAGIC *
# MAGIC , -- Convertir la lista de dias de extraccion en n filas con un dia de extraccion cada una. Si esta vacia pone el dia anterior al actual
# MAGIC   explode(
# MAGIC     split(
# MAGIC     NVL(ExecutionDays, extract(DAY FROM date_add(day, -1, current_date()))),
# MAGIC     ';'
# MAGIC     )
# MAGIC   ) AS ExtractDay
# MAGIC , --Concvertir la lista de dias de la semana de extraccion en n filas.
# MAGIC   explode(
# MAGIC     split(
# MAGIC     NVL(ExecutionWeekDay, ''),
# MAGIC     ';'
# MAGIC     )
# MAGIC   ) AS WeekExplode
# MAGIC
# MAGIC  -- Sustituir los dias de extraccion a numero segun la codificacion de la funcion weekday (0:Lunes,1:Martes, etc.)
# MAGIC ,case
# MAGIC   when WeekExplode = 'Monday' then 0
# MAGIC   when WeekExplode = 'Tuesday' then 1
# MAGIC   when WeekExplode = 'Wednesday' then 2
# MAGIC   when WeekExplode = 'Tuesday' then 3
# MAGIC   when WeekExplode = 'Friday' then 4
# MAGIC   when WeekExplode = 'Saturday' then 5
# MAGIC   when WeekExplode = 'Sunday' then 6
# MAGIC end as ExtractWeek
# MAGIC --Obtener el dia que deberia haberse extraido este mes. Para las semanales y diarias pone el dia anterior al actual
# MAGIC ,case 
# MAGIC   when PeriodicidadId in (1,6) then date_format(date_add(day,-1, current_date()), 'yyyy-MM-dd')
# MAGIC   else to_date(concat(extract(YEAR FROM current_date()),'-',LPAD(extract(MONTH FROM current_date()),2,0), '-', LPAD(ExtractDay,2,0)), 'yyyy-MM-dd')
# MAGIC end as ExtraccionMesActual
# MAGIC FROM config.ExtractionConfig

# COMMAND ----------

# MAGIC %md
# MAGIC ###Coming...
# MAGIC