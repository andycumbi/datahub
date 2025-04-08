# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook Definition  
# MAGIC
# MAGIC **Name**: 00-Create Catalog
# MAGIC </br> **Description**: Crear el catalogo en la ubicacion correspondiente.
# MAGIC </br> **Created By** : Andy Cumbicus amcumbicus@gmail.com
# MAGIC </br> **Created Date** : 2025-04-04
# MAGIC </br> **App**: [Raw]
# MAGIC
# MAGIC </br>
# MAGIC ## Control Version
# MAGIC
# MAGIC | **Version** | **Date**            | **Modified Name**           | **Description**           |
# MAGIC |-------------|---------------------|-----------------------------|---------------------------|
# MAGIC |1.0|2025-03-04|Andy Cumbicus amcumbicus@gmail.com| Creacion catalog|

# COMMAND ----------

# Global Params
datalake = os.environ.get("DATALAKE")
env = os.environ.get("ENVIRONMENT")  

#Catalog Params
projectName = "river"
catalogName = f"ctl_{env}_{projectName}"
catalogFolder = "UnityCatalog"  

# Path
adlsPath = f"abfss://{projectName}@{env}{datalake}.dfs.core.windows.net/{catalogFolder}"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Unity Catalog in Data Lake

# COMMAND ----------

spark.catalog.createCatalog(
    name=catalogName,
    location=adlsPath,
    ifNotExists=True,
    comment=f"Catálogo para el proyecto {projectName}",
    properties={"owner": "data_team", "environment": env}
        )