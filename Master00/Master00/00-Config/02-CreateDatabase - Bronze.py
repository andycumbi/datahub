# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook Definition  
# MAGIC
# MAGIC **Name**: 01 - CreateDatabase - Bronze
# MAGIC </br> **Created By** : Andy Cumbicus amcumbicus@gmail.com
# MAGIC </br> **Created Date** : 2025-03-04
# MAGIC
# MAGIC </br>
# MAGIC ## Control Version
# MAGIC
# MAGIC | **Version** | **Date**            | **Modified Name**           | **Description**           |
# MAGIC |-------------|---------------------|-----------------------------|---------------------------|
# MAGIC |1.0|2025-02-04|Andy Cumbicus amcumbicus@gmail.com| Schema Bronze Setup |

# COMMAND ----------

# MAGIC %md
# MAGIC ##Parameters

# COMMAND ----------

# Global Params
datalake = os.environ.get("DATALAKE")
env = os.environ.get("ENVIRONMENT")  

#DB Params
databaseName = "bronze"
projectName = "river"
catalogName = f"ctl_{env}_{projectName}"

# Path
adlsPath = f"abfss://{projectName}@{env}{datalake}.dfs.core.windows.net/{databaseName}"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create DB

# COMMAND ----------

spark.catalog.createDatabase(
    name=schemaName,
    catalog=catalogName,
    location=f"{adlsPath}",
    ifNotExists=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Origen [SQL Server] Tables

# COMMAND ----------

tableFolder = "T001"
origen = "SQLServer"

spark.sql(f"""
CREATE OR REPLACE TABLE {catalogName}.bronze.{tableFolder} (
    SaleId INT,
    CustomerID INT,
    ProductID INT,
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    TotalAmount DECIMAL(10,2),
    SaleDate TIMESTAMP,
    StoreLocation STRING,
    PaymentMethod STRING,
    EmployeeID INT,
    Discount DECIMAL(5,2),
    IsOnline BOOLEAN,
    source_system STRING,
    ingestion_date TIMESTAMP,
    batch_id STRING,
    file_name STRING,
    file_date STRING,
    _is_deleted BOOLEAN,
    _processing_time TIMESTAMP)
LOCATION '{adlsPath}/bronze/{origen}/{tableFolder}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Origen [Sharepoint] Tables

# COMMAND ----------

tableFolder = "SalesReport"
origen = "Sharepoint"

spark.sql(f"""
CREATE OR REPLACE TABLE {catalogName}.{databaseName}.{tableFolder} (
    OrderID STRING,
    ProductRef STRING,
    ProductName STRING,
    QuantitySold INT,
    Price DECIMAL(10,2),
    TotalSales DECIMAL(10,2),
    Date STRING,
    Store STRING,
    Region STRING,
    SalesRep STRING,
    PaymentType STRING,
    DiscountApplied DECIMAL(5,2),
    DiscountReason STRING,
    CustomerType STRING,
    Notes STRING)
LOCATION '{adlsPath}/{origen}/{tableFolder}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Origen [SAP] Tables

# COMMAND ----------

tableFolder = "SAP_MaterialMaster"
origen = "SAP"

spark.sql(f"""
CREATE OR REPLACE TABLE  {catalogName}.{databaseName}.{tableFolder} (
    MATNR STRING,                   
    MAKTX STRING,                   
    MTART STRING,                   
    MATKL STRING,                  
    MEINS STRING,                   
    MTPOS STRING,                   
    WERKS STRING,                   
    LGORT STRING,                   
    PSTAT STRING,                   
    LVORM BOOLEAN,                  
    BRGEW DECIMAL(13,3),            
    GEWEI STRING,                  
    VOLUM DECIMAL(13,3),            
    VOLEH STRING,                  
    EAN11 STRING,                   
    STPRS DECIMAL(11,2),            
    PEINH INT,                     
    WAERS STRING,                  
    EKGRP STRING,                   
    DISMM STRING,                   
    MAABC STRING,                  
    ERSDA DATE,                   
    ERNAM STRING,                   
    LAEDA DATE,                     
    AENAM STRING,                   
    EXTWG STRING,                   
    MBRSH STRING,                   
    MHDRZ INT,                      
    MHDHB INT,                     
    VKORG STRING,                   
    VTWEG STRING,                   
    TAXM1 STRING                   
)
LOCATION '{adlsPath}/{origen}/{tableFolder}'
""")