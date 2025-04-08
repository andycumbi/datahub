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
# MAGIC ###Create Gold Tables

# COMMAND ----------

tableFolder = "Fct_Sales"

spark.sql(f"""
CREATE OR REPLACE TABLE {catalogName}.{databaseName}.{tableFolder} (
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
    IsOnline BOOLEAN)
LOCATION '{adlsPath}{tableFolder}'
""")

# COMMAND ----------

tableFolder = "Dim_Products"

spark.sql(f"""
CREATE OR REPLACE TABLE {catalogName}.{databaseName}.{tableFolder} (
    ProductID INT,
    ProductName STRING,
    Description STRING,
    CategoryID INT,
    BrandID INT,
    SupplierID INT,
    UnitPrice DECIMAL(10,2),
    CostPrice DECIMAL(10,2),
    StockQuantity INT,
    ReorderLevel INT,
    Weight DECIMAL(8,3),
    Dimensions STRING,
    SKU STRING,
    Barcode STRING,
    IsActive BOOLEAN,
    CreatedDate TIMESTAMP,
    LastModifiedDate TIMESTAMP)
LOCATION '{adlsPath}/{tableFolder}'
""")