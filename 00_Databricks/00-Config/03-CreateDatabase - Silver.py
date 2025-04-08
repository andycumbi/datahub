# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook Definition  
# MAGIC
# MAGIC **Name**: 01 - CreateDatabase - Silver
# MAGIC </br> **Created By** : Andy Cumbicus amcumbicus@gmail.com
# MAGIC </br> **Created Date** : 2025-03-04
# MAGIC
# MAGIC </br>
# MAGIC ## Control Version
# MAGIC
# MAGIC | **Version** | **Date**            | **Modified Name**           | **Description**           |
# MAGIC |-------------|---------------------|-----------------------------|---------------------------|
# MAGIC |1.0|2025-02-04|Andy Cumbicus amcumbicus@gmail.com| Schema Silver Setup |

# COMMAND ----------

# MAGIC %md
# MAGIC ##Parameters

# COMMAND ----------

# Global Params
datalake = os.environ.get("DATALAKE")
env = os.environ.get("ENVIRONMENT")  

#DB Params
databaseName = "silver"
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

tableFolder = "Sales_out"
origen = "SQLServer"

spark.sql(f"""
CREATE OR REPLACE TABLE {catalogName}.{databaseName}.{tableFolder} (
    SaleId INT,
    CustomerID INT,
    ProductID INT,
    Quantity INT,
    UnitPrice DECIMAL(,2),
    TotalAmount DECIMAL(10,2),
    SaleDate TIMESTAMP,
    StoreLocation STRING,
    PaymentMethod STRING,
    EmployeeID INT,
    Discount DECIMAL(5,2),
    IsOnline BOOLEAN)
LOCATION '{adlsPath}/{origen}/{tableFolder}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Origen [Sharepoint] Tables

# COMMAND ----------

tableFolder = "Sales_in"
origen = "Sharepoint"

spark.sql(f"""
CREATE OR REPLACE TABLE  {catalogName}.{databaseName}.{tableFolder} (
    Id INT,
    ProductID INT,
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    TotalAmount DECIMAL(10,2),
    SaleDate TIMESTAMP,
    StoreLocation STRING,
    PaymentMethod STRING,
    Discount DECIMAL(5,2),
    IsOnline BOOLEAN)
LOCATION '{adlsPath}/{origen}/{tableFolder}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create Origen [SAP] Tables

# COMMAND ----------

tableFolder = "Products"
origen = "SAP"

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
LOCATION '{adlsPath}/{origen}/{tableFolder}'
""")