# Databricks notebook source
# MAGIC %sql
# MAGIC select '${Name}' AS Name;
# MAGIC select '${PipelineBronze}' AS PipelineBronze;
# MAGIC select '${Stage}' AS Stage;
# MAGIC select '${NotebookPath}' AS NotebookPath;
# MAGIC select '${Status}' AS Status;
# MAGIC select '${ErrorMessage}' AS ErrorMessage;
# MAGIC
# MAGIC INSERT INTO config.ExecutionLogs  (Name,PipelineBronze,Stage,NotebookPath,Status,ErrorMessage, ExecutionDate)
# MAGIC VALUES ("${Name}","${PipelineBronze}","${Stage}","${NotebookPath}","${Status}","${ErrorMessage}",cast(current_timestamp() as TIMESTAMP));