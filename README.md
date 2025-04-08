# datahub
Data projecto framework

# Data Processing Project Documentation

## Overview
This project integrates Azure Databricks and Azure Data Factory to create an end-to-end data processing solution. Databricks handles the data transformation logic through notebooks, while Data Factory manages the orchestration, connections, and execution pipeline.

## Architecture
### Azure Databricks
- Contains notebooks with data processing logic
- Performs data transformation, cleaning, and analysis
- Executes code in a distributed computing environment
- Notebooks are parameterized to receive inputs from Data Factory

### Azure Data Factory
- Orchestrates the end-to-end data pipeline
- Manages connections to data sources and destinations
- Schedules and triggers notebook execution in Databricks
- Handles dependencies between pipeline activities

## Data Factory Components

### Linked Services
- **AzureDatabricks**: Connection to Databricks workspace
- **AzureDataLakeStorage**: Connection to data lake storage
- **AzureSqlDatabase**: Connection to SQL databases
- **AzureKeyVault**: Connection to secrets management

### Datasets
- **SourceData**: Points to raw data in data lake

### Pipelines
- **MainETLPipeline**: Orchestrates the complete ETL process
- **DailyIncrementalLoad**: Handles daily data processing
- **DataQualityCheck**: Performs data quality validation

### Triggers
- **DailyScheduleTrigger**: Runs pipelines on a daily schedule
- **EventBasedTrigger**: Triggers pipelines when new data arrives

## Data Lake Structure
```
data-lake/
├── config/
│   ├── setup/
│   ├── configtable/
│   ├── log/
├── bronze/
│   ├── source1/
│   ├── source2/
│   └── source3/
├── silver/
│   ├── tran2/
│   └── tran1/
├── gold/
│   ├── dim/
│   └── fact/

```

## Setup and Configuration

### Prerequisites
- Azure subscription
- Proper IAM permissions for resource creation
- Service Principal with appropriate access

### Databricks Setup
1. Create a Databricks workspace
2. Configure clusters with required libraries
3. Import notebooks from the repository
4. Set up workspace access control

### Data Factory Setup
1. Create a Data Factory instance
2. Configure linked services with appropriate connection strings
3. Create datasets pointing to source and destination locations
4. Build pipelines that call Databricks notebooks
5. Set up monitoring and alerts


## Monitoring and Logging
- Pipeline runs are logged in Data Factory monitoring
- Detailed execution logs are available in Databricks
- Alert mechanisms are configured for pipeline failures
- Activity run history is maintained for auditing


## Future Enhancements
- Implement CI/CD for automated deployment
- Add unit and integration testing
- Optimize performance with fine-tuned configurations
