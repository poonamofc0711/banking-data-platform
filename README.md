# Banking Data Platform 🏦

End-to-end banking analytics pipeline built on **Azure** using 
**Medallion Architecture** (Bronze → Silver → Gold).

## Architecture
![Medallion Architecture](docs/architecture/medallion.png)

## Tech Stack
| Tool | Purpose |
|------|---------|
| Azure Data Lake Gen2 | Storage for all layers |
| Azure Databricks + PySpark | Data transformation |
| Azure Data Factory | Pipeline orchestration |
| Azure Synapse Analytics | SQL serving layer |
| Azure Key Vault | Secrets management |
| Delta Lake | Table format (ACID transactions) |
| GitHub + Azure DevOps | Version control + CI/CD |

## Data Scale
- **1,000,000** raw banking transactions
- **50TB–200TB** design capacity
- Partitioned by year/month for query optimization

## Medallion Layers

### 🥉 Bronze — Raw Ingestion
- Exact copy of source data in Delta format
- Preserves all raw data including nulls and duplicates
- Partitioned by year/month

### 🥈 Silver — Cleansed & Standardized  
- Removed 53,361 bad records (nulls, duplicates, future dates)
- Standardized currency codes (7 variants → 4 ISO codes)
- Deduplicated using PySpark window functions
- Added derived business columns (amount_usd, is_fraud, etc.)

### 🥇 Gold — Business Aggregates
| Table | Records | Purpose |
|-------|---------|---------|
| daily_txn_summary | 171,041 | Executive dashboards |
| customer_360 | 9,895 | Relationship managers |
| fraud_intelligence | 25,380 | Risk/fraud teams |

## Key Results
- **Fraud Exposure Detected**: $64,455,293
- **High Risk Customers**: 5,729
- **Data Quality**: 53,361 bad records caught and removed
- **Currency Standardization**: 7 variants → 4 ISO codes

## Project Structure
| Notebook | Layer | Description |
|----------|-------|-------------|
| 01_data_generation_bronze | Bronze | Raw data ingestion |
| 02_silver_transformation | Silver | Cleaning & standardization |
| 03_gold_aggregations | Gold | Business KPIs & analytics |

## Pipeline
ADF orchestrates Bronze → Silver → Gold automatically
CI/CD via Azure DevOps triggers on every GitHub push