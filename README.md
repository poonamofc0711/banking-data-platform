# Banking Data Platform

End-to-end data engineering project built on Azure using Medallion Architecture.

## Architecture
- **Bronze Layer** — Raw ingestion from banking sources (Delta Lake)
- **Silver Layer** — Cleansed, deduplicated, standardized data
- **Gold Layer** — Business aggregates optimized for analytics

## Tech Stack
| Tool | Purpose |
|------|---------|
| Azure Data Lake Gen2 | Storage for all layers |
| Azure Databricks + PySpark | Data transformation |
| Azure Data Factory | Pipeline orchestration |
| Azure Synapse Analytics | SQL serving layer |
| Azure Key Vault | Secrets management |
| Delta Lake | Table format across all layers |
| Azure DevOps | CI/CD |

## Data Scale
Designed for 50TB–200TB banking transaction data
- 10B+ transaction records
- Incremental loads via CDC pattern
- Z-Order optimization on critical query columns