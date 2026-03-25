# FinFlow — Real-Time Financial Transaction Pipeline

An end-to-end data engineering portfolio project simulating a 
bank transaction processing system built on Apache Hadoop ecosystem.

## Stack
- **Containerization:** Docker Compose
- **Storage:** HDFS (Hadoop 3.2)
- **Processing:** Apache PySpark 3.5
- **Warehouse:** Apache Hive
- **Transformation:** DBT
- **Orchestration:** Apache Airflow 2.8
- **Data Quality:** Great Expectations
- **Reporting:** Apache Impala

## Architecture
```
Python Generator → HDFS → PySpark → Hive → DBT → Airflow → Reverse ETL
```

## Quick Start
```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/finflow.git
cd finflow

# Start the full stack
docker-compose up -d

# Verify all containers
docker ps
```

## Project Structure
```
finflow/
├── docker/              # Docker config and environment files
├── ingestion/           # Transaction data generator
├── processing/          # PySpark cleaning and fraud detection
├── warehouse/           # Hive DDL and table definitions
├── airflow/dags/        # Airflow pipeline DAGs
├── dbt/                 # DBT transformation models
├── reporting/           # Impala reporting queries
└── docs/                # Architecture diagrams
```

## Pipeline Phases
- **Phase 1:** Data ingestion — Python generates transactions → HDFS
- **Phase 2:** Processing — PySpark cleans and detects fraud
- **Phase 3:** Warehouse — Hive star schema + DBT models
- **Phase 4:** Orchestration — Airflow DAG runs every hour
- **Phase 5:** Reverse ETL — Fraud alerts to Slack + CRM sync

## Status
In Progress