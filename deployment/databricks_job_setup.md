# Databricks Workflow / Job Configuration

## Job Name
**lakebridge_daily_etl_pipeline**

## Purpose
This Databricks Workflow orchestrates the end-to-end ETL pipeline for the LakeBridge multi-tenant data platform.
It automates ingestion, transformation, gold-layer modeling, incremental processing, and dashboard refresh preparation.

---

## Task Sequence

1. **bronze_ingestion**
   - Ingest raw source files from AWS S3 into Bronze layer Delta tables

2. **silver_transformation**
   - Clean, standardize, validate, and deduplicate Bronze data into Silver layer

3. **gold_modeling**
   - Build Gold layer fact and dimension tables for analytics

4. **incremental_merge**
   - Process only new or changed records using Delta MERGE logic

5. **dashboard_kpi_refresh**
   - Execute SQL queries or prepare final KPI datasets for dashboard consumption

---

## Task Dependency Flow

bronze_ingestion → silver_transformation → gold_modeling → incremental_merge → dashboard_kpi_refresh

- `silver_transformation` runs only after `bronze_ingestion` succeeds
- `gold_modeling` runs only after `silver_transformation` succeeds
- `incremental_merge` runs only after `gold_modeling` succeeds
- `dashboard_kpi_refresh` runs only after `incremental_merge` succeeds

---

## Schedule

**Daily at 11:00 PM IST**

> For portfolio/demo environments, this workflow can also be triggered manually.

---

## Failure Handling

- If any upstream task fails, downstream tasks do not execute
- Failed tasks can be retried from the Databricks Workflows UI
- Logging and validation checkpoints are included in each stage

---

## Cluster / Compute Configuration

- Runtime: Databricks Runtime (PySpark enabled)
- Compute Type: Job Cluster / Shared Cluster
- Language: Python + SQL
- Storage Layer: AWS S3
- Table Format: Delta Lake

---

## Workflow DAG Screenshot

See: `/Atlikon BI 360 2026-04-07 20_30.pdf`

---

## Notes

This workflow is designed to simulate a production-style medallion architecture pipeline:
- **Bronze** = Raw ingestion
- **Silver** = Cleansed and standardized data
- **Gold** = Analytics-ready fact and dimension tables
