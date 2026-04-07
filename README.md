# LakeBridge
Built a multi-tenant data platform for a corporate group, enabling seamless ingestion, transformation, and consolidation of raw business data from multiple Child Companies into standardized, analytics-ready datasets for the Parent Company. Delivered scalable pipelines to support unified reporting, governance, and cross-entity insights.
# Multi-Tenant Data Platform

## Overview
This project simulates a multi-tenant data platform for a corporate group where a Parent Company receives business data from multiple Child Companies. Each child company may provide data in different formats and structures. The platform ingests raw data, applies cleansing and standardization, builds analytics-ready fact and dimension tables, and enables consolidated reporting for the parent company.

## Business Problem
In enterprise groups, multiple child companies often operate with separate source systems, leading to inconsistent raw data formats. The parent company requires a unified, scalable, and reliable platform to consolidate data across entities for centralized reporting, performance tracking, and decision-making.

## Solution
This project implements a layered data pipeline using a lakehouse approach:
- **Landing/Raw Zone** for incoming files
- **Bronze Layer** for raw ingestion
- **Silver Layer** for cleaned and standardized data
- **Gold Layer** for fact and dimension tables

The platform supports:
- Multi-tenant ingestion
- Full load processing
- Incremental load processing
- Data quality checks
- Dimensional modeling
- Parent-level consolidated analytics

## Tech Stack
- Databricks
- PySpark
- Spark SQL
- Delta Lake
- AWS S3 (simulated paths)
- Git & GitHub
- Python

## Architecture
1. Child companies generate raw sales, customer, and product data.
2. Files are landed in the raw/landing zone.
3. Bronze layer stores ingested raw data with metadata.
4. Silver layer performs:
   - Data cleansing
   - Standardization
   - Null handling
   - Date parsing
   - Deduplication
   - Validation
5. Gold layer creates:
   - `dim_customers`
   - `dim_products`
   - `dim_date`
   - `fact_sales`
6. Incremental logic processes only new or changed records.
7. Final tables support consolidated parent-company reporting.

