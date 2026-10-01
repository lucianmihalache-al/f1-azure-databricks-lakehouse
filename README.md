# Formula 1 End-to-End Cloud Data Lakehouse

An production-grade, incremental Data Engineering project implementing a modern **Data Lakehouse architecture** on **Azure Databricks** and **Apache Spark**, built around historical Formula 1 Motor Racing datasets.

## 🚀 Architectural Overview

This platform processes multi-format raw datasets (CSV, split JSON, multi-line JSON) into structured, analytics-ready business insights following the **Medallion Design Pattern** and modern 2026 data governance standards.


```
                  ┌───────────────┐
                  │  Raw Sources  │ (CSV, JSON via Azure Blob Storage)
                  └───────┬───────┘
                          │ (PySpark Ingestion)
                          ▼
               ┌─────────────────────┐
               │    BRONZE LAYER     │ (Raw Ingestion / Append-Only Delta)
               └───────┬─────────────┘
                          │ (Schema Validation & Cleansing)
                          ▼
               ┌─────────────────────┐
               │    SILVER LAYER     │ (Enriched / Cleaned Relational Delta Tables)
               └───────┬─────────────┘
                          │ (Aggregations & Analytical Business Logic)
                          ▼
               ┌─────────────────────┐
               │     GOLD LAYER      │ (Star/Snowflake Schema / Aggregated Data)
               └───────┬─────────────┘
                          │
                          ▼
            ┌───────────────────────────┐
            │   Databricks SQL / BI     │ (Reporting & Downstream Consumption)
            └───────────────────────────┘                        
   
```

## 🛠️ Tech Stack & Modern Capabilities

* **Compute & Processing:** Apache Spark (PySpark & Spark SQL) for high-performance distributed data transformations.
* **Storage Framework:** Delta Lake to ensure ACID transactions, time travel capabilities, and schema enforcement.
* **Data Governance:** Integrated **Unity Catalog** for centralized data lineage, secure data access controls, and object cataloging.
* **Orchestration:** Automated incremental scheduling workflows built via **Lakeflow Jobs**.
* **Analytics Delivery:** Engineered analytical views and serving layers optimized for **Databricks SQL Dashboards**.

## 📁 Pipeline Implementation Details

1. **Bronze (Ingestion Layer):** Ingests raw data from cloud storage, enforces strict schema mapping where required, and stores them as append-only Delta tables. Handles nested JSON flattening dynamically.
2. **Silver (Transformation Layer):** Cleanses data by handling null values, applying data type casting, renaming columns for business alignment, and performing complex relational joins (Drivers, Results, Constructors, Races).
3. **Gold (Presentation Layer):** Produces multi-dimensional analytical views and fact/dimension tables tailored for management metrics (e.g., Driver Standings, Team Dominance, Circuit Performance).

## 📈 Key Engineering Patterns Demonstrated

* **Incremental Data Loading:** Implemented optimized delta processing to ensure pipelines only ingest new/modified records, significantly minimizing compute overhead.
* **Data Quality Checks:** Deployed runtime assertion points to filter anomalous records before reaching production-ready Silver layers.
* **Centralized Security:** Leveraged Unity Catalog schemas to demonstrate cross-functional data isolation and enterprise-grade role-based access management.
