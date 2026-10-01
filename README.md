# Formula 1 End-to-End Cloud Data Lakehouse

An production-grade, incremental Data Engineering project implementing a modern **Data Lakehouse architecture** on **Azure Databricks** and **Apache Spark**, built around historical Formula 1 Motor Racing datasets.

##  Architectural Overview

This platform processes multi-format raw datasets (CSV, single-line JSON, multi-line JSON) into structured, analytics-ready business insights following the **Medallion Design Pattern** and modern 2026 data governance standards.


```
                  ┌───────────────┐
                  │  Raw Sources  │ (CSV, JSON via Azure Data Lake Storage)
                  └───────┬───────┘
                          │ (PySpark Ingestion)
                          ▼
               ┌─────────────────────┐
               │    BRONZE LAYER     │ (Raw Ingestion -> Delta Tables)
               └───────┬─────────────┘
                          │ (Schema Validation & Cleansing)
                          ▼
               ┌─────────────────────┐
               │    SILVER LAYER     │ (Enriched / Cleaned Relational Delta Tables)
               └───────┬─────────────┘
                          │ (Aggregations & Analytical Business Logic)
                          ▼
               ┌─────────────────────┐
               │     GOLD LAYER      │ (Star Schema / Aggregated Data)
               └───────┬─────────────┘
                          │
                          ▼
            ┌───────────────────────────┐
            │   Databricks SQL / BI     │ (Reporting & Downstream Consumption)
            └───────────────────────────┘                        
   
```

##  Tech Stack & Modern Capabilities

* **Compute & Processing:** Apache Spark (PySpark & Spark SQL) for high-performance distributed data transformations.
* **Storage Framework:** Delta Lake to ensure ACID transactions, time travel capabilities, and schema enforcement.
* **Data Governance:** Integrated **Unity Catalog** for centralized data lineage, secure data access controls, and object cataloging.
* **Orchestration:** Automated full and incremental scheduling workflows built via **Lakeflow Jobs**.
* **Analytics Delivery:** Engineered analytical views and serving layers optimized for **Databricks SQL Dashboards**.

##  Pipeline Implementation Details

1. **Bronze (Ingestion Layer):** Ingests raw data from cloud storage, enforces strict schema mapping where required, and stores them as Delta tables.
2. **Silver (Transformation Layer):** Cleanses data by handling null values, applying data type casting, renaming columns for business alignment, and performing complex relational joins (Drivers, Results, Constructors, Races).
3. **Gold (Presentation Layer):** Produces multi-dimensional analytical views and fact/dimension tables tailored for management metrics (Driver Standings, Team Dominance, Circuit Performance).

