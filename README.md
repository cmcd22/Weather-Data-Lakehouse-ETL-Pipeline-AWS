# 🌦️ Weather Data Lakehouse ETL Pipeline (AWS)

## Overview

This project implements a **production-style, end-to-end data engineering pipeline** that ingests global weather data from a public API, processes it through a **Bronze / Silver / Gold lakehouse architecture**, and makes analytics-ready outputs available for downstream BI tools.

The pipeline is built using **AWS-native services** and follows best practices around **idempotency, data quality validation, orchestration, and partitioned storage**.

---

## 🧱 Architecture

```
Open-Meteo API
      ↓
Bronze Layer (Raw JSON, S3)
      ↓
Silver Layer (Hourly Parquet, S3)
      ↓
Gold Layer (Daily Aggregates, Parquet, S3)
      ↓
CSV Export (for Tableau Public)
```

**Orchestration:** AWS Glue Workflows  
**Processing:** AWS Glue (PySpark)  
**Storage:** Amazon S3  
**Analytics Format:** Parquet  
**Visualisation:** Tableau Public (via CSV export)

---

## 📂 Repository Structure

```
.
├── src/
│   ├── extract/
│   │   └── get_weather.py
│   ├── transform/
│   │   ├── silver_job.py
│   │   └── gold_job.py
│   └── utils/
│       └── parquet_to_csv.py
│
├── config/
│   ├── cities.json
│   └── config.json
│
├── diagrams/
│   └── architecture.png
│
├── README.md
```

---

## 🥉 Bronze Layer — Raw Ingestion

- Source: Open-Meteo public API  
- Format: JSON  
- Storage: Amazon S3  

**Key features**
- Immutable raw data storage
- Hourly partitioning by city and timestamp
- Idempotent ingestion (safe re-runs)

---

## 🥈 Silver Layer — Cleaned Hourly Data

- Format: Parquet  
- Processing: AWS Glue (PySpark)

**Transformations**
- Explodes hourly arrays
- Normalises schema
- Adds partition columns

**Data Quality Checks**
- Physical bounds on temperature
- Non-negative precipitation and wind metrics
- Valid UV index range
- Non-null timestamps

Jobs fail fast if invalid data is detected.

---

## 🥇 Gold Layer — Daily Aggregates

- Format: Parquet  
- Grain: City × Day  

**Metrics**
- Max / Min / Avg temperature
- Total precipitation
- Rainy hours
- Max wind gust
- Avg wind speed
- Max UV index
- Total solar radiation

**Quality checks ensure analytics-ready outputs only.**

---

## 🔁 Orchestration

The pipeline is orchestrated using **AWS Glue Workflows**:

1. Scheduled start trigger
2. Silver transformation job
3. Event-based trigger launches Gold job on Silver success

This ensures clear dependencies and failure propagation.

---

## 📊 Analytics & Visualisation

Gold data is stored in Parquet for efficiency.  
For Tableau Public compatibility, Gold outputs are exported to CSV using Pandas.

---

## 🧠 Design Decisions

| Decision | Rationale |
|--------|-----------|
| Bronze/Silver/Gold | Separation of concerns |
| Parquet | Columnar analytics performance |
| Glue | Cloud-native scalability |
| Event triggers | Production-style orchestration |
| CSV for BI | Tableau Public limitation |

---

## 🔒 Security

- AWS access handled via IAM roles
- Config files contain non-sensitive settings only

---

## 🎯 Purpose

This project demonstrates real-world data engineering practices, including ingestion, transformation, validation, orchestration, and analytics handoff.

