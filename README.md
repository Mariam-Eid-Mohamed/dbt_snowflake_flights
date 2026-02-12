# ✈️ Flight Performance Big Data Pipeline

A modern, scalable Big Data architecture designed to transform millions of raw aviation records into analytics-ready datasets for decision-making in the aviation sector.

---

## 📌 Project Overview

Aviation generates massive volumes of flight data each year, including departure times, cancellations, aircraft details, routes, and delays. However, raw transactional tables are not optimized for analytics.

This project implements a cloud-based ELT pipeline that:

- Automates ingestion from AWS S3
- Builds a layered Snowflake warehouse (Bronze / Silver / Gold)
- Creates a star schema for analytics
- Delivers KPI dashboards via Power BI

---

## 🏗 Architecture Overview

**Pipeline Flow:**

S3 (Raw Files)  
→ Airbyte (Ingestion)  
→ Snowflake RAW Schema (Bronze)  
→ dbt Staging Models (Silver)  
→ dbt Mart Models (Gold)  
→ Power BI Dashboards  

### Key Design Principles:
- Layered architecture (Bronze–Silver–Gold)
- Dimensional modeling (Star Schema)
- Incremental ELT workflows
- Orchestrated execution (Airflow)
- Scalable cloud-native design

---

## 🧱 Data Warehouse Model

### Dimensions
- DIM_DATE
- DIM_AIRLINE
- DIM_AIRPORT

### Fact Table
- FACT_FLIGHTS (grain: 1 flight per date & route)

The dimensional model enables:

- Fast aggregations
- KPI tracking
- Multi-year trend analysis
- Efficient BI slicing & filtering

---

## ⚙️ Tech Stack

| Layer | Technology |
|-------|------------|
| Storage | Amazon S3 |
| Ingestion | Airbyte |
| Warehouse | Snowflake |
| Transformation | dbt |
| Orchestration | Airflow |
| Visualization | Power BI |

---

## 📊 Key Insights Generated

- Average yearly flights: 7.57M
- Cancellation rate: 1.78%
- Diversion rate: 0.22%
- Busiest hubs: New York, Dallas, Chicago
- Weekday traffic dominates 73% of cancellations

---

## 📈 Scalability & Reliability

- Incremental ingestion
- Snowflake elastic scaling
- Retry logic via Airbyte
- DAG-based orchestration via Airflow
- Data validation & monitoring

---

## 🔮 Future Enhancements

- ML-based delay prediction
- Weather & external API integration
- Real-time streaming ingestion
- Advanced anomaly detection

---

## 📂 Repository Structure




Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test

