# E-Commerce Sales Data Pipeline — AWS

An end-to-end production-grade data engineering pipeline built on AWS, processing nightly e-commerce sales data through ingestion, transformation, aggregation, and analytics-ready loading into Redshift.

---

## Architecture

```
S3 (Raw CSV)
    │
    ▼
AWS Glue (PySpark ETL)
    │   ├── Data Validation & Quality Checks
    │   ├── Transformations & Enrichment
    │   └── KPI Aggregations (Daily Sales, RFM, Product Performance)
    │
    ▼
S3 (Processed Parquet — Partitioned)
    │
    ▼
Amazon Redshift (Staging → Production Upsert)
    │
    ▼
Athena / BI Dashboards
```

**Orchestration:** Apache Airflow (daily schedule @ 02:00 UTC)

---

## Tech Stack

| Layer | Tool |
|---|---|
| Cloud | AWS |
| Storage | Amazon S3 |
| Processing | AWS Glue + PySpark |
| Warehouse | Amazon Redshift |
| Query Layer | Amazon Athena |
| Orchestration | Apache Airflow |
| Language | Python 3.9+ |
| Format | Parquet (partitioned) |

---

## Pipeline Steps

### 1. Ingestion
- Raw CSV files land in S3 under `raw/ecommerce/{date}/`
- Airflow S3KeySensor waits for file arrival before triggering ETL

### 2. Transformation (AWS Glue)
- Schema enforcement and type casting
- Null checks and invalid record removal
- Revenue calculation: `quantity × unit_price × (1 - discount)`
- Date enrichment: year, month, day, quarter, is_weekend flags
- Customer and product ID normalization

### 3. Aggregations Produced
| Table | Description |
|---|---|
| `fact_orders` | Clean transactional fact table |
| `agg_daily_sales` | Daily revenue & order KPIs by category |
| `agg_product_performance` | Units sold, revenue, order count per product |
| `agg_customer_rfm` | Recency, Frequency, Monetary per customer |
| `agg_monthly_revenue` | Monthly revenue trends with category ranking |

### 4. Redshift Load
- Staging table load via `COPY` command (Parquet from S3)
- Upsert pattern: delete existing records → insert from staging
- Optimized with `DISTKEY` and `SORTKEY` for analytical queries

### 5. Data Quality Checks
Automated post-load checks via Airflow PythonOperator:
- Row count validation
- Null check on critical columns
- Negative revenue detection
- Duplicate order ID detection

---

## Project Structure

```
ecommerce-pipeline/
├── dags/
│   └── ecommerce_pipeline_dag.py   # Airflow DAG definition
├── scripts/
│   └── glue_etl.py                 # PySpark ETL job (AWS Glue)
├── sql/
│   └── redshift_schema.sql         # DDL + analytical KPI queries
├── docs/
│   └── architecture.png            # Architecture diagram
└── README.md
```

---

## Key Design Decisions

**Partitioned Parquet Output**
Output is partitioned by `order_year/order_month` enabling partition pruning in Athena and Redshift Spectrum — significantly reducing query costs and improving performance.

**Broadcast Joins in PySpark**
Small lookup tables (product metadata, category mapping) are broadcast to avoid shuffle — reduces ETL runtime on large datasets.

**Upsert Pattern in Redshift**
Rather than full truncate-reload, the pipeline uses a staging-based upsert to support late-arriving data corrections without data loss.

**Airflow Sensor with Reschedule Mode**
S3KeySensor uses `mode="reschedule"` to free up worker slots while waiting for file arrival — prevents resource starvation in shared Airflow environments.

---

## Sample KPIs Enabled

- Daily / Monthly revenue by category
- Month-over-month revenue growth %
- Top products by revenue and units sold
- Customer RFM segmentation (High / Mid / Low value)
- Weekend vs weekday sales comparison
- Discount impact on revenue

---

## How to Run Locally (Simulation Mode)

```bash
# Install dependencies
pip install pyspark pandas boto3 apache-airflow

# Run PySpark script locally (replace S3 paths with local paths)
python scripts/glue_etl.py

# Start Airflow locally
airflow db init
airflow webserver --port 8080
airflow scheduler
```

---

## Dataset

Uses the [E-Commerce Data](https://www.kaggle.com/datasets/carrie1/ecommerce-data) dataset from Kaggle (UCI ML Repository) — 500K+ transactions across 8 countries.

---

## Author

Uday M Patel — Senior Data Engineer  
AWS | PySpark | Redshift | Airflow | Data Lake Architecture  
[LinkedIn](#) | [GitHub](#)
