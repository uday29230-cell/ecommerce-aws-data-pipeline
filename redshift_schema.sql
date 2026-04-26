-- ============================================================
-- E-Commerce Sales Pipeline — Redshift Schema
-- ============================================================

-- ── Schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS prod;

-- ============================================================
-- STAGING TABLES
-- ============================================================

DROP TABLE IF EXISTS staging.fact_orders;
CREATE TABLE staging.fact_orders (
    order_id        VARCHAR(50)     NOT NULL,
    customer_id     VARCHAR(50)     NOT NULL,
    product_id      VARCHAR(50)     NOT NULL,
    category        VARCHAR(100),
    order_date      TIMESTAMP       NOT NULL,
    order_year      INTEGER,
    order_month     INTEGER,
    order_day       INTEGER,
    order_quarter   INTEGER,
    is_weekend      BOOLEAN,
    quantity        INTEGER,
    unit_price      DECIMAL(10, 2),
    discount        DECIMAL(5, 2),
    revenue         DECIMAL(12, 2),
    ingestion_ts    TIMESTAMP       DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (order_date);

-- ============================================================
-- PRODUCTION TABLES
-- ============================================================

-- Fact: Orders
DROP TABLE IF EXISTS prod.fact_orders;
CREATE TABLE prod.fact_orders (
    order_id        VARCHAR(50)     NOT NULL,
    customer_id     VARCHAR(50)     NOT NULL,
    product_id      VARCHAR(50)     NOT NULL,
    category        VARCHAR(100),
    order_date      TIMESTAMP       NOT NULL,
    order_year      INTEGER         ENCODE AZ64,
    order_month     INTEGER         ENCODE AZ64,
    order_day       INTEGER         ENCODE AZ64,
    order_quarter   INTEGER         ENCODE AZ64,
    is_weekend      BOOLEAN,
    quantity        INTEGER         ENCODE AZ64,
    unit_price      DECIMAL(10, 2)  ENCODE AZ64,
    discount        DECIMAL(5, 2)   ENCODE AZ64,
    revenue         DECIMAL(12, 2)  ENCODE AZ64,
    ingestion_ts    TIMESTAMP       DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (customer_id)
COMPOUND SORTKEY (order_date, category);

-- Aggregate: Daily Sales
DROP TABLE IF EXISTS prod.agg_daily_sales;
CREATE TABLE prod.agg_daily_sales (
    order_year          INTEGER,
    order_month         INTEGER,
    order_day           INTEGER,
    category            VARCHAR(100),
    total_orders        INTEGER,
    unique_customers    INTEGER,
    total_units_sold    INTEGER,
    total_revenue       DECIMAL(14, 2),
    avg_order_value     DECIMAL(10, 2),
    max_order_value     DECIMAL(10, 2),
    min_order_value     DECIMAL(10, 2)
)
DISTSTYLE ALL
SORTKEY (order_year, order_month, order_day);

-- Aggregate: Product Performance
DROP TABLE IF EXISTS prod.agg_product_performance;
CREATE TABLE prod.agg_product_performance (
    product_id          VARCHAR(50),
    category            VARCHAR(100),
    total_units_sold    INTEGER,
    total_revenue       DECIMAL(14, 2),
    order_count         INTEGER,
    avg_unit_price      DECIMAL(10, 2)
)
DISTSTYLE ALL
SORTKEY (total_revenue);

-- Aggregate: Customer RFM
DROP TABLE IF EXISTS prod.agg_customer_rfm;
CREATE TABLE prod.agg_customer_rfm (
    customer_id         VARCHAR(50),
    last_order_date     TIMESTAMP,
    frequency           INTEGER,
    monetary            DECIMAL(14, 2),
    recency_days        INTEGER
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (monetary);

-- Aggregate: Monthly Revenue
DROP TABLE IF EXISTS prod.agg_monthly_revenue;
CREATE TABLE prod.agg_monthly_revenue (
    order_year          INTEGER,
    order_month         INTEGER,
    category            VARCHAR(100),
    monthly_revenue     DECIMAL(14, 2),
    total_orders        INTEGER,
    unique_customers    INTEGER,
    revenue_rank        INTEGER
)
DISTSTYLE ALL
SORTKEY (order_year, order_month);

-- ============================================================
-- ANALYTICAL QUERIES (sample KPIs)
-- ============================================================

-- Top 10 products by revenue this month
SELECT product_id, category, total_revenue, total_units_sold
FROM prod.agg_product_performance
ORDER BY total_revenue DESC
LIMIT 10;

-- Month-over-month revenue growth
SELECT
    order_year,
    order_month,
    SUM(monthly_revenue) AS total_revenue,
    LAG(SUM(monthly_revenue)) OVER (ORDER BY order_year, order_month) AS prev_month_revenue,
    ROUND(
        (SUM(monthly_revenue) - LAG(SUM(monthly_revenue)) OVER (ORDER BY order_year, order_month))
        / NULLIF(LAG(SUM(monthly_revenue)) OVER (ORDER BY order_year, order_month), 0) * 100, 2
    ) AS mom_growth_pct
FROM prod.agg_monthly_revenue
GROUP BY order_year, order_month
ORDER BY order_year, order_month;

-- High value customers (top 20% by monetary value)
SELECT
    customer_id,
    monetary,
    frequency,
    recency_days,
    NTILE(5) OVER (ORDER BY monetary DESC) AS monetary_quintile
FROM prod.agg_customer_rfm
ORDER BY monetary DESC;
