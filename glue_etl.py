"""
E-Commerce Sales Data Pipeline
AWS Glue PySpark ETL Job
Author: Data Engineering Consultant
Description: Ingests raw e-commerce CSV data from S3, transforms and aggregates,
             then loads clean Parquet output back to S3 for Redshift Spectrum / Athena querying.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType
from pyspark.sql.window import Window
import logging

# ──────────────────────────────────────────────
# Setup
# ──────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_OUTPUT_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

S3_INPUT  = args['S3_INPUT_PATH']   # e.g. s3://your-bucket/raw/ecommerce/
S3_OUTPUT = args['S3_OUTPUT_PATH']  # e.g. s3://your-bucket/processed/ecommerce/

# ──────────────────────────────────────────────
# 1. INGESTION — Read raw CSV from S3
# ──────────────────────────────────────────────
logger.info(f"Reading raw data from {S3_INPUT}")

raw_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("mode", "PERMISSIVE") \
    .csv(S3_INPUT)

logger.info(f"Raw record count: {raw_df.count()}")
raw_df.printSchema()

# ──────────────────────────────────────────────
# 2. DATA QUALITY — Validation & Null Checks
# ──────────────────────────────────────────────
def validate_data(df):
    """Drop records with critical null fields and log counts."""
    critical_cols = ['order_id', 'customer_id', 'product_id', 'order_date', 'quantity', 'unit_price']
    before = df.count()
    df = df.dropna(subset=critical_cols)
    after = df.count()
    dropped = before - after
    logger.info(f"Data Quality: Dropped {dropped} records with nulls in critical columns.")
    return df

raw_df = validate_data(raw_df)

# ──────────────────────────────────────────────
# 3. TRANSFORMATION — Clean & Enrich
# ──────────────────────────────────────────────
logger.info("Starting transformations...")

transformed_df = raw_df \
    .withColumn("order_date",     F.to_timestamp(F.col("order_date"), "yyyy-MM-dd")) \
    .withColumn("quantity",       F.col("quantity").cast(IntegerType())) \
    .withColumn("unit_price",     F.col("unit_price").cast(DoubleType())) \
    .withColumn("discount",       F.coalesce(F.col("discount").cast(DoubleType()), F.lit(0.0))) \
    .withColumn("revenue",        F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")), 2)) \
    .withColumn("order_year",     F.year(F.col("order_date"))) \
    .withColumn("order_month",    F.month(F.col("order_date"))) \
    .withColumn("order_day",      F.dayofmonth(F.col("order_date"))) \
    .withColumn("order_quarter",  F.quarter(F.col("order_date"))) \
    .withColumn("is_weekend",     F.dayofweek(F.col("order_date")).isin([1, 7]).cast("boolean")) \
    .withColumn("customer_id",    F.trim(F.upper(F.col("customer_id")))) \
    .withColumn("product_id",     F.trim(F.upper(F.col("product_id")))) \
    .withColumn("category",       F.coalesce(F.col("category"), F.lit("UNKNOWN"))) \
    .withColumn("ingestion_ts",   F.current_timestamp()) \
    .filter(F.col("quantity") > 0) \
    .filter(F.col("unit_price") > 0)

# ──────────────────────────────────────────────
# 4. AGGREGATION — KPI Computation
# ──────────────────────────────────────────────

# Daily Sales Summary
daily_sales = transformed_df.groupBy("order_year", "order_month", "order_day", "category") \
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("quantity").alias("total_units_sold"),
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.round(F.avg("revenue"), 2).alias("avg_order_value"),
        F.round(F.max("revenue"), 2).alias("max_order_value"),
        F.round(F.min("revenue"), 2).alias("min_order_value")
    )

# Product Performance
product_performance = transformed_df.groupBy("product_id", "category") \
    .agg(
        F.sum("quantity").alias("total_units_sold"),
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.countDistinct("order_id").alias("order_count"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price")
    )

# Customer RFM (Recency, Frequency, Monetary)
max_date = transformed_df.agg(F.max("order_date")).collect()[0][0]

customer_rfm = transformed_df.groupBy("customer_id") \
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.countDistinct("order_id").alias("frequency"),
        F.round(F.sum("revenue"), 2).alias("monetary")
    ) \
    .withColumn("recency_days", F.datediff(F.lit(max_date), F.col("last_order_date")))

# Monthly Revenue Trend
monthly_revenue = transformed_df.groupBy("order_year", "order_month", "category") \
    .agg(
        F.round(F.sum("revenue"), 2).alias("monthly_revenue"),
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers")
    ) \
    .withColumn("revenue_rank",
        F.rank().over(Window.partitionBy("order_year", "order_month").orderBy(F.desc("monthly_revenue")))
    )

# ──────────────────────────────────────────────
# 5. WRITE — Partitioned Parquet to S3
# ──────────────────────────────────────────────
logger.info(f"Writing processed data to {S3_OUTPUT}")

# Fact table — partitioned by year/month for Athena efficiency
transformed_df.write \
    .mode("overwrite") \
    .partitionBy("order_year", "order_month") \
    .parquet(f"{S3_OUTPUT}/fact_orders/")

daily_sales.write \
    .mode("overwrite") \
    .partitionBy("order_year", "order_month") \
    .parquet(f"{S3_OUTPUT}/agg_daily_sales/")

product_performance.write \
    .mode("overwrite") \
    .parquet(f"{S3_OUTPUT}/agg_product_performance/")

customer_rfm.write \
    .mode("overwrite") \
    .parquet(f"{S3_OUTPUT}/agg_customer_rfm/")

monthly_revenue.write \
    .mode("overwrite") \
    .partitionBy("order_year", "order_month") \
    .parquet(f"{S3_OUTPUT}/agg_monthly_revenue/")

logger.info("ETL Job completed successfully.")
job.commit()
