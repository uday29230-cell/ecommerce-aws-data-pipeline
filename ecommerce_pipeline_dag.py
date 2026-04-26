"""
E-Commerce Sales Pipeline — Airflow DAG
Orchestrates: S3 Upload Check → Glue ETL → Redshift Load → Data Quality → Notification
Schedule: Daily at 02:00 AM UTC (nightly batch)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule
import logging

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# DAG Config
# ──────────────────────────────────────────────
S3_BUCKET       = "your-ecommerce-bucket"
S3_RAW_PREFIX   = "raw/ecommerce/"
S3_PROC_PREFIX  = "processed/ecommerce/"
GLUE_JOB_NAME   = "ecommerce-sales-etl"
REDSHIFT_CONN   = "redshift_default"
AWS_CONN        = "aws_default"

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "start_date":       datetime(2025, 1, 1),
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
    "email":            ["data-alerts@yourcompany.com"],
}

# ──────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────
with DAG(
    dag_id="ecommerce_sales_pipeline",
    default_args=default_args,
    description="Nightly e-commerce sales ETL: S3 → Glue → Redshift",
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "etl", "aws", "production"],
) as dag:

    # ── 1. Start
    start = DummyOperator(task_id="start")

    # ── 2. Wait for raw file to land in S3 (up to 2 hours)
    wait_for_raw_file = S3KeySensor(
        task_id="wait_for_raw_file",
        bucket_name=S3_BUCKET,
        bucket_key=f"{S3_RAW_PREFIX}{{{{ ds_nodash }}}}/ecommerce_orders_{{{{ ds }}}}.csv",
        aws_conn_id=AWS_CONN,
        poke_interval=60,       # check every 60 seconds
        timeout=7200,           # timeout after 2 hours
        mode="reschedule",      # free up worker slot while waiting
    )

    # ── 3. Run AWS Glue ETL Job
    run_glue_etl = GlueJobOperator(
        task_id="run_glue_etl",
        job_name=GLUE_JOB_NAME,
        aws_conn_id=AWS_CONN,
        script_args={
            "--S3_INPUT_PATH":  f"s3://{S3_BUCKET}/{S3_RAW_PREFIX}",
            "--S3_OUTPUT_PATH": f"s3://{S3_BUCKET}/{S3_PROC_PREFIX}",
            "--JOB_NAME":       GLUE_JOB_NAME,
        },
        concurrent_run_limit=1,
        wait_for_completion=True,
    )

    # ── 4. Load Fact Orders into Redshift
    load_fact_orders = RedshiftSQLOperator(
        task_id="load_fact_orders",
        redshift_conn_id=REDSHIFT_CONN,
        sql="""
            -- Truncate staging and reload
            TRUNCATE TABLE staging.fact_orders;

            COPY staging.fact_orders
            FROM 's3://{{ var.value.s3_bucket }}/processed/ecommerce/fact_orders/'
            IAM_ROLE '{{ var.value.redshift_iam_role }}'
            FORMAT AS PARQUET;

            -- Upsert into production table
            BEGIN;
                DELETE FROM prod.fact_orders
                WHERE order_id IN (SELECT order_id FROM staging.fact_orders);

                INSERT INTO prod.fact_orders
                SELECT * FROM staging.fact_orders;
            COMMIT;
        """,
    )

    # ── 5. Load Aggregation Tables
    load_daily_sales = RedshiftSQLOperator(
        task_id="load_daily_sales",
        redshift_conn_id=REDSHIFT_CONN,
        sql="""
            TRUNCATE TABLE prod.agg_daily_sales;
            COPY prod.agg_daily_sales
            FROM 's3://{{ var.value.s3_bucket }}/processed/ecommerce/agg_daily_sales/'
            IAM_ROLE '{{ var.value.redshift_iam_role }}'
            FORMAT AS PARQUET;
        """,
    )

    load_customer_rfm = RedshiftSQLOperator(
        task_id="load_customer_rfm",
        redshift_conn_id=REDSHIFT_CONN,
        sql="""
            TRUNCATE TABLE prod.agg_customer_rfm;
            COPY prod.agg_customer_rfm
            FROM 's3://{{ var.value.s3_bucket }}/processed/ecommerce/agg_customer_rfm/'
            IAM_ROLE '{{ var.value.redshift_iam_role }}'
            FORMAT AS PARQUET;
        """,
    )

    load_product_perf = RedshiftSQLOperator(
        task_id="load_product_performance",
        redshift_conn_id=REDSHIFT_CONN,
        sql="""
            TRUNCATE TABLE prod.agg_product_performance;
            COPY prod.agg_product_performance
            FROM 's3://{{ var.value.s3_bucket }}/processed/ecommerce/agg_product_performance/'
            IAM_ROLE '{{ var.value.redshift_iam_role }}'
            FORMAT AS PARQUET;
        """,
    )

    # ── 6. Data Quality Checks
    def run_dq_checks(**context):
        """Run post-load data quality checks on Redshift."""
        from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
        hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN)

        checks = [
            ("Row count check",        "SELECT COUNT(*) FROM prod.fact_orders WHERE order_date::date = CURRENT_DATE - 1",         1),
            ("Null order_id check",     "SELECT COUNT(*) FROM prod.fact_orders WHERE order_id IS NULL",                           0),
            ("Negative revenue check",  "SELECT COUNT(*) FROM prod.fact_orders WHERE revenue < 0",                                0),
            ("Duplicate order check",   "SELECT COUNT(*) FROM (SELECT order_id, COUNT(*) c FROM prod.fact_orders GROUP BY 1 HAVING c > 1)", 0),
        ]

        failed = []
        for name, query, expected_min in checks:
            result = hook.get_first(query)[0]
            if name == "Row count check" and result < expected_min:
                failed.append(f"FAILED: {name} — got {result}, expected >= {expected_min}")
                logger.error(f"DQ Check FAILED: {name}")
            elif name != "Row count check" and result != expected_min:
                failed.append(f"FAILED: {name} — got {result}, expected {expected_min}")
                logger.error(f"DQ Check FAILED: {name}")
            else:
                logger.info(f"DQ Check PASSED: {name} — result: {result}")

        if failed:
            raise ValueError(f"Data Quality checks failed:\n" + "\n".join(failed))

        logger.info("All Data Quality checks passed.")

    data_quality_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_dq_checks,
        provide_context=True,
    )

    # ── 7. End
    end = DummyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ──────────────────────────────────────────────
    # Pipeline Dependencies
    # ──────────────────────────────────────────────
    start >> wait_for_raw_file >> run_glue_etl

    run_glue_etl >> [load_fact_orders, load_daily_sales, load_customer_rfm, load_product_perf]

    [load_fact_orders, load_daily_sales, load_customer_rfm, load_product_perf] >> data_quality_checks

    data_quality_checks >> end
