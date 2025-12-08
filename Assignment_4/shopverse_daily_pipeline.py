# dags/shopverse_daily_pipeline.py
"""
Shopverse Daily ETL Pipeline (No PostgresOperator Version)
----------------------------------------------------------
This version uses ONLY:
  - @dag / @task
  - FileSensor
  - PostgresHook
No provider packages are required besides core Airflow.
"""

from __future__ import annotations
import os
import json
import pandas as pd
from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    SLACK_AVAILABLE = True
except Exception:
    SLACK_AVAILABLE = False


# -------------------------------------------------------------------
# CONFIG FROM VARIABLES
# -------------------------------------------------------------------
BASE_PATH = Variable.get("shopverse_data_base_path", "/opt/airflow/data")
SQL_BASE = Variable.get("shopverse_sql_base_path", "/opt/airflow/sql")
MIN_THRESHOLD = int(Variable.get("shopverse_min_order_threshold", "10"))

POSTGRES_CONN = "postgres_dwh"
ANOMALY_DIR = os.path.join(BASE_PATH, "anomalies")
os.makedirs(ANOMALY_DIR, exist_ok=True)

DEFAULT_ARGS = {
    "owner": "shopverse",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------
def processing_date(ds_nodash: str) -> tuple[str, str]:
    """Return (YYYYMMDD, YYYY-MM-DD) for previous day's data."""
    dt = datetime.strptime(ds_nodash, "%Y%m%d") - timedelta(days=1)
    return dt.strftime("%Y%m%d"), dt.strftime("%Y-%m-%d")


def run_sql_file(path: str):
    """Reads an external SQL file and executes it with PostgresHook."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
    sql = open(path).read()
    hook.run(sql)


# -------------------------------------------------------------------
# DAG
# -------------------------------------------------------------------
@dag(
    dag_id="shopverse_daily_pipeline",
    schedule="0 1 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
)
def shopverse_daily_pipeline():

    start = EmptyOperator(task_id="start")

    # -------------------------------------------------------------------
    # FILE SENSORS
    # -------------------------------------------------------------------
    customers_sensor = FileSensor(
        task_id="wait_customers",
        filepath=f"{BASE_PATH}/landing/customers/customers_{{{{  macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d')  }}}}.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        mode="reschedule",
    )

    products_sensor = FileSensor(
        task_id="wait_products",
        filepath=f"{BASE_PATH}/landing/products/products_{{{{  macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d')  }}}}.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        mode="reschedule",
    )

    orders_sensor = FileSensor(
        task_id="wait_orders",
        filepath=f"{BASE_PATH}/landing/orders/orders_{{{{  macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d')  }}}}.json",
        fs_conn_id="fs_default",
        poke_interval=30,
        mode="reschedule",
    )

    # -------------------------------------------------------------------
    # STAGING TaskGroup (Python-only)
    # -------------------------------------------------------------------
    with TaskGroup("staging") as staging:

        @task(task_id="truncate_staging")
        def truncate_staging():
            run_sql_file(f"{SQL_BASE}/ddl/truncate_staging.sql")

        @task
        def load_stg_customers(ds_nodash: str):
            file_date, _ = processing_date(ds_nodash)
            fp = f"{BASE_PATH}/landing/customers/customers_{file_date}.csv"

            df = pd.read_csv(fp, dtype=str)
            df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce").dt.date

            hook = PostgresHook(POSTGRES_CONN)
            df.to_sql("stg_customers", hook.get_sqlalchemy_engine(), if_exists="replace", index=False)
            return len(df)

        @task
        def load_stg_products(ds_nodash: str):
            file_date, _ = processing_date(ds_nodash)
            fp = f"{BASE_PATH}/landing/products/products_{file_date}.csv"

            df = pd.read_csv(fp, dtype=str)
            df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

            hook = PostgresHook(POSTGRES_CONN)
            df.to_sql("stg_products", hook.get_sqlalchemy_engine(), if_exists="replace", index=False)
            return len(df)

        @task
        def load_stg_orders(ds_nodash: str):
            file_date, _ = processing_date(ds_nodash)
            fp = f"{BASE_PATH}/landing/orders/orders_{file_date}.json"

            records = []
            with open(fp) as f:
                try:
                    data = json.load(f)
                    records = data if isinstance(data, list) else [data]
                except:
                    f.seek(0)
                    for line in f:
                        if line.strip():
                            records.append(json.loads(line))

            df = pd.DataFrame(records)
            df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
            df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")

            hook = PostgresHook(POSTGRES_CONN)
            df.to_sql("stg_orders", hook.get_sqlalchemy_engine(), if_exists="replace", index=False)
            return len(df)

        trunc = truncate_staging()
        c = load_stg_customers()
        p = load_stg_products()
        o = load_stg_orders()

        trunc >> [c, p, o]

    # -------------------------------------------------------------------
    # WAREHOUSE TaskGroup (External SQL via PostgresHook)
    # -------------------------------------------------------------------
    with TaskGroup("warehouse") as warehouse:

        @task
        def upsert_dim_customers():
            run_sql_file(f"{SQL_BASE}/dml/upsert_dim_customers.sql")

        @task
        def upsert_dim_products():
            run_sql_file(f"{SQL_BASE}/dml/upsert_dim_products.sql")

        @task
        def transform_fact_orders():
            run_sql_file(f"{SQL_BASE}/dml/transform_fact_orders.sql")

        d1 = upsert_dim_customers()
        d2 = upsert_dim_products()
        f1 = transform_fact_orders()

        d1 >> d2 >> f1

    # -------------------------------------------------------------------
    # DATA QUALITY CHECKS
    # -------------------------------------------------------------------
    @task
    def dq_row_count(sql: str, check_name: str, fail_condition: str):
        """Generic DQ check using PostgresHook."""
        hook = PostgresHook(POSTGRES_CONN)
        count = hook.get_first(sql)[0]

        logging.info(f"DQ {check_name} = {count}")

        if fail_condition == "count==0" and count == 0:
            raise ValueError(f"DQ Failed: {check_name}")
        if fail_condition == "count>0" and count > 0:
            raise ValueError(f"DQ Failed: {check_name}")

        return count

    dq1 = dq_row_count(
        sql="SELECT COUNT(*) FROM dim_customers;",
        check_name="dim_customers_nonzero",
        fail_condition="count==0",
    )

    dq2 = dq_row_count(
        sql="SELECT COUNT(*) FROM fact_orders WHERE customer_id IS NULL OR product_id IS NULL;",
        check_name="fact_no_null_fks",
        fail_condition="count>0",
    )

    @task
    def validate_fact_match(ds_nodash: str):
        _, iso = processing_date(ds_nodash)

        hook = PostgresHook(POSTGRES_CONN)
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM fact_orders WHERE DATE(order_timestamp)=%s;", (iso,))
        fact = cur.fetchone()[0]

        cur.execute("""
            WITH cleaned AS (
                SELECT order_id,
                       (order_timestamp)::timestamptz AS ts,
                       customer_id, product_id,
                       quantity, total_amount
                FROM stg_orders
            )
            SELECT COUNT(*) FROM (
                SELECT DISTINCT ON(order_id) order_id
                FROM cleaned
                WHERE order_id IS NOT NULL
                  AND customer_id IS NOT NULL
                  AND product_id IS NOT NULL
                  AND quantity >= 0
                  AND total_amount IS NOT NULL
                ORDER BY order_id, ts DESC
            ) x;
        """)
        stg_valid = cur.fetchone()[0]

        if fact != stg_valid:
            fn = os.path.join(ANOMALY_DIR, f"dq_mismatch_{iso}.json")
            json.dump({"fact": fact, "stg_valid": stg_valid}, open(fn, "w"))
            raise ValueError(f"Fact mismatch: fact={fact}, staging={stg_valid}")

        return {"fact": fact}

    fact_dq = validate_fact_match()

    # -------------------------------------------------------------------
    # BRANCHING: LOW VOLUME DETECTION
    # -------------------------------------------------------------------
    def decide_branch(ti):
        fact_count = ti.xcom_pull(task_ids="validate_fact_match")["fact"]
        return "warn_low_volume" if fact_count < MIN_THRESHOLD else "normal_completion"

    branch = BranchPythonOperator(
        task_id="branch_volume",
        python_callable=decide_branch,
    )

    @task
    def warn_low_volume(ds_nodash: str):
        _, iso = processing_date(ds_nodash)
        fn = os.path.join(ANOMALY_DIR, f"low_volume_{iso}.json")
        json.dump({"warning": "LOW ORDER COUNT", "date": iso}, open(fn, "w"))
        return fn

    warn = warn_low_volume()
    normal = EmptyOperator(task_id="normal_completion")

    # -------------------------------------------------------------------
    # OPTIONAL SLACK ON FAILURE
    # -------------------------------------------------------------------
    if SLACK_AVAILABLE:
        slack_fail = SlackWebhookOperator(
            task_id="slack_failure",
            slack_webhook_conn_id="slack_alerts",
            message=":red_circle: *Shopverse ETL FAILED!* Check logs.",
            trigger_rule="one_failed",
        )

    else:
        slack_fail = EmptyOperator(task_id="slack_failure")

    # -------------------------------------------------------------------
    # DAG WIRING
    # -------------------------------------------------------------------
     # -------------------------------------------------------------------
    # DAG WIRING (Corrected Slack Failure Handling)
    # -------------------------------------------------------------------
    (
        start
        >> [customers_sensor, products_sensor, orders_sensor]
        >> staging
        >> warehouse
        >> [dq1, dq2, fact_dq]
        >> branch
    )

    # Branch paths
    branch >> warn >> slack_fail     # Low volume → warn → Slack
    branch >> normal                 # Normal path → no Slack

    # Slack should trigger ONLY if a critical ETL or DQ task fails
    [dq1, dq2, fact_dq] >> slack_fail


dag = shopverse_daily_pipeline()
