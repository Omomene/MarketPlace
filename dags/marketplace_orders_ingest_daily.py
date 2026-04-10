from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import psycopg2
import json

API_URL = "http://api-marketplace:5000/orders"

DB_CONFIG = {
    "host": "postgres-dwh",
    "database": "dwh",
    "user": "dwh_user",
    "password": "dwh_password",
    "port": 5432,
}


# -----------------------------
# 1. EXTRACT FROM API
# -----------------------------
def extract_orders(**context):
    ds = context["ds"]  # execution date

    response = requests.get(f"{API_URL}?date={ds}")
    response.raise_for_status()

    orders = response.json()

    # push to next task via XCom
    return orders


# -----------------------------
# 2. LOAD INTO RAW TABLE
# -----------------------------
def load_raw_orders(**context):
    ds = context["ds"]
    orders = context["ti"].xcom_pull(task_ids="extract_orders")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # 🔁 IDMPOTENCY RULE
    cur.execute(
        "DELETE FROM raw.raw_orders WHERE dt = %s",
        (ds,)
    )

    # insert raw JSON
    for order in orders:
        cur.execute(
            """
            INSERT INTO raw.raw_orders (payload, dt)
            VALUES (%s, %s)
            """,
            (json.dumps(order), ds)
        )

    conn.commit()
    cur.close()
    conn.close()


# -----------------------------
# DAG DEFINITION
# -----------------------------
with DAG(
    dag_id="marketplace_orders_ingest_daily",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["marketplace", "ingestion"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_orders,
    )

    load_task = PythonOperator(
        task_id="load_raw_orders",
        python_callable=load_raw_orders,
    )

    extract_task >> load_task