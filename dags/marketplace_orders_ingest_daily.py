from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import json

from hooks.marketplace_api_hook import MarketplaceAPIHook
from utils.minio_client import MinioClient

DB_CONFIG = {
    "host": "postgres-dwh",
    "database": "dwh",
    "user": "dwh_user",
    "password": "dwh_password",
    "port": 5432,
}


# EXTRACT WITH HOOK
def extract_orders(**context):
    ds = context["ds"]

    hook = MarketplaceAPIHook()
    orders = hook.get_orders(ds)

    minio = MinioClient()
    minio.upload_json(
        data={"date": ds, "orders": orders},
        path_prefix="bronze/orders"
    )

    return orders

# LOAD RAW
def load_raw_orders(**context):
    ds = context["ds"]
    orders = context["ti"].xcom_pull(task_ids="extract_orders")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("DELETE FROM raw.raw_orders WHERE dt = %s", (ds,))

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