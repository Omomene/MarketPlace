from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import psycopg2
import json
import boto3

from data_quality_operator import DataQualityOperator

DB_CONFIG = {
    "host": "postgres-dwh",
    "database": "dwh",
    "user": "dwh_user",
    "password": "dwh_password",
    "port": 5432,
}

# -----------------------
# MINIO CLIENT
# -----------------------
def get_minio_client():
    conn = BaseHook.get_connection("minio_local")

    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name="us-east-1"
    )


# -----------------------
# EXTRACT FROM MINIO
# -----------------------
def extract_raw(**context):
    ds = context["ds"]

    client = get_minio_client()
    bucket = "data-lake"

    key = f"bronze/orders/dt={ds}/data.json"

    response = client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")

    payload = json.loads(content)

    # IMPORTANT: your JSON contains {"date": "...", "orders": [...]}
    data = payload.get("orders", [])

    if not isinstance(data, list):
        raise ValueError("Invalid MinIO format: 'orders' must be a list")

    return data


# -----------------------
# LOAD DIMENSIONS (SAFE VERSION)
# -----------------------
def load_dimensions(**context):
    data = context["ti"].xcom_pull(task_ids="extract_raw")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for row in data:

        # SELLER DIMENSION
        cur.execute("""
            INSERT INTO dwh.dim_seller (seller_id, name, country, joined_date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (seller_id) DO NOTHING
        """, (
            row.get("seller_id"),
            row.get("seller_name", "unknown"),
            row.get("seller_country", "unknown"),
            None
        ))

        # CUSTOMER DIMENSION
        cur.execute("""
            INSERT INTO dwh.dim_customer (customer_id, email, city, signup_date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING
        """, (
            row.get("customer_id"),
            row.get("customer_email", "unknown"),
            row.get("customer_city", "unknown"),
            None
        ))

        # PRODUCT DIMENSION
        cur.execute("""
            INSERT INTO dwh.dim_product (product_id, name, category, base_price)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING
        """, (
            row.get("product_id"),
            row.get("product_name", "unknown"),
            row.get("product_category", "unknown"),
            0
        ))

    conn.commit()
    cur.close()
    conn.close()


# -----------------------
# LOAD FACT TABLE
# -----------------------
def load_fact(**context):
    data = context["ti"].xcom_pull(task_ids="extract_raw")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for row in data:

        cur.execute("""
            INSERT INTO dwh.fact_orders (
                order_id, seller_id, customer_id,
                product_id, dt, quantity, total_amount, status
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (order_id, dt)
            DO UPDATE SET
                seller_id = EXCLUDED.seller_id,
                customer_id = EXCLUDED.customer_id,
                product_id = EXCLUDED.product_id,
                quantity = EXCLUDED.quantity,
                total_amount = EXCLUDED.total_amount,
                status = EXCLUDED.status
        """, (
            row["order_id"],
            row.get("seller_id"),
            row.get("customer_id"),
            row.get("product_id"),
            row.get("dt"),
            row.get("quantity"),
            row.get("total_amount"),
            row.get("status")
        ))

    conn.commit()
    cur.close()
    conn.close()


# -----------------------
# DAG
# -----------------------
with DAG(
    dag_id="marketplace_dwh_build_daily",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dwh", "star_schema"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_raw",
        python_callable=extract_raw,
    )

    dq_fact = DataQualityOperator(
        task_id="dq_fact_table",
        table="dwh.fact_orders",
        pk=["order_id", "dt"],
        expected_columns=[
            "order_id",
            "seller_id",
            "customer_id",
            "product_id",
            "dt",
            "quantity",
            "total_amount",
            "status",
        ],
    )

    load_dims = PythonOperator(
        task_id="load_dimensions",
        python_callable=load_dimensions,
    )

    load_fact_task = PythonOperator(
        task_id="load_fact",
        python_callable=load_fact,
    )

    extract_task >> dq_fact >> load_dims >> load_fact_task