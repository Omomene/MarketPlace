from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import json

from data_quality_operator import DataQualityOperator  # FIXED IMPORT

DB_CONFIG = {
    "host": "postgres-dwh",
    "database": "dwh",
    "user": "dwh_user",
    "password": "dwh_password",
    "port": 5432,
}

# -----------------------
# EXTRACT
# -----------------------
def extract_raw(**context):
    ds = context["ds"]

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT payload
        FROM raw.raw_orders
        WHERE dt = %s
    """, (ds,))

    rows = cur.fetchall()

    cur.close()
    conn.close()

    # FIX JSON ERROR (already safe now)
    return [json.loads(r[0]) if isinstance(r[0], str) else r[0] for r in rows]


# -----------------------
# DIMENSIONS
# -----------------------
def load_dimensions(**context):
    data = context["ti"].xcom_pull(task_ids="extract_raw")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for row in data:

        cur.execute("""
            INSERT INTO dwh.dim_seller (seller_id, name, country, joined_date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (seller_id) DO NOTHING
        """, (row["seller_id"], "unknown", "unknown", None))

        cur.execute("""
            INSERT INTO dwh.dim_customer (customer_id, email, city, signup_date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING
        """, (row["customer_id"], "unknown", "unknown", None))

        cur.execute("""
            INSERT INTO dwh.dim_product (product_id, name, category, base_price)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING
        """, (row["product_id"], "unknown", "unknown", 0))

    conn.commit()
    cur.close()
    conn.close()


# -----------------------
# FACT
# -----------------------   
def load_fact(**context):
    data = context["ti"].xcom_pull(task_ids="extract_raw")
    ds = context["ds"]

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
            row["seller_id"],
            row["customer_id"],
            row["product_id"],
            row["dt"],
            row["quantity"],
            row["total_amount"],
            row["status"]
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

    # ✅ CORRECT ORDER
    extract_task >> dq_fact >> load_dims >> load_fact_task   