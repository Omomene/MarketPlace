from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import psycopg2

DB_CONFIG = {
    "host": "postgres-dwh",
    "database": "dwh",
    "user": "dwh_user",
    "password": "dwh_password",
    "port": 5432,
}

THRESHOLD = 0.30


# -------------------------
# ANOMALY DETECTION
# -------------------------
def detect_anomalies(**context):
    ds = context["ds"]

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # 1. Get today's revenue per seller
    cur.execute("""
        SELECT seller_id, SUM(total_amount)
        FROM dwh.fact_orders
        WHERE dt = %s
        GROUP BY seller_id
    """, (ds,))

    today_rows = cur.fetchall()

    # 2. Idempotence (clean rerun)
    cur.execute("""
        DELETE FROM analytics.anomalies
        WHERE dt = %s AND metric = 'revenue_drop'
    """, (ds,))

    # 3. Loop sellers
    for seller_id, today_revenue in today_rows:

        # IMPORTANT FIX: avoid NULL crashes
        if today_revenue is None:
            continue

        # 4. Compute 7-day avg BEFORE today
        cur.execute("""
            SELECT AVG(daily_revenue)
            FROM (
                SELECT dt, SUM(total_amount) AS daily_revenue
                FROM dwh.fact_orders
                WHERE seller_id = %s
                  AND dt < %s
                  AND dt >= %s::date - INTERVAL '7 days'
                GROUP BY dt
            ) t
        """, (seller_id, ds, ds))

        avg_7d = cur.fetchone()[0]

        if not avg_7d or avg_7d == 0:
            continue

        drop = (avg_7d - today_revenue) / avg_7d

        # 5. Insert anomaly if rule triggered
        if drop > THRESHOLD:
            cur.execute("""
                INSERT INTO analytics.anomalies (
                    seller_id,
                    dt,
                    metric,
                    value,
                    threshold
                )
                VALUES (%s, %s, %s, %s, %s)
            """, (
                seller_id,
                ds,
                "revenue_drop",
                drop,
                THRESHOLD
            ))

    conn.commit()
    cur.close()
    conn.close()


# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="marketplace_anomaly_detect_daily",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["analytics", "anomaly"],
) as dag:

    detect_task = PythonOperator(
        task_id="detect_anomalies",
        python_callable=detect_anomalies,
    )

    detect_task