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


def build_kpis(**context):
    ds = context["ds"]

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # clean day
    cur.execute("""
        DELETE FROM analytics.seller_revenue_daily
        WHERE dt = %s
    """, (ds,))

    # daily revenue
    cur.execute("""
        INSERT INTO analytics.seller_revenue_daily (seller_id, dt, revenue)
        SELECT
            seller_id,
            dt,
            SUM(total_amount)
        FROM dwh.fact_orders
        WHERE dt = %s
        GROUP BY seller_id, dt
    """, (ds,))

    # rolling avg
    cur.execute("""
        UPDATE analytics.seller_revenue_daily t
        SET avg_7d = sub.avg_7d
        FROM (
            SELECT
                seller_id,
                dt,
                AVG(revenue) OVER (
                    PARTITION BY seller_id
                    ORDER BY dt
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) AS avg_7d
            FROM analytics.seller_revenue_daily
        ) sub
        WHERE t.seller_id = sub.seller_id
          AND t.dt = sub.dt
    """)

    # anomaly flag
    cur.execute("""
        UPDATE analytics.seller_revenue_daily
        SET drop_flag =
            CASE
                WHEN avg_7d IS NOT NULL
                 AND revenue < 0.7 * avg_7d
                THEN TRUE
                ELSE FALSE
            END
        WHERE dt = %s
    """, (ds,))

    conn.commit()
    cur.close()
    conn.close()


with DAG(
    dag_id="marketplace_analytics_aggregate_daily",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["analytics", "kpi"],
) as dag:

    kpi_task = PythonOperator(
        task_id="build_kpis",
        python_callable=build_kpis,
    )

    kpi_task