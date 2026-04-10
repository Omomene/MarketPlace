from airflow import DAG
from datetime import datetime

with DAG(
    dag_id="marketplace_orders_ingest_daily",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:
    pass