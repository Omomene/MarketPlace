import streamlit as st
import pandas as pd
import psycopg2

st.title("Marketplace Dashboard")

conn = psycopg2.connect(
    host="postgres-dwh",
    database="dwh",
    user="dwh_user",
    password="dwh_password",
    port=5432
)

df = pd.read_sql("SELECT * FROM raw_orders", conn)

st.dataframe(df)