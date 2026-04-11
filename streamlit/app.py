import streamlit as st
import pandas as pd
import psycopg2

st.set_page_config(page_title="Marketplace Analytics", layout="wide")

DB_CONFIG = {
    "host": "postgres-dwh",
    "database": "dwh",
    "user": "dwh_user",
    "password": "dwh_password",
    "port": 5432,
}


def get_data(query):
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# ----------------------------------
# TITLE
# ----------------------------------
st.title(" Marketplace Analytics Dashboard")

# ----------------------------------
# LOAD DATA
# ----------------------------------
df = get_data("SELECT * FROM analytics.seller_revenue_daily")

if df.empty:
    st.warning("No data available")
    st.stop()

# ----------------------------------
# KPI CARDS
# ----------------------------------
st.header(" Key Metrics")

col1, col2, col3 = st.columns(3)

total_revenue = df["revenue"].sum()
avg_revenue = df["revenue"].mean()
nb_sellers = df["seller_id"].nunique()

col1.metric("💰 Total Revenue", f"{total_revenue:,.0f}")
col2.metric("📊 Avg Revenue", f"{avg_revenue:,.0f}")
col3.metric("🧑‍💼 Active Sellers", nb_sellers)

# ----------------------------------
# REVENUE TREND
# ----------------------------------
st.header(" Revenue Trend")

daily = df.groupby("dt")["revenue"].sum().reset_index()
st.line_chart(daily, x="dt", y="revenue")

# ----------------------------------
# TOP SELLERS (BAR CHART)
# ----------------------------------
st.header(" Top Sellers")

top = (
    df.groupby("seller_id")["revenue"]
    .sum()
    .reset_index()
    .sort_values(by="revenue", ascending=False)
    .head(10)
)

st.bar_chart(top.set_index("seller_id"))

# ----------------------------------
# ANOMALIES
# ----------------------------------
st.header(" Anomalies")

df_anomalies = get_data("""
    SELECT seller_id, dt, value
    FROM analytics.anomalies
    ORDER BY dt DESC
""")

if df_anomalies.empty:
    st.success("No anomalies detected 🎉")
else:
    st.error(f"{len(df_anomalies)} anomalies detected")
    st.dataframe(df_anomalies)

# ----------------------------------
# DEBUG SECTION (OPTIONAL)
# ----------------------------------
with st.expander("🔍 Raw Data"):
    st.dataframe(df)