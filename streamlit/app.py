import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

# ----------------------------------
# CONFIG
# ----------------------------------
st.set_page_config(
    page_title="Maelys Marketplace",
    layout="wide",
)

px.defaults.color_continuous_scale = "Blues"

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


def format_euro(value):
    return f"{value:,.0f} €".replace(",", " ")


# ----------------------------------
# GLOBAL STYLE
# ----------------------------------
st.markdown("""
<style>

body {
    background-color: #F4F6FA;
}

/* HEADER */
.header {
    background: linear-gradient(90deg, #2A2A72, #009FFD);
    padding: 25px;
    border-radius: 12px;
    text-align: center;
    color: white;
    margin-bottom: 30px;
}

.header h1 {
    margin: 0;
    font-size: 36px;
}

/* CARD */
.card {
    background: white;
    padding: 20px;
    border-radius: 14px;
    box-shadow: 0px 4px 15px rgba(0,0,0,0.08);
    margin-bottom: 20px;
}

/* KPI */
.kpi {
    text-align: center;
}

.kpi h2 {
    margin: 0;
    font-size: 26px;
    color: #2A2A72;
}

.kpi p {
    margin: 0;
    color: #777;
}

/* SECTION TITLE */
.section {
    font-size: 20px;
    font-weight: 600;
    margin-bottom: 10px;
}

</style>
""", unsafe_allow_html=True)


# ----------------------------------
# HEADER
# ----------------------------------
st.markdown("""
<div class="header">
    <h1>Maelys Marketplace</h1>
    <div>Plateforme d'analyse data</div>
</div>
""", unsafe_allow_html=True)


# ----------------------------------
# LOAD DATA
# ----------------------------------
df = get_data("SELECT * FROM analytics.seller_revenue_daily")

if df.empty:
    st.warning("Aucune donnée disponible")
    st.stop()

df["dt"] = pd.to_datetime(df["dt"])
df["dt"] = df["dt"].dt.date


# ----------------------------------
# KPI CARDS
# ----------------------------------
col1, col2, col3, col4 = st.columns(4)

total_revenue = df["revenue"].sum()
avg_revenue = df["revenue"].mean()
nb_sellers = df["seller_id"].nunique()
max_revenue = df["revenue"].max()

with col1:
    st.markdown(f"""
    <div class="card kpi">
        <p>Chiffre d'affaires total</p>
        <h2>{format_euro(total_revenue)}</h2>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="card kpi">
        <p>CA moyen</p>
        <h2>{format_euro(avg_revenue)}</h2>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="card kpi">
        <p>Vendeurs actifs</p>
        <h2>{nb_sellers}</h2>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="card kpi">
        <p>Meilleure journée</p>
        <h2>{format_euro(max_revenue)}</h2>
    </div>
    """, unsafe_allow_html=True)


# ----------------------------------
# REVENUE + TOP SELLERS
# ----------------------------------
col1, col2 = st.columns(2)

daily = df.groupby("dt")["revenue"].sum().reset_index()

with col1:
    st.markdown('<div class="card"><div class="section">Évolution du chiffre d’affaires</div>', unsafe_allow_html=True)

    fig = px.line(daily, x="dt", y="revenue", markers=True)
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), plot_bgcolor="white")
    fig.update_xaxes(type="category") 

    st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)


top_sellers = (
    df.groupby("seller_id")["revenue"]
    .sum()
    .reset_index()
    .sort_values(by="revenue", ascending=False)
    .head(10)
)

with col2:
    st.markdown('<div class="card"><div class="section">Top vendeurs</div>', unsafe_allow_html=True)

    fig = px.bar(top_sellers, x="seller_id", y="revenue")
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
    fig.update_xaxes(type="category") 

    st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)


# ----------------------------------
# CATEGORY + SELLER DETAIL
# ----------------------------------
df_categories = get_data("""
    SELECT p.category, SUM(f.total_amount) AS revenue
    FROM dwh.fact_orders f
    JOIN dwh.dim_product p ON f.product_id = p.product_id
    GROUP BY p.category
""")

col1, col2 = st.columns(2)

with col1:
    st.markdown('<div class="card"><div class="section">Performance par catégorie</div>', unsafe_allow_html=True)

    if not df_categories.empty:
        fig = px.bar(df_categories, x="category", y="revenue")
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.write("Aucune donnée")

    st.markdown('</div>', unsafe_allow_html=True)


with col2:
    st.markdown('<div class="card"><div class="section">Détail vendeur</div>', unsafe_allow_html=True)

    seller = st.selectbox("Sélectionner un vendeur", df["seller_id"].unique())
    seller_df = df[df["seller_id"] == seller]

    fig = px.line(seller_df, x="dt", y="revenue", markers=True)
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10))
    fig.update_xaxes(type="category") 

    st.plotly_chart(fig, use_container_width=True)

    st.markdown('</div>', unsafe_allow_html=True)


# ----------------------------------
# ANOMALIES
# ----------------------------------
df_anomalies = get_data("""
    SELECT seller_id, dt, value
    FROM analytics.anomalies
    ORDER BY dt DESC
""")

st.markdown('<div class="card"><div class="section">Surveillance des anomalies</div>', unsafe_allow_html=True)

if df_anomalies.empty:
    st.write("Aucune anomalie détectée")
else:
    st.dataframe(df_anomalies)

st.markdown('</div>', unsafe_allow_html=True)


# ----------------------------------
# DEBUG
# ----------------------------------
with st.expander("Explorateur de données"):
    st.dataframe(df)