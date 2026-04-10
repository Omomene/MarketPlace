CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE analytics.seller_revenue_daily (
    seller_id TEXT,
    dt DATE,
    revenue NUMERIC,
    avg_7d NUMERIC,
    drop_flag BOOLEAN
);

CREATE TABLE analytics.anomalies (
    seller_id TEXT,
    dt DATE,
    metric TEXT,
    value NUMERIC,
    threshold NUMERIC
);