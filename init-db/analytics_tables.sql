CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.seller_revenue_daily (
    seller_id TEXT,
    dt DATE,
    revenue NUMERIC,
    avg_7d NUMERIC,
    drop_flag BOOLEAN,
    CONSTRAINT unique_seller_dt UNIQUE (seller_id, dt)  
);

CREATE TABLE IF NOT EXISTS analytics.anomalies (
    seller_id TEXT,
    dt DATE,
    metric TEXT,
    value NUMERIC,
    threshold NUMERIC
);