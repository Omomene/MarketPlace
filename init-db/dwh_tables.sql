CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE dwh.fact_orders (
    order_id TEXT,
    seller_id TEXT,
    customer_id TEXT,
    product_id TEXT,
    dt DATE,
    quantity INT,
    total_amount NUMERIC,
    status TEXT,
    PRIMARY KEY (order_id, dt)
);

CREATE TABLE dwh.dim_seller (
    seller_id TEXT PRIMARY KEY,
    name TEXT,
    country TEXT,
    joined_date DATE
);

CREATE TABLE dwh.dim_customer (
    customer_id TEXT PRIMARY KEY,
    email TEXT,
    city TEXT,
    signup_date DATE
);

CREATE TABLE dwh.dim_product (
    product_id TEXT PRIMARY KEY,
    name TEXT,
    category TEXT,
    base_price NUMERIC
);

CREATE TABLE dwh.dim_date (
    dt DATE PRIMARY KEY,
    year INT,
    month INT,
    day_of_week INT
);