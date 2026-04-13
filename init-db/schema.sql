CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.raw_orders (
    payload JSONB,
    dt DATE
);