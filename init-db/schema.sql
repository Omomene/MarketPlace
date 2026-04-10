CREATE TABLE IF NOT EXISTS raw_orders (
    id SERIAL PRIMARY KEY,
    order_id TEXT,
    customer_id TEXT,
    amount FLOAT,
    created_at TIMESTAMP
);