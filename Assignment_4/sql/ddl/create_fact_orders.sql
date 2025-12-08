-- create_fact_orders.sql
-- Transactional fact table

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id TEXT PRIMARY KEY,
    order_timestamp TIMESTAMPTZ,
    customer_id TEXT,
    product_id TEXT,
    quantity INT,
    total_amount NUMERIC,
    currency TEXT,
    currency_mismatch_flag BOOLEAN DEFAULT FALSE,
    status TEXT,
    created_at TIMESTAMP DEFAULT now()
);
