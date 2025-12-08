-- create_dimension_tables.sql
-- Slowly changing (type 1) dimensional structures

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    signup_date DATE,
    country TEXT,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    unit_price NUMERIC,
    created_at TIMESTAMP DEFAULT now()
);
