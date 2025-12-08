-- truncate_staging.sql
-- Clears staging tables before each run

TRUNCATE TABLE
    stg_customers,
    stg_products,
    stg_orders;
