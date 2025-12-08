-- upsert_dim_customers.sql
-- Load customer dimension (Type 1 slowly changing)

INSERT INTO dim_customers (
    customer_id,
    first_name,
    last_name,
    email,
    signup_date,
    country
)
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    signup_date::date,
    country
FROM stg_customers
WHERE customer_id IS NOT NULL
ON CONFLICT (customer_id)
DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name  = EXCLUDED.last_name,
    email      = EXCLUDED.email,
    signup_date= EXCLUDED.signup_date,
    country    = EXCLUDED.country;
