-- upsert_dim_products.sql
-- Load product dimension (Type 1)

INSERT INTO dim_products (
    product_id,
    product_name,
    category,
    unit_price
)
SELECT
    product_id,
    product_name,
    category,
    unit_price::numeric
FROM stg_products
WHERE product_id IS NOT NULL
ON CONFLICT (product_id)
DO UPDATE SET
    product_name = EXCLUDED.product_name,
    category     = EXCLUDED.category,
    unit_price   = EXCLUDED.unit_price;
