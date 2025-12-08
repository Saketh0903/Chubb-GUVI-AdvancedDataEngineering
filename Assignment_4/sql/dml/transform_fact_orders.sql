-- transform_fact_orders.sql
-- Main fact transformation and load

WITH cleaned AS (
    SELECT
        order_id::text,
        order_timestamp::timestamptz AS ts_utc,
        customer_id::text,
        product_id::text,
        quantity::int,
        total_amount::numeric,
        COALESCE(NULLIF(currency,''),'USD') AS currency,
        status
    FROM stg_orders
),
valid AS (
    SELECT *
    FROM cleaned
    WHERE order_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND product_id IS NOT NULL
      AND quantity >= 0
      AND total_amount IS NOT NULL
),
flagged AS (
    SELECT *,
        (UPPER(currency) <> 'USD') AS currency_mismatch_flag
    FROM valid
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY ts_utc DESC NULLS LAST
        ) AS rn
    FROM flagged
),
dedup AS (
    SELECT * FROM ranked WHERE rn = 1
)
INSERT INTO fact_orders (
    order_id,
    order_timestamp,
    customer_id,
    product_id,
    quantity,
    total_amount,
    currency,
    currency_mismatch_flag,
    status
)
SELECT
    order_id,
    ts_utc,
    customer_id,
    product_id,
    quantity,
    total_amount,
    currency,
    currency_mismatch_flag,
    status
FROM dedup
ON CONFLICT (order_id) DO UPDATE
SET
    order_timestamp        = EXCLUDED.order_timestamp,
    customer_id            = EXCLUDED.customer_id,
    product_id             = EXCLUDED.product_id,
    quantity               = EXCLUDED.quantity,
    total_amount           = EXCLUDED.total_amount,
    currency               = EXCLUDED.currency,
    currency_mismatch_flag = EXCLUDED.currency_mismatch_flag,
    status                 = EXCLUDED.status;
