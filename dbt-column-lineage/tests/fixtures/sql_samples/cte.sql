WITH base AS (
    SELECT id, amount FROM raw_orders
),
filtered AS (
    SELECT id AS order_id, amount FROM base WHERE amount > 0
)
SELECT order_id, amount * 100 AS amount_cents FROM filtered
