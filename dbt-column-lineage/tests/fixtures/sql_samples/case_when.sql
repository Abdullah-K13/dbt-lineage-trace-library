SELECT
    order_id,
    CASE
        WHEN status = 'completed' THEN 1
        ELSE 0
    END AS is_completed
FROM stg_orders
