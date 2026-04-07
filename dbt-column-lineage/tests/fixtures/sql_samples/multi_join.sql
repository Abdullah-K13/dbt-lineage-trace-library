SELECT
    o.order_id,
    o.customer_id,
    c.name AS customer_name,
    o.amount
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
