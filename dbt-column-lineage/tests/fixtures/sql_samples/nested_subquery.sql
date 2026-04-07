SELECT order_id, total_amount
FROM (
    SELECT order_id, SUM(amount) AS total_amount
    FROM line_items
    GROUP BY order_id
) subq
WHERE total_amount > 100
