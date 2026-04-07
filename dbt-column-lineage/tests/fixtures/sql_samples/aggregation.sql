SELECT customer_id, SUM(amount) AS total_amount, COUNT(*) AS order_count
FROM orders
GROUP BY customer_id
