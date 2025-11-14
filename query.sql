-- query.sql
-- Join customers, orders, payments, and shipments to produce the requested output

SELECT
  c.customer_id,
  c.name AS customer_name,
  o.order_id,
  o.order_date,
  o.total_amount,
  p.method AS payment_method,
  p.amount AS payment_amount,
  s.status AS shipment_status,
  s.shipment_date
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN payments p ON p.order_id = o.order_id
LEFT JOIN shipments s ON s.order_id = o.order_id
ORDER BY datetime(o.order_date) DESC;
