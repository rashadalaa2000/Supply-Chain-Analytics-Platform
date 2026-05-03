-- eda
SELECT
    (SELECT COUNT(DISTINCT order_id) FROM fact_order_details) AS total_orders,
    (SELECT COUNT(DISTINCT retailer_id) FROM dim_retailers) AS total_retailers,
    (SELECT COUNT(DISTINCT supplier_id) FROM dim_suppliers) AS total_suppliers,
    (SELECT COUNT(DISTINCT driver_id) FROM dim_drivers) AS total_drivers,
    (SELECT COUNT(DISTINCT product_id) FROM dim_products) AS total_products,
    (SELECT ROUND(SUM(line_total), 2) FROM fact_order_details) AS total_revenue,
    (SELECT ROUND(AVG(line_total), 2) FROM fact_order_details) AS avg_order_value,
    (SELECT ROUND(SUM(amount), 2) FROM fact_payments WHERE payment_status IN ('Paid', 'Completed')) AS total_collected,
    (SELECT ROUND(SUM(amount), 2) FROM fact_payments WHERE payment_status NOT IN ('Paid', 'Completed')) AS total_outstanding;


-- Order_to_Cash_Performance
SELECT 
    SUM(CASE WHEN payment_status = 'Paid' AND delivery_status = 'Delivered' THEN line_total ELSE 0 END) AS Realized_Revenue,
    -- (Net Revenue)
    
    SUM(CASE WHEN payment_status = 'Failed' AND delivery_status = 'Delivered' THEN line_total ELSE 0 END) AS Revenue_Loss_Risk,
    -- (Bad Debt / Risk)
    
    SUM(CASE WHEN payment_status = 'Pending' AND delivery_status = 'Delivered' THEN line_total ELSE 0 END) AS Accrued_Revenue
    -- (Pending Pipeline)
    
FROM fact_order_details f
JOIN fact_payments p ON f.order_id = p.order_id
JOIN fact_deliveries d ON d.order_id = p.order_id;
