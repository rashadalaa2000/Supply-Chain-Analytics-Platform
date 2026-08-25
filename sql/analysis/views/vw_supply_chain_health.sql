CREATE OR ALTER VIEW vw_supply_chain_health AS
    
WITH SupplyChain_Data_Check AS (
    SELECT 
        f.order_id,
        f.order_status,
        d.delivery_status,
        p.payment_status,
        f.line_total,
        f.area_id,
        p.payment_method
    FROM fact_order_details f
    LEFT JOIN fact_payments p ON f.order_id = p.order_id
    LEFT JOIN fact_deliveries d ON f.order_id = d.order_id
)
SELECT 
    order_status,
    delivery_status,
    payment_status,
    payment_method,
    COUNT(DISTINCT order_id) AS orders_count,
    SUM(line_total) AS total_value, 
    
CASE 
    WHEN delivery_status = 'Delivered' AND payment_status = 'Paid'
        THEN 'Completed - Revenue Realized'
    WHEN delivery_status = 'Delivered' AND payment_status = 'Pending' AND payment_method LIKE 'Net-%'
        THEN 'Completed - Healthy Accrual'
    WHEN delivery_status = 'Delivered' AND payment_status = 'Failed'
        THEN 'Completed - Payment Failed'
    WHEN delivery_status = 'Delivered' AND payment_status = 'Pending' AND payment_method NOT LIKE 'Net-%'
        THEN 'Completed - Payment Pending'
    WHEN order_status = 'Cancelled' AND delivery_status = 'In Transit'
        THEN 'Cancelled - In Transit'
    WHEN order_status = 'Cancelled' AND delivery_status = 'Delivered'
        THEN 'Cancelled - Already Delivered'
    WHEN order_status = 'Cancelled' AND delivery_status = 'Delayed'
        THEN 'Cancelled - Delayed Shipment'
    WHEN order_status = 'Cancelled' AND delivery_status = 'Not Dispatched' AND payment_status = 'Refunded'
        THEN 'Cancelled - Refunded'
    WHEN order_status = 'Cancelled' AND delivery_status = 'Not Dispatched' AND payment_status = 'Paid'
        THEN 'Cancelled - Refund Pending'
    WHEN order_status = 'Cancelled' AND delivery_status = 'Not Dispatched' AND payment_status = 'Pending'
        THEN 'Cancelled - No Payment'
    WHEN payment_status = 'Paid' AND delivery_status = 'Failed'
        THEN 'In Progress - Delivery Failed'
    WHEN payment_status = 'Paid' AND delivery_status = 'Delayed'
        THEN 'In Progress - Delivery Delayed'
    WHEN delivery_status = 'Failed' AND payment_status = 'Failed'
        THEN 'Failed - Delivery and Payment'
    WHEN delivery_status = 'Failed' AND payment_status = 'Pending'
        THEN 'Failed - Delivery Issue'
    WHEN payment_status = 'Refunded' AND delivery_status != 'Not Dispatched'
        THEN 'Refunded - Already Shipped'
    WHEN payment_status = 'Refunded' AND delivery_status = 'Not Dispatched'
        THEN 'Refunded - Not Shipped'
    WHEN delivery_status = 'In Transit' AND payment_status = 'Failed'
        THEN 'In Progress - Payment Failed'
    WHEN delivery_status = 'In Transit' AND payment_status = 'Pending'
        THEN 'In Progress - Payment Pending'
    WHEN delivery_status = 'In Transit' AND payment_status = 'Paid'
        THEN 'In Progress - Payment Received'
    WHEN delivery_status = 'Delayed' AND payment_status = 'Failed'
        THEN 'In Progress - Delayed and Payment Failed'
    WHEN delivery_status = 'Delayed' AND payment_status = 'Pending'
        THEN 'In Progress - Delayed and Payment Pending'
    WHEN order_status = 'Pending' AND delivery_status = 'Delivered' AND payment_status = 'Paid'
        THEN 'Completed - Status Out of Sync'
    ELSE 'Uncategorized'
END AS business_impact
FROM SupplyChain_Data_Check
GROUP BY 
    order_status, 
    delivery_status, 
    payment_status,
    payment_method;
