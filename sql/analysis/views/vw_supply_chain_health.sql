-- vw_supply_chain_health
CREATE OR ALTER VIEW vw_supply_chain_health AS
SELECT 
    order_status,
    delivery_status,
    payment_status,
    payment_method,
    COUNT(DISTINCT order_id) AS orders_count,
    SUM(payment_amount) AS total_value, 

CASE 
    -- 1) Cancelled orders first — catch these before any Delivered/Paid logic below
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

    -- 2) Status-sync anomaly must come before the general Delivered+Paid rule below,
    --    otherwise it's unreachable
    WHEN order_status = 'Pending' AND delivery_status = 'Delivered' AND payment_status = 'Paid'
        THEN 'Completed - Status Out of Sync'

    -- 3) General Delivered logic
    WHEN delivery_status = 'Delivered' AND payment_status = 'Paid'
        THEN 'Completed - Revenue Realized'
    WHEN delivery_status = 'Delivered' AND payment_status = 'Pending' AND payment_method LIKE 'Net-%'
        THEN 'Completed - Healthy Accrual'
    WHEN delivery_status = 'Delivered' AND payment_status = 'Failed'
        THEN 'Completed - Payment Failed'
    WHEN delivery_status = 'Delivered' AND payment_status = 'Pending' AND payment_method NOT LIKE 'Net-%'
        THEN 'Completed - Payment Pending'

    -- 4) Refund logic
    WHEN payment_status = 'Refunded' AND delivery_status != 'Not Dispatched'
        THEN 'Refunded - Already Shipped'
    WHEN payment_status = 'Refunded' AND delivery_status = 'Not Dispatched'
        THEN 'Refunded - Not Shipped'

    -- 5) Failed delivery logic
    WHEN payment_status = 'Paid' AND delivery_status = 'Failed'
        THEN 'In Progress - Delivery Failed'
    WHEN delivery_status = 'Failed' AND payment_status = 'Failed'
        THEN 'Failed - Delivery and Payment'
    WHEN delivery_status = 'Failed' AND payment_status = 'Pending'
        THEN 'Failed - Delivery Issue'

    -- 6) Delayed delivery logic
    WHEN payment_status = 'Paid' AND delivery_status = 'Delayed'
        THEN 'In Progress - Delivery Delayed'
    WHEN delivery_status = 'Delayed' AND payment_status = 'Failed'
        THEN 'In Progress - Delayed and Payment Failed'
    WHEN delivery_status = 'Delayed' AND payment_status = 'Pending'
        THEN 'In Progress - Delayed and Payment Pending'

    -- 7) In Transit logic
    WHEN delivery_status = 'In Transit' AND payment_status = 'Failed'
        THEN 'In Progress - Payment Failed'
    WHEN delivery_status = 'In Transit' AND payment_status = 'Pending'
        THEN 'In Progress - Payment Pending'
    WHEN delivery_status = 'In Transit' AND payment_status = 'Paid'
        THEN 'In Progress - Payment Received'

    ELSE 'Uncategorized'
END AS business_impact
FROM fact_orders
GROUP BY 
    order_status, 
    delivery_status, 
    payment_status,
    payment_method;