CREATE OR ALTER VIEW  vw_supply_chain_health AS

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
        THEN 'Success (Revenue Realized)'
    WHEN delivery_status = 'Delivered' AND payment_status = 'Pending' AND payment_method LIKE 'Net-%'
        THEN 'Healthy Accrual (B2B Net Terms)'
    WHEN delivery_status = 'Delivered' AND payment_status = 'Failed'
        THEN 'Critical Loss (Delivered - Payment Failed)'
    WHEN delivery_status = 'Delivered' AND payment_status = 'Pending' AND payment_method NOT LIKE 'Net-%'
        THEN 'Uncollected Cash (Settlement Risk)'
    WHEN order_status = 'Cancelled' AND delivery_status = 'In Transit'
        THEN 'Logistics Disaster (Cancelled - Still Shipping)'
    WHEN order_status = 'Cancelled' AND delivery_status = 'Delivered'
        THEN 'Logistics Disaster (Cancelled - Already Delivered)'
    WHEN order_status = 'Cancelled' AND delivery_status = 'Delayed'
        THEN 'Logistics Disaster (Cancelled - Delayed Shipment)'
    WHEN order_status = 'Cancelled' AND delivery_status = 'Not Dispatched' AND payment_status = 'Refunded'
        THEN 'Safe Cancellation (Refunded - Not Shipped)'
    WHEN order_status = 'Cancelled' AND delivery_status = 'Not Dispatched' AND payment_status = 'Paid'
        THEN 'Cancellation Pending Refund (Not Shipped - Paid)'
    WHEN order_status = 'Cancelled' AND delivery_status = 'Not Dispatched' AND payment_status = 'Pending'
        THEN 'Safe Cancellation (Not Shipped - No Payment)'
    WHEN payment_status = 'Paid' AND delivery_status = 'Failed'
        THEN 'Logistics Waste (Paid - Delivery Failed)'
    WHEN payment_status = 'Paid' AND delivery_status = 'Delayed'
        THEN 'At Risk (Paid - Delivery Delayed)'
    WHEN delivery_status = 'Failed' AND payment_status = 'Failed'
        THEN 'Double Failure (Delivery Failed - Payment Failed)'
    WHEN delivery_status = 'Failed' AND payment_status = 'Pending'
        THEN 'Logistics Failure (Delivery Failed - Awaiting Payment)'
    WHEN payment_status = 'Refunded' AND delivery_status != 'Not Dispatched'
        THEN 'Costly Refund (Shipping Cost Wasted)'
    WHEN payment_status = 'Refunded' AND delivery_status = 'Not Dispatched'
        THEN 'Safe Refund (No Shipping Cost)'
    WHEN delivery_status = 'In Transit' AND payment_status = 'Failed'
        THEN 'At Risk (In Transit - Payment Failed)'
    WHEN delivery_status = 'In Transit' AND payment_status = 'Pending'
        THEN 'In Progress (In Transit - Awaiting Payment)'
    WHEN delivery_status = 'In Transit' AND payment_status = 'Paid'
        THEN 'In Progress (In Transit - Paid)'
    WHEN delivery_status = 'Delayed' AND payment_status = 'Failed'
        THEN 'At Risk (Delayed - Payment Failed)'
    WHEN delivery_status = 'Delayed' AND payment_status = 'Pending'
        THEN 'In Progress (Delayed - Awaiting Payment)'
    WHEN order_status = 'Pending' AND delivery_status = 'Delivered' AND payment_status = 'Paid'
        THEN 'Success - Order Confirmation Lagging'
    ELSE 'Uncategorized - Review Needed'
END AS business_impact

FROM SupplyChain_Data_Check
GROUP BY 
    order_status, 
    delivery_status, 
    payment_status,
    payment_method;