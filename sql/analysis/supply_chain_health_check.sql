-- ==========================================
-- Supply Chain Health Check
-- ==========================================

WITH SupplyChain_Data_Check AS (
    SELECT 
        f.order_id,
        f.order_status,
        o.delivery_status,
        o.payment_status,
        f.line_total,
        f.area_id,
        o.payment_method
    FROM fact_order_details f
    LEFT JOIN fact_orders o ON f.order_id = o.order_id
)
SELECT 
    order_status,
    delivery_status,
    payment_status,
    payment_method,
    COUNT(DISTINCT order_id) AS orders_count,
    SUM(line_total) AS total_value, 
    
CASE 

    -- Success
    WHEN delivery_status = 'Delivered' AND payment_status = 'Paid'
        THEN 'Success (Revenue Realized)'

    -- Net Terms
    WHEN delivery_status = 'Delivered' AND payment_status = 'Pending' AND payment_method LIKE 'Net-%'
        THEN 'Healthy Accrual (B2B Net Terms)'

    -- Critical Loss
    WHEN delivery_status = 'Delivered' AND payment_status = 'Failed'
        THEN 'Critical Loss (Delivered - Payment Failed)'

    -- Net Terms
    WHEN delivery_status = 'Delivered' AND payment_status = 'Pending' AND payment_method NOT LIKE 'Net-%'
        THEN 'Uncollected Cash (Settlement Risk)'

    -- Logistics Disaster
    WHEN order_status = 'Cancelled' AND delivery_status = 'In Transit'
        THEN 'Logistics Disaster (Cancelled - Still Shipping)'

    WHEN order_status = 'Cancelled' AND delivery_status = 'Delivered'
        THEN 'Logistics Disaster (Cancelled - Already Delivered)'

    WHEN order_status = 'Cancelled' AND delivery_status = 'Delayed'
        THEN 'Logistics Disaster (Cancelled - Delayed Shipment)'

    -- Safe Cancellation
    WHEN order_status = 'Cancelled' AND delivery_status = 'Not Dispatched' AND payment_status = 'Refunded'
        THEN 'Safe Cancellation (Refunded - Not Shipped)'

    -- Cancellation Pending Refund
    WHEN order_status = 'Cancelled' AND delivery_status = 'Not Dispatched' AND payment_status = 'Paid'
        THEN 'Cancellation Pending Refund (Not Shipped - Paid)'

    -- Safe Cancellation
    WHEN order_status = 'Cancelled' AND delivery_status = 'Not Dispatched' AND payment_status = 'Pending'
        THEN 'Safe Cancellation (Not Shipped - No Payment)'

    -- Logistics Waste
    WHEN payment_status = 'Paid' AND delivery_status = 'Failed'
        THEN 'Logistics Waste (Paid - Delivery Failed)'

    -- At Risk
    WHEN payment_status = 'Paid' AND delivery_status = 'Delayed'
        THEN 'At Risk (Paid - Delivery Delayed)'

    -- Double Failure
    WHEN delivery_status = 'Failed' AND payment_status = 'Failed'
        THEN 'Double Failure (Delivery Failed - Payment Failed)'

    -- Logistics Failure
    WHEN delivery_status = 'Failed' AND payment_status = 'Pending'
        THEN 'Logistics Failure (Delivery Failed - Awaiting Payment)'

    -- Costly Refund
    WHEN payment_status = 'Refunded' AND delivery_status != 'Not Dispatched'
        THEN 'Costly Refund (Shipping Cost Wasted)'

    -- Safe Refund
    WHEN payment_status = 'Refunded' AND delivery_status = 'Not Dispatched'
        THEN 'Safe Refund (No Shipping Cost)'

    -- At Risk 
    WHEN delivery_status = 'In Transit' AND payment_status = 'Failed'
        THEN 'At Risk (In Transit - Payment Failed)'

    -- In Progress
    WHEN delivery_status = 'In Transit' AND payment_status = 'Pending'
        THEN 'In Progress (In Transit - Awaiting Payment)'

    WHEN delivery_status = 'In Transit' AND payment_status = 'Paid'
        THEN 'In Progress (In Transit - Paid)'

    -- At Risk
    WHEN delivery_status = 'Delayed' AND payment_status = 'Failed'
        THEN 'At Risk (Delayed - Payment Failed)'

    -- In Progress 
    WHEN delivery_status = 'Delayed' AND payment_status = 'Pending'
        THEN 'In Progress (Delayed - Awaiting Payment)'

    -- Success
    WHEN order_status = 'Pending' AND delivery_status = 'Delivered' AND payment_status = 'Paid'
        THEN 'Success - Order Confirmation Lagging'

    ELSE 'Uncategorized - Review Needed'

END AS business_impact

FROM SupplyChain_Data_Check
GROUP BY 
    order_status, 
    delivery_status, 
    payment_status,
    payment_method
ORDER BY
    order_status, 
    delivery_status,
    payment_status,
    orders_count DESC;