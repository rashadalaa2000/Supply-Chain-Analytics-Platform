-- ============================================================
-- FINANCIAL HEALTH SUMMARY 
-- ============================================================

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

    SUM(CASE WHEN payment_status = 'Paid'    AND delivery_status = 'Delivered'  
        THEN line_total ELSE 0 END)  AS Realized_Revenue,
    SUM(CASE WHEN payment_status = 'Pending' AND delivery_status = 'Delivered' AND payment_method LIKE 'Net-%'    
        THEN line_total ELSE 0 END)  AS B2B_Accrued_Revenue,
    SUM(CASE WHEN payment_status = 'Pending' AND delivery_status = 'Delivered' AND payment_method NOT LIKE 'Net-%'   
        THEN line_total ELSE 0 END)  AS Uncollected_Cash_Risk,
    SUM(CASE WHEN payment_status = 'Failed'  AND delivery_status = 'Delivered'   
        THEN line_total ELSE 0 END)  AS Bad_Debt_Loss,
    SUM(CASE WHEN payment_status = 'Paid'    AND delivery_status = 'Failed'   
        THEN line_total ELSE 0 END)  AS Logistics_Waste,
    SUM(CASE WHEN payment_status = 'Paid'    AND delivery_status = 'Delayed'     
        THEN line_total ELSE 0 END)  AS Delayed_Delivery_Risk,
    SUM(CASE WHEN payment_status = 'Failed'  AND delivery_status = 'Failed'  
        THEN line_total ELSE 0 END)  AS Double_Failure_Loss,
    SUM(CASE WHEN order_status   = 'Cancelled' AND delivery_status IN ('In Transit', 'Delivered', 'Delayed')  
        THEN line_total ELSE 0 END)  AS Cancelled_But_Shipped_Loss,
    SUM(CASE WHEN order_status   = 'Cancelled' AND delivery_status = 'Not Dispatched' 
        THEN line_total ELSE 0 END)  AS Safe_Cancellation_Value,
    SUM(CASE WHEN payment_status = 'Refunde' AND delivery_status != 'Not Dispatched'   
        THEN line_total ELSE 0 END)  AS Costly_Refund_Value,
    SUM(CASE WHEN payment_status = 'Refunde' AND delivery_status  = 'Not Dispatched' 
        THEN line_total ELSE 0 END)  AS Safe_Refund_Value,
    SUM(CASE WHEN delivery_status = 'In Transit' AND payment_status = 'Paid'    
        THEN line_total ELSE 0 END)  AS In_Transit_Paid_Pipeline,
    SUM(CASE WHEN delivery_status = 'In Transit' AND payment_status = 'Pending' 
        THEN line_total ELSE 0 END)  AS In_Transit_Pending_Pipeline,

    ROUND(CAST(
        100.0 * SUM(CASE WHEN payment_status = 'Paid' AND delivery_status = 'Delivered'
            THEN line_total ELSE 0 END) /
        NULLIF(SUM(line_total), 0)
    AS FLOAT), 2)    AS Realized_Revenue_Pct,

    ROUND(CAST(
        100.0 * SUM(CASE WHEN payment_status = 'Failed' AND delivery_status = 'Delivered'
            THEN line_total ELSE 0 END) /
        NULLIF(SUM(line_total), 0)
    AS FLOAT), 2)    AS Bad_Debt_Pct,

    ROUND(CAST(
        100.0 * SUM(CASE WHEN
            (payment_status = 'Failed'   AND delivery_status = 'Delivered')  OR
            (payment_status = 'Pending'  AND delivery_status = 'Delivered' AND payment_method NOT LIKE 'Net-%') OR
            (payment_status = 'Paid'     AND delivery_status = 'Failed')  OR
            (payment_status = 'Failed'   AND delivery_status = 'Failed')  OR
            (order_status   = 'Cancelled' AND delivery_status IN ('In Transit', 'Delivered', 'Delayed'))
        THEN line_total ELSE 0 END) /
        NULLIF(SUM(line_total), 0)
    AS FLOAT), 2)  AS Total_Risk_Pct

FROM SupplyChain_Data_Check;