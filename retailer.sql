--  average life time 
WITH lift_time as (
SELECT 
	retailer_id,
	DATEDIFF(DAY,MIN(order_date),MAX(order_date)) as range_
FROM fact_order_details f
GROUP BY retailer_id
) 
SELECT 
	avg(range_) as avg_lift_time
FROM lift_time;

-- ==============================================================================
-- who active every month in year 2022
-- ==============================================================================
WITH monthly AS (
    SELECT DISTINCT
        retailer_id,
        MONTH(order_date) AS month_num
    FROM fact_order_details
    WHERE YEAR(order_date) = 2022
)

SELECT
    COUNT(*) AS full_year_retailers
FROM (
    SELECT retailer_id
    FROM monthly
    GROUP BY retailer_id
    HAVING COUNT(DISTINCT month_num) = 12
) t; -- NUMBER
------------------------------------
WITH monthly AS (
    SELECT DISTINCT
        retailer_id,
        MONTH(order_date) AS month_num
    FROM fact_order_details
    WHERE YEAR(order_date) = 2022
),
counts AS (
    SELECT
        retailer_id,
        COUNT(DISTINCT month_num) AS active_months
    FROM monthly
    GROUP BY retailer_id
)

SELECT
    COUNT(CASE WHEN active_months = 12 THEN 1 END) * 1.0
    / COUNT(*) AS full_year_retention_rate
FROM counts; -- rate
-- ============================================================================== 

-- Retailers who didn't create any orders 
SELECT 
    r.retailer_id,r.retailer_name,r.registration_date
FROM dim_retailers r
LEFT JOIN fact_order_details o ON o.retailer_id = r.retailer_id
where o.retailer_id IS NULL;

-- Retailers who created orders but Pending
SELECT 
    r.retailer_id,r.retailer_name
FROM dim_retailers r
LEFT JOIN fact_order_details o ON o.retailer_id = r.retailer_id
where o.order_status = 'Pending';
