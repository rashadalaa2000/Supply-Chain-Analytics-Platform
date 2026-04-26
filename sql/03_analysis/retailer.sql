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
-- ==============================================================================

-- City-Wise Demand: Unique retailers count + total revenue per city 
SELECT 
	a.city,
	COUNT(DISTINCT retailer_id) as retailers_count,
	SUM(line_total) as total_revenue
FROM dim_areas a
JOIN fact_order_details o 
ON a.area_id = o.area_id
group by a.city;


-- Supplier Ratings: Find average line_total per supplier_name where rating > 4.0
SELECT 
	supplier_name,
	AVG(line_total) as avg_line_total
FROM dim_suppliers S
JOIN fact_order_details O
ON O.supplier_id = S.supplier_id
WHERE rating > 4
group by supplier_name;


-- Churn Risk Identification:Inactive 60 days + GMV > 5000 retailers
WITH retailer_stats AS (
    SELECT 
        retailer_id,
        MAX(order_date) AS last_order_date,
        SUM(line_total) AS total_gmv
    FROM fact_order_details
    group by retailer_id
)

SELECT 
    retailer_id,
    last_order_date,
    total_gmv
FROM retailer_stats
WHERE 
    DATEDIFF(DAY, last_order_date, GETDATE()) > 60
    AND total_gmv > 5000;
