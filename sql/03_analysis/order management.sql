-- ==============================================================================
-- ORDER MANAGEMENT
-- ==============================================================================

-- Order Volume & Status Breakdown
SELECT
    fod.order_status,
    COUNT(DISTINCT fod.order_id) AS order_count,
    SUM(fod.quantity) AS total_units,
    SUM(fod.line_total) AS total_value,
    ROUND(AVG(fod.line_total), 2) AS avg_order_value,
    ROUND((CAST(COUNT(DISTINCT fod.order_id) as float) / (SELECT COUNT(DISTINCT order_id) FROM fact_order_details) * 100), 2) AS status_percent
FROM fact_order_details fod
GROUP BY fod.order_status
ORDER BY order_count DESC;

-- Total Orders Volume by Time Period
SELECT
    d.[year],
    d.[month],
    d.month_name,
    COUNT(DISTINCT fod.order_id) AS total_orders,
    ROUND(AVG(fod.quantity), 2) AS avg_units_per_order,
    SUM(fod.quantity) AS total_units
FROM fact_order_details fod
INNER JOIN dim_date d ON fod.order_date = d.[date]
GROUP BY d.[year], d.[month], d.month_name
ORDER BY d.[year], d.[month];

-- Cancellation Rate
WITH order_status_count AS (
    SELECT
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(CASE WHEN order_status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_orders
    FROM fact_order_details
)
SELECT
    total_orders,
    cancelled_orders,
    ROUND((CAST(cancelled_orders AS FLOAT) / CAST(total_orders AS FLOAT) * 100), 2) AS cancellation_rate_percent
FROM order_status_count; 

-- Peak Ordering Times 
SELECT TOP 20
    d.[date],
    d.month_name,
    d.is_weekend,
    COUNT(DISTINCT fod.order_id) AS orders_count,
    SUM(fod.line_total) AS revenue,
    ROUND(AVG(fod.line_total), 2) AS avg_order_value
FROM fact_order_details fod
INNER JOIN dim_date d ON fod.order_date = d.[date]
GROUP BY d.[date], d.month_name, d.is_weekend
ORDER BY orders_count DESC;

-- Repeat Order Rate 
WITH retailer_order_frequency AS (
    SELECT
        retailer_id,
        COUNT(DISTINCT order_id) AS order_count
    FROM fact_order_details
    GROUP BY retailer_id
)
SELECT
    SUM(CASE WHEN order_count = 1 THEN 1 ELSE 0 END) AS one_time_customers,
    SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) AS repeat_customers,
    COUNT(*) AS total_retailers,
    ROUND((CAST(SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100), 2) AS repeat_rate_percent
FROM retailer_order_frequency;

-- Average Order Value Trends
SELECT
    d.[year],
    d.[month],
    d.month_name,
    ROUND(AVG(fod.line_total), 2) AS avg_order_value,
    ROUND(STDEV(fod.line_total), 2) AS std_dev,
    MIN(fod.line_total) AS min_order_value,
    MAX(fod.line_total) AS max_order_value
FROM fact_order_details fod
INNER JOIN dim_date d ON fod.order_date = d.[date]
GROUP BY d.[year], d.[month], d.month_name
ORDER BY d.[year], d.[month]; 