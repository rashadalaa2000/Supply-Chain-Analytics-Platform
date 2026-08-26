-- ==============================================================================
-- DELIVERY & LOGISTICS
-- ==============================================================================

-- 1.On-Time Delivery Rate
WITH delivery_performance AS (
    SELECT
        delivery_id,
        order_id,
        CASE 
            WHEN delivery_status = 'Delivered' AND DATEDIFF(HOUR, scheduled_at, actual_at) <= 0 THEN 'On-Time'
            WHEN delivery_status = 'Delivered' AND DATEDIFF(HOUR, scheduled_at, actual_at) > 0 THEN 'Late'
            ELSE delivery_status
        END AS delivery_performance
    FROM fact_orders
    WHERE delivery_id IS NOT NULL
),
total AS (
    SELECT COUNT(*) AS total_count FROM fact_orders WHERE delivery_id IS NOT NULL
)
SELECT
    dp.delivery_performance,
    COUNT(*) AS delivery_count,
    ROUND(COUNT(*) * 100.0 / t.total_count, 2) AS percentage
FROM delivery_performance dp
CROSS JOIN total t
GROUP BY dp.delivery_performance, t.total_count
ORDER BY delivery_count DESC; -- 50 % Late  

-- 2.Average Delivery Time
SELECT
    ROUND(AVG(delay_hours), 2) AS avg_delivery_hours,
    ROUND(AVG(DATEDIFF(HOUR, scheduled_at, actual_at)) / 24.0, 2) AS avg_delivery_days,
    MIN(delay_hours) AS min_delivery_hours,
    MAX(delay_hours) AS max_delivery_hours,
    ROUND(STDEV(delay_hours), 2) AS std_dev_hours
FROM fact_orders
WHERE actual_at IS NOT NULL; -- avg_delivery_hours => 12  

-- 3.Delivery Success vs Failures
SELECT
    delivery_status,
    COUNT(*) AS delivery_count,
    ROUND((CAST(COUNT(*) AS FLOAT) / (SELECT COUNT(*) FROM fact_orders WHERE delivery_id IS NOT NULL) * 100), 2) AS percentage,
    ROUND(AVG(delay_hours), 2) AS avg_hours
FROM fact_orders
WHERE delivery_id IS NOT NULL
GROUP BY delivery_status
ORDER BY delivery_count DESC;

-- 4.Driver Productivity
SELECT TOP 50
    dd.driver_id,
    dd.driver_name,
    dd.vehicle_type,
    dd.city,
    dd.driver_rating,
    COUNT(DISTINCT fo.delivery_id) AS total_deliveries,
    ROUND(AVG(DATEDIFF(HOUR, fo.scheduled_at, fo.actual_at)), 2) AS avg_delivery_hours,
    SUM(CASE WHEN fo.delivery_status = 'Delivered' THEN 1 ELSE 0 END) AS successful_deliveries,
    ROUND((CAST(SUM(CASE WHEN fo.delivery_status = 'Delivered' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100), 2) AS success_rate_percent
FROM fact_orders fo
INNER JOIN dim_drivers dd ON fo.driver_id = dd.driver_id
WHERE fo.delivery_id IS NOT NULL
GROUP BY dd.driver_id, dd.driver_name, dd.vehicle_type, dd.city, dd.driver_rating
ORDER BY driver_rating DESC;

-- 5.Delivery Delays Analysis
SELECT
    d.month_name,
    d.[quarter],
    COUNT(*) AS total_deliveries,
    SUM(CASE WHEN fo.delay_hours > 0 THEN 1 ELSE 0 END) AS delayed_deliveries,
    ROUND((CAST(SUM(CASE WHEN fo.delay_hours > 0 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100), 2) AS delay_rate_percent,
    ROUND(AVG(fo.delay_hours), 2) AS avg_delay_hours,
    ROUND(AVG(fo.extra_delay_days), 2) AS avg_extra_delay_days
FROM fact_orders fo
INNER JOIN dim_date d ON fo.scheduled_date = d.[date]
GROUP BY d.month_name, d.[quarter]
ORDER BY d.[quarter];

-- 6.Cold Chain Compliance
SELECT
    is_cold_city,
    is_winter_month,
    COUNT(*) AS delivery_count,
    SUM(CASE WHEN delivery_status = 'Delivered' THEN 1 ELSE 0 END) AS successful_deliveries,
    ROUND((CAST(SUM(CASE WHEN delivery_status = 'Delivered' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100), 2) AS success_rate_percent,
    ROUND(AVG(DATEDIFF(HOUR, scheduled_at, actual_at)), 2) AS avg_delivery_hours
FROM fact_orders
WHERE delivery_id IS NOT NULL
GROUP BY is_cold_city, is_winter_month
ORDER BY delivery_count DESC;