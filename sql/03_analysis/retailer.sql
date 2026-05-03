-- ==============================================================================
-- RETAILER BEHAVIOR & SEGMENTATION
-- ==============================================================================

-- 1.average life time 
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


-- 2.Retention Rate 
WITH monthly AS (
    SELECT DISTINCT
        retailer_id,
        DATEFROMPARTS(YEAR(order_date), MONTH(order_date), 1) AS month_start
    FROM fact_order_details
),
-- total retailers per month
monthly_counts AS (
    SELECT
        month_start AS month_start,
        COUNT(DISTINCT retailer_id) AS total_retailers
    FROM monthly
    GROUP BY month_start
),
-- retained retailers 
retained AS (
    SELECT
        curr.month_start,
        COUNT(DISTINCT curr.retailer_id) AS retained_retailers
    FROM monthly curr
    JOIN monthly prev
        ON curr.retailer_id = prev.retailer_id
        AND curr.month_start = DATEADD(MONTH, 1, prev.month_start)
    GROUP BY curr.month_start
)
SELECT
    mc.month_start,
    mc.total_retailers AS current_month_retailers,
    r.retained_retailers,
    LAG(mc.total_retailers) OVER (ORDER BY mc.month_start) AS previous_month_retailers,
    CAST(r.retained_retailers AS FLOAT) /
    NULLIF(LAG(mc.total_retailers) OVER (ORDER BY mc.month_start), 0) AS retention_rate
FROM monthly_counts mc
LEFT JOIN retained r
    ON mc.month_start = r.month_start
ORDER BY mc.month_start;


-- 3.Retailer Segment Analysis
SELECT
    dr.segment,
    COUNT(DISTINCT dr.retailer_id) AS retailer_count,
    COUNT(DISTINCT fod.order_id) AS total_orders,
    SUM(fod.line_total) AS segment_revenue,
    ROUND(AVG(fod.line_total), 2) AS avg_order_value,
    ROUND((SUM(fod.line_total) / (SELECT SUM(line_total) FROM fact_order_details) * 100), 2) AS revenue_share_percent
FROM fact_order_details fod
INNER JOIN dim_retailers dr ON fod.retailer_id = dr.retailer_id
GROUP BY dr.segment
ORDER BY segment_revenue DESC;

-- 4.New vs Returning Retailers
WITH retailer_cohorts AS (
    SELECT
        dr.retailer_id,
        YEAR(registration_date) AS cohort_year,
        MIN(YEAR(order_date)) AS first_order_year
    FROM dim_retailers dr
    LEFT JOIN fact_order_details fod ON dr.retailer_id = fod.retailer_id
    GROUP BY dr.retailer_id, YEAR(registration_date)
)
SELECT
    cohort_year,
    COUNT(*) AS registered_retailers,
    SUM(CASE WHEN first_order_year IS NOT NULL THEN 1 ELSE 0 END) AS retailers_with_orders,
    ROUND((CAST(SUM(CASE WHEN first_order_year IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100), 2) AS conversion_rate_percent
FROM retailer_cohorts
GROUP BY cohort_year
ORDER BY cohort_year;

-- 5.RFM Segmentation (Recency, Frequency, Monetary)
WITH retailer_rfm AS (
    SELECT
        dr.retailer_id,
        dr.retailer_name,
        dr.segment,
        DATEDIFF(DAY, MAX(fod.order_date), GETDATE()) AS recency_days,
        COUNT(DISTINCT fod.order_id) AS frequency_orders,
        SUM(fod.line_total) AS monetary_value,
        NTILE(4) OVER (ORDER BY DATEDIFF(DAY, MAX(fod.order_date), GETDATE()) DESC) AS recency_quartile,
        NTILE(4) OVER (ORDER BY COUNT(DISTINCT fod.order_id)) AS frequency_quartile,
        NTILE(4) OVER (ORDER BY SUM(fod.line_total)) AS monetary_quartile
    FROM fact_order_details fod
    INNER JOIN dim_retailers dr ON fod.retailer_id = dr.retailer_id
    GROUP BY dr.retailer_id, dr.retailer_name, dr.segment
)
SELECT
    retailer_id,
    retailer_name,
    segment,
    recency_days,
    frequency_orders,
    ROUND(monetary_value, 2) AS monetary_value,
    CASE 
        WHEN recency_quartile = 1 AND frequency_quartile = 1 AND monetary_quartile = 1 THEN 'VIP'
        WHEN recency_quartile IN (1, 2) AND frequency_quartile IN (1, 2) THEN 'High Value'
        WHEN recency_quartile <= 3 THEN 'At Risk'
        ELSE 'Low Value'
    END AS customer_segment
FROM retailer_rfm
ORDER BY monetary_value DESC;

-- 6.Retailer Churn Analysis (No orders in last year)
WITH retailer_activity AS (
    SELECT
        dr.retailer_id,
        dr.retailer_name,
        dr.segment,
        MAX(fod.order_date) AS last_order_date,
        DATEDIFF(DAY, MAX(fod.order_date), GETDATE()) AS days_since_last_order
    FROM dim_retailers dr
    LEFT JOIN fact_order_details fod ON dr.retailer_id = fod.retailer_id
    GROUP BY dr.retailer_id, dr.retailer_name, dr.segment
)
SELECT
    SUM(CASE WHEN days_since_last_order > 365 THEN 1 ELSE 0 END) AS churned_retailers,
    COUNT(*) AS total_retailers,
    ROUND((CAST(SUM(CASE WHEN days_since_last_order > 365 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100), 2) AS churn_rate_percent
FROM retailer_activity;

-- 7.Growth rate
WITH base AS (
    SELECT 
        YEAR(order_date) AS year,
        SUM(line_total) AS total_gmv,
        AVG(line_total) AS avg_gmv
    FROM fact_order_details
    GROUP BY YEAR(order_date)
)

SELECT 
    *,
    (total_gmv - LAG(total_gmv) OVER (ORDER BY year)) * 1.0 /
    NULLIF(LAG(total_gmv) OVER (ORDER BY year), 0) AS growth_rate
FROM base;

-- 8.Retailers who didn't create any orders 
SELECT 
    r.retailer_id,r.retailer_name,r.registration_date
FROM dim_retailers r
LEFT JOIN fact_order_details o ON o.retailer_id = r.retailer_id
where o.retailer_id IS NULL;
