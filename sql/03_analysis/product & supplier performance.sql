-- ==============================================================================
-- PRODUCT & SUPPLIER PERFORMANCE
-- ==============================================================================

-- 1.Market basket analysis
WITH ProductPairs AS (
    SELECT 
        A.product_id AS P1, 
        B.product_id AS P2, 
        COUNT(*) AS Frequency
    FROM fact_order_details A
    JOIN fact_order_details B ON A.order_id = B.order_id AND A.product_id < B.product_id
    GROUP BY A.product_id, B.product_id
)
SELECT 
    PR1.product_name AS Item_1, 
    PR2.product_name AS Item_2, 
    Frequency
FROM ProductPairs
JOIN dim_products PR1 ON PR1.product_id = P1
JOIN dim_products PR2 ON PR2.product_id = P2
WHERE Frequency > 20 
ORDER BY Frequency DESC;


-- 2.Top Products by Volume & Revenue
SELECT TOP 50
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.sku,
    ROUND(dp.unit_price, 2) AS unit_price,
    SUM(fod.quantity) AS total_units_sold,
    COUNT(DISTINCT fod.order_id) AS order_count,
    SUM(fod.line_total) AS total_revenue,
    ROUND(AVG(fod.line_total), 2) AS avg_order_value,
    ROUND((SUM(fod.line_total) / (SELECT SUM(line_total) FROM fact_order_details) * 100), 2) AS revenue_share_percent
FROM fact_order_details fod
INNER JOIN dim_products dp ON fod.product_id = dp.product_id
GROUP BY dp.product_id, dp.product_name, dp.category, dp.sku, dp.unit_price
ORDER BY total_revenue DESC;


-- 3.Supplier Performance
SELECT TOP 50
    ds.supplier_id,
    ds.supplier_name,
    ds.city,
    ds.primary_category,
    ds.supplier_rating,
    ds.established_year,
    COUNT(DISTINCT fod.order_id) AS total_orders,
    COUNT(DISTINCT fod.product_id) AS product_count,
    SUM(fod.line_total) AS supplier_revenue,
    ROUND(AVG(fod.line_total), 2) AS avg_order_value,
    SUM(fod.quantity) AS total_units,
    ROUND((SUM(fod.line_total) / (SELECT SUM(line_total) FROM fact_order_details) * 100), 2) AS revenue_share_percent
FROM fact_order_details fod
INNER JOIN dim_suppliers ds ON fod.supplier_id = ds.supplier_id
GROUP BY ds.supplier_id, ds.supplier_name, ds.city, ds.primary_category, ds.supplier_rating, ds.established_year
ORDER BY supplier_revenue DESC;

-- 4.Supplier Fill Rate (Successful orders vs total orders)
SELECT
    ds.supplier_id,
    ds.supplier_name,
    COUNT(*) AS total_line_items,
    SUM(CASE WHEN fod.order_status = 'Completed' THEN 1 ELSE 0 END) AS completed_items,
    ROUND((CAST(SUM(CASE WHEN fod.order_status = 'Completed' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100), 2) AS fill_rate_percent,
    SUM(fod.line_total) AS revenue_from_completed
FROM fact_order_details fod
INNER JOIN dim_suppliers ds ON fod.supplier_id = ds.supplier_id
GROUP BY ds.supplier_id, ds.supplier_name
ORDER BY fill_rate_percent DESC;

-- 5.Product Category Performance
SELECT
    dp.category,
    COUNT(DISTINCT fod.order_id) AS total_orders,
    COUNT(DISTINCT dp.product_id) AS product_count,
    SUM(fod.quantity) AS total_units,
    SUM(fod.line_total) AS category_revenue,
    ROUND(AVG(fod.line_total), 2) AS avg_order_value,
    ROUND((SUM(fod.line_total) / (SELECT SUM(line_total) FROM fact_order_details) * 100), 2) AS revenue_share_percent
FROM fact_order_details fod
INNER JOIN dim_products dp ON fod.product_id = dp.product_id
GROUP BY dp.category
ORDER BY category_revenue DESC;
