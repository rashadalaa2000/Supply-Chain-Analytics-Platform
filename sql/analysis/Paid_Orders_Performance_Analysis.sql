IF OBJECT_ID('tempdb..#PaidOrders') IS NOT NULL DROP TABLE #PaidOrders;
SELECT DISTINCT order_id
INTO #PaidOrders
FROM fact_orders
WHERE payment_status = 'Paid';

IF OBJECT_ID('tempdb..#Base') IS NOT NULL DROP TABLE #Base;
SELECT 
    fod.order_id,
    fod.retailer_id,
    fod.area_id,
    fod.supplier_id,
    fod.line_total,
    fod.quantity
INTO #Base
FROM fact_order_details fod
INNER JOIN #PaidOrders po ON fod.order_id = po.order_id;

DECLARE @TotalRev DECIMAL(18,2);
SELECT @TotalRev = SUM(line_total) FROM #Base;

---

-- (Retailers)
SELECT TOP 50
    dr.retailer_id,
    dr.retailer_name,
    dr.city,
    dr.province,
    dr.segment,
    COUNT(DISTINCT b.order_id) AS total_orders,
    SUM(b.line_total) AS retailer_revenue,
    ROUND(SUM(b.line_total) / NULLIF(COUNT(DISTINCT b.order_id), 0), 2) AS avg_order_value,
    ROUND(SUM(b.line_total) * 100.0 / NULLIF(@TotalRev, 0), 2) AS revenue_share_percent,
    SUM(b.quantity) AS total_units
FROM #Base b
JOIN dim_retailers dr ON b.retailer_id = dr.retailer_id
GROUP BY dr.retailer_id, dr.retailer_name, dr.city, dr.province, dr.segment
ORDER BY retailer_revenue DESC;

-- (Areas)
SELECT TOP 50
    da.area_id,
    da.area_name,
    da.city,
    da.province,
    COUNT(DISTINCT b.order_id) AS total_orders,
    SUM(b.line_total) AS area_revenue,
    ROUND(SUM(b.line_total) / NULLIF(COUNT(DISTINCT b.order_id), 0), 2) AS avg_order_value,
    ROUND(SUM(b.line_total) * 100.0 / NULLIF(@TotalRev, 0), 2) AS revenue_share_percent
FROM #Base b
JOIN dim_areas da ON b.area_id = da.area_id
GROUP BY da.area_id, da.area_name, da.city, da.province
ORDER BY area_revenue DESC;

-- (Suppliers)
SELECT TOP 50
    ds.supplier_id,
    ds.supplier_name,
    ds.city,
    ds.primary_category,
    ds.supplier_rating,
    COUNT(DISTINCT b.order_id) AS total_orders,
    SUM(b.line_total) AS supplier_revenue,
    ROUND(SUM(b.line_total) / NULLIF(COUNT(DISTINCT b.order_id), 0), 2) AS avg_order_value,
    ROUND(SUM(b.line_total) * 100.0 / NULLIF(@TotalRev, 0), 2) AS revenue_share_percent
FROM #Base b
JOIN dim_suppliers ds ON b.supplier_id = ds.supplier_id
GROUP BY ds.supplier_id, ds.supplier_name, ds.city, ds.primary_category, ds.supplier_rating
ORDER BY supplier_revenue DESC;

DROP TABLE #PaidOrders;
DROP TABLE #Base;