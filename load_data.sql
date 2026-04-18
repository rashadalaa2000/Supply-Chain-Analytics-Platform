-- ==============================================================
-- 3. ETL INSERT LOGIC
-- ==============================================================

-- 3.1 Load dim_date (2020 to 2025)
WITH dates AS (
    SELECT CAST('2020-01-01' AS DATE) AS dt
    UNION ALL
    SELECT DATEADD(DAY, 1, dt)
    FROM dates
    WHERE dt < '2025-12-31'
)
INSERT INTO dim_date ([date], date_key, [year], [month], month_name, [quarter], [day], is_weekend, is_winter)
SELECT 
    dt AS [date],
    CAST(CONVERT(VARCHAR(8), dt, 112) AS INT) AS date_key,
    YEAR(dt),
    MONTH(dt),
    DATENAME(MONTH, dt),
    DATEPART(QUARTER, dt),
    DAY(dt),
    CASE WHEN DATENAME(WEEKDAY, dt) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END,
    CASE WHEN MONTH(dt) IN (12,1,2,3) THEN 1 ELSE 0 END
FROM dates
OPTION (MAXRECURSION 10000);

-- 3.2 Load Dimensions
INSERT INTO dim_areas (area_id, city, province, is_cold)
SELECT 
    area_id, 
    city, 
    province, 
    CASE WHEN city IN ('Edmonton', 'Winnipeg', 'Saskatoon', 'Regina', 'Quebec City', 'Montreal', 'Calgary') THEN 1 ELSE 0 END 
FROM source_areas;

INSERT INTO dim_retailers (retailer_id, retailer_name, segment, city)
SELECT retailer_id, retailer_name, segment, city FROM source_retailers;

INSERT INTO dim_suppliers (supplier_id, supplier_name, rating)
SELECT supplier_id, supplier_name, supplier_rating FROM source_suppliers;

INSERT INTO dim_products (product_id, product_name, category, sku)
SELECT product_id, product_name, category, sku FROM source_products;

INSERT INTO dim_drivers (driver_id, driver_name, vehicle_type, rating)
SELECT driver_id, driver_name, vehicle_type, driver_rating FROM source_drivers;

-- 3.3 Load FACT_ORDER_DETAILS
INSERT INTO fact_order_details (
    detail_id, order_id, product_id, retailer_id, supplier_id, area_id, driver_id, 
    order_date, quantity, unit_price, line_total
)
SELECT 
    od.detail_id,
    o.order_id,
    od.product_id,
    o.retailer_id,
    od.supplier_id,
    o.area_id,
    o.driver_id,
    CAST(o.order_date AS DATE) AS order_date,
    CAST(od.quantity AS INT),
    CAST(od.unit_price AS DECIMAL(18,2)),
    CAST((od.quantity * od.unit_price) AS DECIMAL(18,2)) AS line_total
FROM source_orders o
JOIN source_order_details od ON o.order_id = od.order_id
JOIN source_products p ON od.product_id = p.product_id;

-- 3.4 Load FACT_PAYMENTS
INSERT INTO fact_payments (
    payment_id, order_id, retailer_id, payment_method, payment_status, amount
)
SELECT 
    p.payment_id,
    p.order_id,
    o.retailer_id,
    p.payment_method,
    p.payment_status,
    CAST(p.amount AS DECIMAL(18,2))
FROM source_payments p
JOIN source_orders o ON p.order_id = o.order_id;

-- 3.5 Load FACT_DELIVERIES
INSERT INTO fact_deliveries (
    delivery_id, order_id, driver_id, area_id, scheduled_at, actual_at, delay_minutes
)
SELECT 
    d.delivery_id,
    d.order_id,
    d.driver_id,
    o.area_id,
    CAST(d.scheduled_datetime AS DATETIME),
    CAST(d.actual_datetime AS DATETIME),
    DATEDIFF(MINUTE, d.scheduled_datetime, d.actual_datetime) AS delay_minutes
FROM source_deliveries d
JOIN source_orders o ON d.order_id = o.order_id;

-- ===============================
-- UPDATE Date
-- ===============================

UPDATE dim_date
SET is_winter =
CASE 
    WHEN month_name IN ('December', 'January', 'February') THEN 1
    ELSE 0
END
WHERE month_name IS NOT NULL;
