-- ==============================================================================
-- 3. ETL
-- ==============================================================================

-- 3.1 Load dim_date (2020 to 2025) 

INSERT INTO dim_date ([date], date_key, [year], [month], month_name, [quarter], [day], is_weekend, is_winter)
SELECT DISTINCT
    dt,
    CAST(CONVERT(VARCHAR(8), dt, 112) AS INT),
    YEAR(dt),
    MONTH(dt),
    DATENAME(MONTH, dt),
    DATEPART(QUARTER, dt),
    DAY(dt),
    CASE WHEN DATENAME(WEEKDAY, dt) IN ('Saturday','Sunday') THEN 1 ELSE 0 END,
    CASE WHEN MONTH(dt) IN (12,1,2) THEN 1 ELSE 0 END
FROM (
    SELECT payment_date AS dt FROM supply_chain.dbo.source_payments WHERE payment_date IS NOT NULL
    UNION
    SELECT CAST(scheduled_datetime AS DATE) FROM supply_chain.dbo.source_deliveries WHERE scheduled_datetime IS NOT NULL
    UNION
    SELECT CAST(actual_datetime AS DATE) FROM supply_chain.dbo.source_deliveries WHERE actual_datetime IS NOT NULL
) AS missing_dates
WHERE dt NOT IN (SELECT [date] FROM dim_date);

-- 3.2 Load Dimensions
INSERT INTO dim_areas (area_id, city, province, neighborhood, area_name, is_cold)
SELECT 
    area_id, city, province, neighborhood, area_name,
    CASE WHEN city IN ('Edmonton','Winnipeg','Saskatoon','Regina','Quebec City','Montreal','Calgary') THEN 1 ELSE 0 END
FROM supply_chain.dbo.source_areas;

INSERT INTO dim_retailers (retailer_id, retailer_name, segment, city, province, cohort_year, preferred_payment, registration_date)
SELECT retailer_id, retailer_name, segment, city, province, cohort_year, preferred_payment, registration_date
FROM supply_chain.dbo.source_retailers;

INSERT INTO dim_suppliers (supplier_id, supplier_name, rating, city, province, primary_category, category_group, established_year)
SELECT supplier_id, supplier_name, supplier_rating, city, province, primary_category, category_group, established_year
FROM supply_chain.dbo.source_suppliers;

INSERT INTO dim_products (product_id, product_name, category, sku, unit_price)
SELECT product_id, product_name, category, sku, CAST(unit_price AS DECIMAL(18,2))
FROM supply_chain.dbo.source_products;

INSERT INTO dim_drivers (driver_id, driver_name, vehicle_type, rating, city, province, hire_year, active)
SELECT driver_id, driver_name, vehicle_type, driver_rating, city, province, hire_year, active
FROM supply_chain.dbo.source_drivers;

-- 3.3 Load fact_order_details
INSERT INTO fact_order_details (
    detail_id, order_id, product_id, retailer_id, supplier_id, area_id, driver_id,
    order_date, order_status, quantity, unit_price, line_total, gmv
)
SELECT 
    od.detail_id,
    o.order_id,
    od.product_id,
    o.retailer_id,
    od.supplier_id,
    o.area_id,
    o.driver_id,
    CAST(o.order_date AS DATE),
    o.order_status,
    CAST(od.quantity AS INT),
    CAST(od.unit_price AS DECIMAL(18,2)),
    CAST(od.quantity * od.unit_price AS DECIMAL(18,2)),
    CAST(o.gmv AS DECIMAL(18,2))
FROM supply_chain.dbo.source_orders o
JOIN supply_chain.dbo.source_order_details od ON o.order_id = od.order_id;

-- 3.4 Load fact_payments
INSERT INTO fact_payments (
    payment_id, order_id, retailer_id, payment_method, payment_status, amount, payment_date, currency
)
SELECT 
    p.payment_id,
    p.order_id,
    o.retailer_id,
    p.payment_method,
    p.payment_status,
    CAST(p.amount AS DECIMAL(18,2)),
    p.payment_date,
    p.currency
FROM supply_chain.dbo.source_payments p
JOIN supply_chain.dbo.source_orders o ON p.order_id = o.order_id;

-- 3.5 Load fact_deliveries
INSERT INTO fact_deliveries (
    delivery_id, order_id, driver_id, area_id,
    scheduled_date, scheduled_at, actual_at,
    delivery_status, delay_hours, delay_days
)
SELECT 
    d.delivery_id,
    d.order_id,
    d.driver_id,
    o.area_id,
    CAST(d.scheduled_datetime AS DATE),
    CAST(d.scheduled_datetime AS DATETIME),
    CAST(d.actual_datetime AS DATETIME),
    d.delivery_status,
    CAST(DATEDIFF(MINUTE, d.scheduled_datetime, d.actual_datetime) AS DECIMAL(10,2)) / 60.0,
    CAST(DATEDIFF(MINUTE, d.scheduled_datetime, d.actual_datetime) AS DECIMAL(10,2)) / 1440.0
FROM supply_chain.dbo.source_deliveries d
JOIN supply_chain.dbo.source_orders o ON d.order_id = o.order_id;
