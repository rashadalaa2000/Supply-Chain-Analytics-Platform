USE supply_chain;

-- ==============================================================================
-- 3. ETL
-- ==============================================================================

-- 3.1 Load dim_date as a FULL continuous calendar (no gaps).
-- Range = MIN/MAX across ALL date columns used anywhere in the model:
-- order_date, payment_date, scheduled_datetime, actual_datetime.
-- This guarantees every day in the business timeline exists in dim_date,
-- which Power BI time-intelligence (MTD/YTD/rolling) requires.

DECLARE @start_date DATE, @end_date DATE;

SELECT
    @start_date = MIN(dt),
    @end_date   = MAX(dt)
FROM (
    SELECT order_date AS dt FROM dbo.source_orders WHERE order_date IS NOT NULL
    UNION ALL
    SELECT payment_date FROM dbo.source_payments WHERE payment_date IS NOT NULL
    UNION ALL
    SELECT CAST(scheduled_datetime AS DATE) FROM dbo.source_deliveries WHERE scheduled_datetime IS NOT NULL
    UNION ALL
    SELECT CAST(actual_datetime AS DATE) FROM dbo.source_deliveries WHERE actual_datetime IS NOT NULL
) AS all_dates;


IF @end_date IS NOT NULL
    SET @end_date = DATEFROMPARTS(YEAR(@end_date), 12, 31);


DECLARE @last_order_date DATE;
SELECT @last_order_date = MAX(order_date) FROM dbo.source_orders;

;WITH calendar AS (
    SELECT @start_date AS dt
    UNION ALL
    SELECT DATEADD(DAY, 1, dt)
    FROM calendar
    WHERE dt < @end_date
)
INSERT INTO dim_date ([date], date_key, [year], [month], month_name, [quarter], [day], is_weekend, is_winter, is_order_period)
SELECT
    dt,
    CAST(CONVERT(VARCHAR(8), dt, 112) AS INT),
    YEAR(dt),
    MONTH(dt),
    DATENAME(MONTH, dt),
    DATEPART(QUARTER, dt),
    DAY(dt),
    CASE WHEN DATENAME(WEEKDAY, dt) IN ('Saturday','Sunday') THEN 1 ELSE 0 END,
    CASE WHEN MONTH(dt) IN (12,1,2) THEN 1 ELSE 0 END,
    CASE WHEN dt <= @last_order_date THEN 1 ELSE 0 END
FROM calendar
OPTION (MAXRECURSION 0);

-- 3.2 Load Dimensions

INSERT INTO dim_areas (area_id, city, province, neighborhood, area_name, is_cold, city_pop_weight)
SELECT
    area_id, city, province, neighborhood, area_name,
CASE WHEN city IN (
    'Toronto','Montreal','Calgary','Ottawa','Edmonton','Winnipeg',
    'Quebec City','Hamilton','Kitchener','London','Halifax',
    'Oshawa','Windsor','Saskatoon','Regina','St. John''s'
) THEN 1 ELSE 0 END ,
city_pop_weight
FROM dbo.source_areas;


INSERT INTO dim_retailers (retailer_id, retailer_name, segment, city, province, cohort_year, preferred_payment, registration_date, service_reliability)
SELECT retailer_id, retailer_name, segment, city, province, cohort_year, preferred_payment, registration_date, service_reliability
FROM dbo.source_retailers;

INSERT INTO dim_suppliers (supplier_id, supplier_name, supplier_rating, city, province, primary_category, category_group, established_year, spec_group_id)
SELECT supplier_id, supplier_name, supplier_rating, city, province, primary_category, category_group, established_year, spec_group_id
FROM dbo.source_suppliers;

INSERT INTO dim_products (product_id, product_name, category, sku, unit_price)
SELECT product_id, product_name, category, sku, CAST(unit_price AS DECIMAL(18,2))
FROM dbo.source_products;

INSERT INTO dim_drivers (driver_id, driver_name, vehicle_type, driver_rating, city, province, hire_year, active, primary_area_id)
SELECT driver_id, driver_name, vehicle_type, driver_rating, city, province, hire_year, active, primary_area_id
FROM dbo.source_drivers;

-- 3.3 Load fact_orders — order header + payment + delivery, merged.
-- Grain is still 1 row per order_id (verified 1:1 assumption, see notes
-- in create.sql). LEFT JOINs because an order can exist before it's
-- paid or delivered, so payment/delivery columns can be NULL.

INSERT INTO fact_orders (
    order_id, retailer_id, area_id, driver_id, order_date, order_status,
    payment_id, payment_method, payment_status, payment_amount, payment_date,
    delivery_id, scheduled_date, scheduled_at, actual_at, delivery_status,
    delay_hours, extra_delay_days, early_hours, early_days,
    is_cold_city, is_winter_month
)
SELECT
    o.order_id,
    o.retailer_id,
    o.area_id,
    o.driver_id,
    CAST(o.order_date AS DATE),
    o.order_status,

    -- payment (was fact_payments)
    p.payment_id,
    p.payment_method,
    p.payment_status,
    CAST(p.amount AS DECIMAL(18,2)),
    p.payment_date,

    -- delivery (was fact_deliveries)
    d.delivery_id,
    CAST(d.scheduled_datetime AS DATE),
    CAST(d.scheduled_datetime AS DATETIME),
    CAST(d.actual_datetime    AS DATETIME),
    d.delivery_status,

    CASE
        WHEN d.actual_datetime IS NULL THEN NULL
        WHEN DATEDIFF(MINUTE, d.scheduled_datetime, d.actual_datetime) > 0
            THEN CAST(DATEDIFF(MINUTE, d.scheduled_datetime, d.actual_datetime) AS DECIMAL(10,2)) / 60.0
        ELSE NULL
    END AS delay_hours,

    CASE
        WHEN d.actual_datetime IS NULL THEN NULL
        WHEN DATEDIFF(DAY, CAST(d.scheduled_datetime AS DATE), CAST(d.actual_datetime AS DATE)) > 0
            THEN DATEDIFF(DAY, CAST(d.scheduled_datetime AS DATE), CAST(d.actual_datetime AS DATE))
        ELSE NULL
    END AS extra_delay_days,

    CASE
        WHEN d.actual_datetime IS NULL THEN NULL
        WHEN DATEDIFF(MINUTE, d.scheduled_datetime, d.actual_datetime) < 0
            THEN CAST(ABS(DATEDIFF(MINUTE, d.scheduled_datetime, d.actual_datetime)) AS DECIMAL(10,2)) / 60.0
        ELSE NULL
    END AS early_hours,

    CASE
        WHEN d.actual_datetime IS NULL THEN NULL
        WHEN DATEDIFF(DAY, CAST(d.scheduled_datetime AS DATE), CAST(d.actual_datetime AS DATE)) < 0
            THEN ABS(DATEDIFF(DAY, CAST(d.scheduled_datetime AS DATE), CAST(d.actual_datetime AS DATE)))
        ELSE NULL
    END AS early_days,

    CASE WHEN d.is_cold_city = 'True' THEN 1 ELSE 0 END,
    d.is_winter_month

FROM dbo.source_orders o
LEFT JOIN dbo.source_payments p   ON o.order_id = p.order_id
LEFT JOIN dbo.source_deliveries d ON o.order_id = d.order_id;

-- 3.4 Load fact_order_details
INSERT INTO fact_order_details (
    detail_id, order_id, product_id, retailer_id, supplier_id, area_id, driver_id,
    order_date, order_status, quantity, unit_price, line_total
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
    CAST(od.quantity * od.unit_price AS DECIMAL(18,2))
FROM dbo.source_orders o
JOIN dbo.source_order_details od ON o.order_id = od.order_id;

-- fact_payments and fact_deliveries inserts removed — folded into the
-- single fact_orders insert above (step 3.3).

-- ==============================================================================
-- 4. DATA FIXES
-- ==============================================================================

-- Defensive Cleansing: Fixes misspelled payment status values to maintain data consistency.

UPDATE fact_orders
SET payment_status = 'Refunded'
WHERE payment_status = 'Refunde';
