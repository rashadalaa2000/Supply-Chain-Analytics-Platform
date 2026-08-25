-- ==============================================================================
-- STAGING LOAD — source_* tables from canada_b2b_delivery_dataset CSVs
-- ==============================================================================
-- Run this BEFORE create.sql + load.sql (in that order):
--   1) THIS SCRIPT   → refreshes source_* staging tables from the CSVs
--   2) create.sql    → rebuilds dim_*/fact_* schema (clean drop + recreate)
--   3) load.sql      → transforms source_* into dim_*/fact_*
--
-- Update @csv_path below if you move the folder.
-- ==============================================================================

USE supply_chain;

DECLARE @csv_path NVARCHAR(500) = N'YOUR_LOCAL_PATH_HERE\';

-- ── 0. Clean rebuild — drop existing staging tables ──────────────────────────
IF OBJECT_ID('dbo.source_order_details', 'U') IS NOT NULL DROP TABLE dbo.source_order_details;
IF OBJECT_ID('dbo.source_payments',      'U') IS NOT NULL DROP TABLE dbo.source_payments;
IF OBJECT_ID('dbo.source_deliveries',    'U') IS NOT NULL DROP TABLE dbo.source_deliveries;
IF OBJECT_ID('dbo.source_orders',        'U') IS NOT NULL DROP TABLE dbo.source_orders;
IF OBJECT_ID('dbo.source_retailers',     'U') IS NOT NULL DROP TABLE dbo.source_retailers;
IF OBJECT_ID('dbo.source_suppliers',     'U') IS NOT NULL DROP TABLE dbo.source_suppliers;
IF OBJECT_ID('dbo.source_products',      'U') IS NOT NULL DROP TABLE dbo.source_products;
IF OBJECT_ID('dbo.source_areas',         'U') IS NOT NULL DROP TABLE dbo.source_areas;
IF OBJECT_ID('dbo.source_drivers',       'U') IS NOT NULL DROP TABLE dbo.source_drivers;

-- ==============================================================================
-- 1. CREATE staging tables — columns match the CSV headers 1:1
-- ==============================================================================

-- retailers.csv  
CREATE TABLE source_retailers (
    retailer_id          INT,
    retailer_name        NVARCHAR(255),
    city                 NVARCHAR(100),
    province             NVARCHAR(50),
    segment              NVARCHAR(100),
    cohort_year          SMALLINT,
    preferred_payment    NVARCHAR(100),
    registration_date    DATE,
    service_reliability  DECIMAL(5,3)
);

-- suppliers.csv
CREATE TABLE source_suppliers (
    supplier_id        INT,
    supplier_name      NVARCHAR(255),
    city               NVARCHAR(100),
    province           NVARCHAR(50),
    primary_category   NVARCHAR(100),
    category_group     NVARCHAR(300),
    spec_group_id      SMALLINT,
    established_year   SMALLINT,
    supplier_rating    DECIMAL(5,2)
);

-- products.csv
CREATE TABLE source_products (
    product_id    INT,
    product_name  NVARCHAR(255),
    category      NVARCHAR(100),
    unit_price    DECIMAL(18,2),
    sku           NVARCHAR(100)
);

-- areas.csv
CREATE TABLE source_areas (
    area_id           INT,
    city              NVARCHAR(100),
    province          NVARCHAR(50),
    neighborhood      NVARCHAR(100),
    area_name         NVARCHAR(150),
    is_cold           NVARCHAR(10),  
    city_pop_weight   DECIMAL(5,3)
);

-- drivers.csv
CREATE TABLE source_drivers (
    driver_id         INT,
    driver_name       NVARCHAR(255),
    city              NVARCHAR(100),
    province          NVARCHAR(50),
    primary_area_id   INT,
    vehicle_type      NVARCHAR(50),
    hire_year         SMALLINT,
    driver_rating     DECIMAL(5,2),
    active            NVARCHAR(10)    
);

-- orders.csv
CREATE TABLE source_orders (
    order_id          INT,
    order_date        DATETIME2,
    retailer_id       INT,
    supplier_id       INT,
    area_id           INT,
    driver_id         INT,
    retailer_segment  NVARCHAR(100),
    order_status      NVARCHAR(50),
    gmv               DECIMAL(18,2)
);

-- order_details.csv
CREATE TABLE source_order_details (
    detail_id      INT,
    order_id       INT,
    supplier_id    INT,
    product_id     INT,
    product_name   NVARCHAR(255),
    category       NVARCHAR(100),
    sku            NVARCHAR(100),
    quantity       INT,
    unit_price     DECIMAL(18,2),
    line_total     DECIMAL(18,2)
);

-- payments.csv
CREATE TABLE source_payments (
    payment_id       INT,
    order_id         INT,
    retailer_id      INT,
    payment_method   NVARCHAR(50),
    payment_date     DATETIME2,
    payment_status   NVARCHAR(50),
    amount           DECIMAL(18,2),
    currency         NVARCHAR(10)
);

-- deliveries.csv
CREATE TABLE source_deliveries (
    delivery_id          INT,
    order_id             INT,
    driver_id            INT,
    area_id              INT,
    scheduled_datetime   DATETIME2,
    actual_datetime      DATETIME2 NULL,
    extra_delay_days     INT,
    delivery_status      NVARCHAR(50),
    is_cold_city         NVARCHAR(10),   
    is_winter_month      NVARCHAR(10)    
);

-- ==============================================================================
-- 2. BULK INSERT — load each CSV (skip header row, comma-delimited)
-- ==============================================================================


DECLARE @sql NVARCHAR(MAX);

-- retailers
SET @sql = N'BULK INSERT source_retailers FROM ''' + @csv_path + N'retailers.csv''
    WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0d0a'', CODEPAGE = ''65001'', FORMAT = ''CSV'', FIELDQUOTE = ''"'', TABLOCK);';
EXEC sp_executesql @sql;

-- suppliers
SET @sql = N'BULK INSERT source_suppliers FROM ''' + @csv_path + N'suppliers.csv''
    WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0d0a'', CODEPAGE = ''65001'', FORMAT = ''CSV'', FIELDQUOTE = ''"'', TABLOCK);';
EXEC sp_executesql @sql;

-- products
SET @sql = N'BULK INSERT source_products FROM ''' + @csv_path + N'products.csv''
    WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0d0a'', CODEPAGE = ''65001'', FORMAT = ''CSV'', FIELDQUOTE = ''"'', TABLOCK);';
EXEC sp_executesql @sql;

-- areas
SET @sql = N'BULK INSERT source_areas FROM ''' + @csv_path + N'areas.csv''
    WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0d0a'', CODEPAGE = ''65001'', FORMAT = ''CSV'', FIELDQUOTE = ''"'', TABLOCK);';
EXEC sp_executesql @sql;

-- drivers
SET @sql = N'BULK INSERT source_drivers FROM ''' + @csv_path + N'drivers.csv''
    WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0d0a'', CODEPAGE = ''65001'', FORMAT = ''CSV'', FIELDQUOTE = ''"'', TABLOCK);';
EXEC sp_executesql @sql;

-- orders
SET @sql = N'BULK INSERT source_orders FROM ''' + @csv_path + N'orders.csv''
    WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0d0a'', CODEPAGE = ''65001'', FORMAT = ''CSV'', FIELDQUOTE = ''"'', TABLOCK);';
EXEC sp_executesql @sql;

-- order_details 
SET @sql = N'BULK INSERT source_order_details FROM ''' + @csv_path + N'order_details.csv''
    WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0d0a'', CODEPAGE = ''65001'', FORMAT = ''CSV'', FIELDQUOTE = ''"'', TABLOCK);';
EXEC sp_executesql @sql;

-- payments
SET @sql = N'BULK INSERT source_payments FROM ''' + @csv_path + N'payments.csv''
    WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0d0a'', CODEPAGE = ''65001'', FORMAT = ''CSV'', FIELDQUOTE = ''"'', TABLOCK);';
EXEC sp_executesql @sql;

-- deliveries
SET @sql = N'BULK INSERT source_deliveries FROM ''' + @csv_path + N'deliveries.csv''
    WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', ROWTERMINATOR = ''0x0d0a'', CODEPAGE = ''65001'', FORMAT = ''CSV'', FIELDQUOTE = ''"'', TABLOCK);';
EXEC sp_executesql @sql;

-- ==============================================================================
-- 3. QUICK ROW-COUNT CHECK — should match the generator's printed summary
-- ==============================================================================
SELECT 'source_retailers'     AS table_name, COUNT(*) AS row_count FROM source_retailers
UNION ALL SELECT 'source_suppliers',      COUNT(*) FROM source_suppliers
UNION ALL SELECT 'source_products',       COUNT(*) FROM source_products
UNION ALL SELECT 'source_areas',          COUNT(*) FROM source_areas
UNION ALL SELECT 'source_drivers',        COUNT(*) FROM source_drivers
UNION ALL SELECT 'source_orders',         COUNT(*) FROM source_orders
UNION ALL SELECT 'source_order_details',  COUNT(*) FROM source_order_details
UNION ALL SELECT 'source_payments',       COUNT(*) FROM source_payments
UNION ALL SELECT 'source_deliveries',     COUNT(*) FROM source_deliveries;
