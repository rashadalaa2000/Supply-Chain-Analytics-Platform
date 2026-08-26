-- ==============================================================================
-- CREATE DATABASE
-- ==============================================================================
BEGIN TRY

    CREATE DATABASE supply_chain;

END TRY
BEGIN CATCH
    PRINT ERROR_MESSAGE();
END CATCH;

USE supply_chain;

-- ==============================================================================
-- 0. CLEAN REBUILD  — drop existing tables (children before parents)
-- ==============================================================================
-- Lets you re-run this script safely every time you regenerate the CSVs
-- (e.g. after editing generate_logistics_data.py), instead of manually
-- dropping the DB or hitting "table already exists" errors.

IF OBJECT_ID('dbo.fact_order_details', 'U') IS NOT NULL DROP TABLE dbo.fact_order_details;
IF OBJECT_ID('dbo.fact_orders',        'U') IS NOT NULL DROP TABLE dbo.fact_orders;
IF OBJECT_ID('dbo.dim_date',           'U') IS NOT NULL DROP TABLE dbo.dim_date;
IF OBJECT_ID('dbo.dim_retailers',      'U') IS NOT NULL DROP TABLE dbo.dim_retailers;
IF OBJECT_ID('dbo.dim_suppliers',      'U') IS NOT NULL DROP TABLE dbo.dim_suppliers;
IF OBJECT_ID('dbo.dim_products',       'U') IS NOT NULL DROP TABLE dbo.dim_products;
IF OBJECT_ID('dbo.dim_areas',          'U') IS NOT NULL DROP TABLE dbo.dim_areas;
IF OBJECT_ID('dbo.dim_drivers',        'U') IS NOT NULL DROP TABLE dbo.dim_drivers;

-- <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
-- CREATE Data Warehouse
-- <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>

-- ==============================================================================
-- 1. DIMENSION TABLES
-- ==============================================================================

-- Continuous calendar dimension capturing date attributes across order lifecycles
CREATE TABLE dim_date (
    [date] DATE PRIMARY KEY,
    date_key INT NOT NULL,
    [year] INT NOT NULL,
    [month] INT NOT NULL,
    month_name NVARCHAR(50) NOT NULL,
    [quarter] INT NOT NULL,
    [day] INT NOT NULL,
    is_weekend BIT NOT NULL,
    is_winter BIT NOT NULL,
    is_order_period BIT NOT NULL DEFAULT 0
);

-- Retailer dimension including profile data and service reliability scores
CREATE TABLE dim_retailers (
    retailer_id INT PRIMARY KEY,
    retailer_name VARCHAR(255) NOT NULL,
    segment VARCHAR(100),
    city VARCHAR(100),
    province VARCHAR(100),
    cohort_year SMALLINT,
    preferred_payment VARCHAR(100),
    registration_date DATE,
    service_reliability DECIMAL(5,3) CHECK (service_reliability BETWEEN 0 AND 1)
);

CREATE TABLE dim_suppliers (
    supplier_id INT PRIMARY KEY,
    supplier_name VARCHAR(255) NOT NULL,
    supplier_rating DECIMAL(5,2) CHECK (supplier_rating BETWEEN 0 AND 5),
    city VARCHAR(100),
    province VARCHAR(100),
    primary_category VARCHAR(100),
    category_group VARCHAR(100),
    established_year SMALLINT,
    spec_group_id SMALLINT
);

CREATE TABLE dim_products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    sku VARCHAR(100),
    unit_price DECIMAL(18,2) NOT NULL CHECK (unit_price >= 0)
);

CREATE TABLE dim_areas (
    area_id INT PRIMARY KEY,
    city VARCHAR(100),
    province VARCHAR(100),
    neighborhood VARCHAR(100),
    area_name VARCHAR(100),
    is_cold BIT NOT NULL,
    city_pop_weight DECIMAL(5,2)
);

CREATE TABLE dim_drivers (
    driver_id INT PRIMARY KEY,
    driver_name VARCHAR(255) NOT NULL,
    vehicle_type VARCHAR(50),
    driver_rating DECIMAL(5,2) CHECK (driver_rating BETWEEN 0 AND 5),
    city VARCHAR(100),
    province VARCHAR(100),
    hire_year SMALLINT,
    active BIT NOT NULL,
    primary_area_id TINYINT
);

-- ==============================================================================
-- 2. FACT TABLES
-- ==============================================================================

-- Order header fact table (Grain: 1 row per order_id)
-- Consolidates order, payment, and delivery state attributes
CREATE TABLE fact_orders (
    order_id INT PRIMARY KEY,
    retailer_id INT NOT NULL,
    area_id INT NOT NULL,
    driver_id INT NOT NULL,
    order_date DATE NOT NULL,
    order_status NVARCHAR(50) NOT NULL,

    -- === Payment attributes (was fact_payments) ===
    payment_id INT NULL,
    payment_method VARCHAR(50) NULL,
    payment_status VARCHAR(50) NULL,
    payment_amount DECIMAL(18,2) NULL CHECK (payment_amount >= 0),
    payment_date DATE NULL,

    -- === Delivery attributes (was fact_deliveries) ===
    delivery_id INT NULL,
    scheduled_date DATE NULL,
    scheduled_at DATETIME NULL,
    actual_at DATETIME NULL,
    delivery_status VARCHAR(50) NULL,
    delay_hours DECIMAL(10,2) NULL,
    extra_delay_days INT NULL,
    early_hours DECIMAL(10,2) NULL,
    early_days INT NULL,
    is_cold_city BIT NULL,
    is_winter_month BIT NULL,

    CONSTRAINT FK_fact_orders_retailer      FOREIGN KEY (retailer_id)    REFERENCES dim_retailers(retailer_id),
    CONSTRAINT FK_fact_orders_area          FOREIGN KEY (area_id)        REFERENCES dim_areas(area_id),
    CONSTRAINT FK_fact_orders_driver        FOREIGN KEY (driver_id)      REFERENCES dim_drivers(driver_id),
    CONSTRAINT FK_fact_orders_date          FOREIGN KEY (order_date)     REFERENCES dim_date([date]),
    CONSTRAINT FK_fact_orders_payment_date  FOREIGN KEY (payment_date)   REFERENCES dim_date([date]),
    CONSTRAINT FK_fact_orders_scheduled_date FOREIGN KEY (scheduled_date) REFERENCES dim_date([date])
);

-- Order line item details fact table (Grain: 1 row per line item / detail_id)
CREATE TABLE fact_order_details (
    detail_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    retailer_id INT NOT NULL,
    supplier_id INT NOT NULL,
    area_id INT NOT NULL,
    driver_id INT NOT NULL,
    order_date DATE NOT NULL,
    order_status NVARCHAR(50) NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(18,2) NOT NULL CHECK (unit_price >= 0),
    line_total DECIMAL(18,2) NOT NULL CHECK (line_total >= 0),

    CONSTRAINT FK_fact_order_details_order     FOREIGN KEY (order_id)     REFERENCES fact_orders(order_id),
    CONSTRAINT FK_fact_order_details_date      FOREIGN KEY (order_date)   REFERENCES dim_date([date]),
    CONSTRAINT FK_fact_order_details_retailer  FOREIGN KEY (retailer_id)  REFERENCES dim_retailers(retailer_id),
    CONSTRAINT FK_fact_order_details_supplier  FOREIGN KEY (supplier_id)  REFERENCES dim_suppliers(supplier_id),
    CONSTRAINT FK_fact_order_details_product   FOREIGN KEY (product_id)   REFERENCES dim_products(product_id),
    CONSTRAINT FK_fact_order_details_area      FOREIGN KEY (area_id)      REFERENCES dim_areas(area_id),
    CONSTRAINT FK_fact_order_details_driver    FOREIGN KEY (driver_id)    REFERENCES dim_drivers(driver_id)
);

