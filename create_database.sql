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

-- Get data 
-- Tasks => improt flat file

-- <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
-- CREATE Data Warehouse
-- <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>

-- ==============================================================================
-- 1. DIMENSION TABLES
-- ==============================================================================

CREATE TABLE dim_date (
    [date] DATE PRIMARY KEY,
    date_key INT NOT NULL,
    [year] INT NOT NULL,
    [month] INT NOT NULL,
    month_name NVARCHAR(50) NOT NULL,
    [quarter] INT NOT NULL,
    [day] INT NOT NULL,
    is_weekend BIT NOT NULL,
    is_winter BIT NOT NULL
);

CREATE TABLE dim_retailers (
    retailer_id INT PRIMARY KEY,
    retailer_name NVARCHAR(255),
    segment NVARCHAR(100),
    city NVARCHAR(100),
    province NVARCHAR(100),
    cohort_year SMALLINT,
    preferred_payment NVARCHAR(100),
    registration_date DATE
);

CREATE TABLE dim_suppliers (
    supplier_id INT PRIMARY KEY,
    supplier_name NVARCHAR(255),
    rating DECIMAL(5,2),
    city NVARCHAR(100),
    province NVARCHAR(100),
    primary_category NVARCHAR(100),
    category_group NVARCHAR(100),
    established_year SMALLINT
);

CREATE TABLE dim_products (
    product_id INT PRIMARY KEY,
    product_name NVARCHAR(255),
    category NVARCHAR(100),
    sku NVARCHAR(100),
    unit_price DECIMAL(18,2)
);

CREATE TABLE dim_areas (
    area_id INT PRIMARY KEY,
    city NVARCHAR(100),
    province NVARCHAR(100),
    neighborhood NVARCHAR(100),
    area_name NVARCHAR(100),
    is_cold BIT NOT NULL
);

CREATE TABLE dim_drivers (
    driver_id INT PRIMARY KEY,
    driver_name NVARCHAR(255),
    vehicle_type NVARCHAR(50),
    rating DECIMAL(5,2),
    city NVARCHAR(100),
    province NVARCHAR(100),
    hire_year SMALLINT,
    active BIT
);

-- ==============================================================================
-- 2. FACT TABLES
-- ==============================================================================

CREATE TABLE fact_order_details (
    detail_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    retailer_id INT NOT NULL,
    supplier_id INT NOT NULL,
    area_id INT NOT NULL,
    driver_id INT NOT NULL,
    order_date DATE NOT NULL,
    order_status NVARCHAR(50),
    quantity INT NOT NULL,
    unit_price DECIMAL(18,2) NOT NULL,
    line_total DECIMAL(18,2) NOT NULL,
    gmv DECIMAL(18,2),

    CONSTRAINT FK_fact_order_details_date     FOREIGN KEY (order_date)   REFERENCES dim_date([date]),
    CONSTRAINT FK_fact_order_details_retailer FOREIGN KEY (retailer_id)  REFERENCES dim_retailers(retailer_id),
    CONSTRAINT FK_fact_order_details_supplier FOREIGN KEY (supplier_id)  REFERENCES dim_suppliers(supplier_id),
    CONSTRAINT FK_fact_order_details_product  FOREIGN KEY (product_id)   REFERENCES dim_products(product_id),
    CONSTRAINT FK_fact_order_details_area     FOREIGN KEY (area_id)      REFERENCES dim_areas(area_id),
    CONSTRAINT FK_fact_order_details_driver   FOREIGN KEY (driver_id)    REFERENCES dim_drivers(driver_id)
);

CREATE TABLE fact_payments (
    payment_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    retailer_id INT NOT NULL,
    payment_method NVARCHAR(50) NOT NULL,
    payment_status NVARCHAR(50) NOT NULL,
    amount DECIMAL(18,2) NOT NULL,
    payment_date DATE,
    currency NVARCHAR(10),

    CONSTRAINT FK_fact_payments_retailer FOREIGN KEY (retailer_id) REFERENCES dim_retailers(retailer_id),
    CONSTRAINT FK_fact_payments_date     FOREIGN KEY (payment_date) REFERENCES dim_date([date])
);

CREATE TABLE fact_deliveries (
    delivery_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    driver_id INT NOT NULL,
    area_id INT NOT NULL,
    scheduled_date DATE NOT NULL,
    scheduled_at DATETIME NOT NULL,
    actual_at DATETIME NULL,
    delivery_status NVARCHAR(50),
    delay_hours DECIMAL(10,2) NULL,
    delay_days DECIMAL(10,2) NULL,

    CONSTRAINT FK_fact_deliveries_driver FOREIGN KEY (driver_id)       REFERENCES dim_drivers(driver_id),
    CONSTRAINT FK_fact_deliveries_area   FOREIGN KEY (area_id)         REFERENCES dim_areas(area_id),
    CONSTRAINT FK_fact_deliveries_date   FOREIGN KEY (scheduled_date)  REFERENCES dim_date([date])
);
