-- Build Order Dimension for Fact Tables Integration
-- to resolve ambiguous relationships in Power BI
CREATE TABLE dim_orders (
       order_id INT PRIMARY KEY
      ,order_date DATE NOT NULL
      ,retailer_id INT NOT NULL
      ,supplier_id INT NOT NULL
      ,area_id INT NOT NULL
      ,driver_id INT NOT NULL
      ,retailer_segment VARCHAR(50) NOT NULL
      ,order_status VARCHAR(25) NOT NULL
      ,gmv DECIMAL(18,2) NOT NULL
)
