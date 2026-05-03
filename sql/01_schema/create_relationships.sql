-- fact_order_details
ALTER TABLE fact_order_details
ADD CONSTRAINT FK_fact_order_details_order
    FOREIGN KEY (order_id) REFERENCES dim_orders(order_id);

-- fact_payments
ALTER TABLE fact_payments
ADD CONSTRAINT FK_fact_payments_order
    FOREIGN KEY (order_id) REFERENCES dim_orders(order_id);

-- fact_deliveries
ALTER TABLE fact_deliveries
ADD CONSTRAINT FK_fact_deliveries_order
    FOREIGN KEY (order_id) REFERENCES dim_orders(order_id);
