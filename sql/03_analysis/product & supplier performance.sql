-- Market basket analysis
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