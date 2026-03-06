INSERT INTO stg.order_items 
    (order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value) 

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
FROM
    extract.order_items

ON CONFLICT(order_id, order_item_id) 
DO UPDATE SET
    shipping_limit_date = EXCLUDED.shipping_limit_date,
    price = EXCLUDED.price,
    freight_value = EXCLUDED.freight_value,
    updated_at = CURRENT_TIMESTAMP
WHERE 
    stg.order_items.shipping_limit_date IS DISTINCT FROM EXCLUDED.shipping_limit_date
    OR stg.order_items.price IS DISTINCT FROM EXCLUDED.price
    OR stg.order_items.freight_value IS DISTINCT FROM EXCLUDED.freight_value;