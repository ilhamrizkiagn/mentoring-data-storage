WITH stg_order_items AS (
    SELECT DISTINCT ON (order_id)
        order_id,
        product_id,
        price,
        freight_value
    FROM stg.order_items
),

fakta_purchase_order AS (
    SELECT
        o.order_id,
        dc.customer_id,
        dp.product_id,
        dd.date_id as order_purchase_date,
        soi.price,
        soi.freight_value
    
    FROM stg.orders o
    
    JOIN dim_customer dc
        ON dc.customer_nk = o.customer_id

    JOIN stg_order_items soi
        ON o.order_id = soi.order_id

    JOIN dim_product dp
        ON dp.product_nk = soi.product_id
        AND dp.is_current = true

    JOIN dim_date dd
        ON dd.date_actual = o.order_purchase_timestamp::date
)

INSERT INTO fct_purchase_order (
    order_id,
    customer_id,
    product_id,
    order_purchase_date,
    price,
    freight_value
)

SELECT * FROM fakta_purchase_order

ON CONFLICT(order_id, product_id) 
DO UPDATE SET
    order_purchase_date = EXCLUDED.order_purchase_date,
    price = EXCLUDED.price,
    freight_value = EXCLUDED.freight_value,
    updated_at = CURRENT_TIMESTAMP
WHERE 
    public.fct_purchase_order.order_purchase_date IS DISTINCT FROM EXCLUDED.order_purchase_date
    OR public.fct_purchase_order.price IS DISTINCT FROM EXCLUDED.price
    OR public.fct_purchase_order.freight_value IS DISTINCT FROM EXCLUDED.freight_value;
