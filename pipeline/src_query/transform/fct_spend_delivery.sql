EXPLAIN
WITH stg_order_items AS (
    SELECT DISTINCT ON (order_id)
        order_id,
        product_id,
        seller_id,
        price,
        freight_value        
    FROM stg.order_items
),

fakta_spend_delivery AS (
    SELECT
        o.order_id,
        dp.product_id,
        dc.customer_id,
        ds.seller_id,
        dd1.date_id AS order_purchase_date,
        dd2.date_id AS order_delivered_customer_date,
        dd3.date_id AS order_estimated_delivery_date,
        o.order_delivered_customer_date::date - o.order_purchase_timestamp::date
            AS order_purchase_delivered_spend_day,
        o.order_estimated_delivery_date::date - o.order_purchase_timestamp::date
            AS order_purchase_estimated_delivered_spend_day

    FROM stg.orders o
        
    JOIN dim_customer dc
        ON dc.customer_nk = o.customer_id

    JOIN stg_order_items soi
        ON o.order_id = soi.order_id

    JOIN dim_seller ds
        ON soi.seller_id = ds.seller_nk

    JOIN dim_product dp
        ON dp.product_nk = soi.product_id
        AND dp.is_current = true

    JOIN dim_date dd1
        ON dd1.date_actual = o.order_purchase_timestamp::date

    JOIN dim_date dd2
        ON dd2.date_actual = o.order_delivered_customer_date::date

    JOIN dim_date dd3
        ON dd3.date_actual = o.order_estimated_delivery_date::date
)

INSERT INTO fct_spend_delivery (
    order_id,
    product_id,
    customer_id,
    seller_id,
    order_purchase_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    order_purchase_delivered_spend_day,
    order_purchase_estimated_delivered_spend_day
)

SELECT * FROM fakta_spend_delivery

ON CONFLICT(order_id, product_id, seller_id) 
DO UPDATE SET
    order_purchase_date = EXCLUDED.order_purchase_date,
    order_estimated_delivery_date = EXCLUDED.order_estimated_delivery_date,
    order_delivered_customer_date = EXCLUDED.order_delivered_customer_date,
    order_purchase_estimated_delivered_spend_day = EXCLUDED.order_purchase_estimated_delivered_spend_day,
    order_purchase_delivered_spend_day = EXCLUDED.order_purchase_delivered_spend_day,
    updated_at = CURRENT_TIMESTAMP
WHERE 
    public.fct_spend_delivery.order_purchase_date IS DISTINCT FROM EXCLUDED.order_purchase_date
    OR public.fct_spend_delivery.order_delivered_customer_date IS DISTINCT FROM EXCLUDED.order_delivered_customer_date
    OR public.fct_spend_delivery.order_estimated_delivery_date IS DISTINCT FROM EXCLUDED.order_estimated_delivery_date
    OR public.fct_spend_delivery.order_purchase_delivered_spend_day IS DISTINCT FROM EXCLUDED.order_purchase_delivered_spend_day
    OR public.fct_spend_delivery.order_purchase_estimated_delivered_spend_day IS DISTINCT FROM EXCLUDED.order_purchase_estimated_delivered_spend_day;
