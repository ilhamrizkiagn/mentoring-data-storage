EXPLAIN
-- Update data lama pada product_category_name
WITH stg_order_items AS (
    SELECT DISTINCT ON (order_id)
        order_id,
        price,
        freight_value,
        seller_id,
		shipping_limit_date
    FROM stg.order_items
),

fakta_shipment_status AS (
    SELECT
        o.order_id,
        dc.customer_id,
        ds.seller_id,
        o.order_status,
        dd1.date_id AS order_purchase_timestamp,
        dd2.date_id AS order_approved_at,
        dd3.date_id AS order_delivered_carrier_date,
        dd4.date_id AS order_delivered_customer_date,
        dd5.date_id AS order_estimated_delivery_date,
        dd6.date_id AS shipping_limit_date

    FROM stg.orders o
    
    JOIN dim_customer dc
        ON dc.customer_nk = o.customer_id

    JOIN stg_order_items soi
        ON o.order_id = soi.order_id

    JOIN dim_seller ds
        ON soi.seller_id = ds.seller_nk

    JOIN dim_date dd1
        ON dd1.date_actual = o.order_purchase_timestamp::date

    JOIN dim_date dd2
        ON dd2.date_actual = o.order_approved_at::date
    
    JOIN dim_date dd3
        ON dd3.date_actual = o.order_delivered_carrier_date::date
    
    JOIN dim_date dd4
        ON dd4.date_actual = o.order_delivered_customer_date::date

    JOIN dim_date dd5
        ON dd5.date_actual = o.order_estimated_delivery_date::date

    JOIN dim_date dd6
        ON dd6.date_actual = soi.shipping_limit_date::date
)

INSERT INTO fct_shipment_status (
    order_id,
    customer_id,
    seller_id,
    order_status,
    order_purchase_date,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    shipping_limit_date
)

SELECT * FROM fakta_shipment_status

ON CONFLICT(order_id, seller_id) 
DO UPDATE SET
    order_status = EXCLUDED.order_status,
    order_purchase_date = EXCLUDED.order_purchase_date,
    order_approved_at = EXCLUDED.order_approved_at,
    order_delivered_carrier_date = EXCLUDED.order_delivered_carrier_date,
    order_delivered_customer_date = EXCLUDED.order_delivered_customer_date,
    order_estimated_delivery_date = EXCLUDED.order_estimated_delivery_date,
    shipping_limit_date = EXCLUDED.shipping_limit_date,
    updated_at = CURRENT_TIMESTAMP
WHERE 
    public.fct_shipment_status.order_status IS DISTINCT FROM EXCLUDED.order_status
    OR public.fct_shipment_status.order_purchase_date IS DISTINCT FROM EXCLUDED.order_purchase_date
    OR public.fct_shipment_status.order_approved_at IS DISTINCT FROM EXCLUDED.order_approved_at
    OR public.fct_shipment_status.order_delivered_carrier_date IS DISTINCT FROM EXCLUDED.order_delivered_carrier_date
    OR public.fct_shipment_status.order_delivered_customer_date IS DISTINCT FROM EXCLUDED.order_delivered_customer_date
    OR public.fct_shipment_status.order_estimated_delivery_date IS DISTINCT FROM EXCLUDED.order_estimated_delivery_date
    OR public.fct_shipment_status.shipping_limit_date IS DISTINCT FROM EXCLUDED.shipping_limit_date;
