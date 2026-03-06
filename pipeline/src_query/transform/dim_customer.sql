INSERT INTO public.dim_customer (
    customer_id,
    customer_nk,
    customer_zip_code_prefix,
    customer_city,
    customer_state
)

SELECT
    c.id,
    c.customer_id,
    g.geolocation_zip_code_prefix,
    c.customer_city,
    c.customer_state
	
FROM 
    stg.customers c

LEFT JOIN public.dim_geolocation g 
    ON g.geolocation_zip_code_prefix = c.customer_zip_code_prefix
    
ON CONFLICT(customer_nk) 
DO UPDATE SET
    customer_zip_code_prefix = EXCLUDED.customer_zip_code_prefix,
    customer_city = EXCLUDED.customer_city,
    customer_state = EXCLUDED.customer_state,
    updated_at = CURRENT_TIMESTAMP
WHERE 
    public.dim_customer.customer_zip_code_prefix IS DISTINCT FROM EXCLUDED.customer_zip_code_prefix
    OR public.dim_customer.customer_city IS DISTINCT FROM EXCLUDED.customer_city
    OR public.dim_customer.customer_state IS DISTINCT FROM EXCLUDED.customer_state;