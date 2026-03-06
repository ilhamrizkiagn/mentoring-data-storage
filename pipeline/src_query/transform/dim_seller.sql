INSERT INTO public.dim_seller (
    seller_id,
    seller_nk,
    seller_zip_code_prefix,
    seller_city,
    seller_state
)

SELECT
    s.id,
    s.seller_id,
    g.geolocation_zip_code_prefix,
    s.seller_city,
    s.seller_state
	
FROM 
    stg.sellers s

LEFT JOIN public.dim_geolocation g 
    ON g.geolocation_zip_code_prefix = s.seller_zip_code_prefix
    
ON CONFLICT(seller_nk) 
DO UPDATE SET
    seller_zip_code_prefix = EXCLUDED.seller_zip_code_prefix,
    seller_city = EXCLUDED.seller_city,
    seller_state = EXCLUDED.seller_state,
    updated_at = CURRENT_TIMESTAMP
WHERE 
    public.dim_seller.seller_zip_code_prefix IS DISTINCT FROM EXCLUDED.seller_zip_code_prefix
    OR public.dim_seller.seller_city IS DISTINCT FROM EXCLUDED.seller_city
    OR public.dim_seller.seller_state IS DISTINCT FROM EXCLUDED.seller_state;