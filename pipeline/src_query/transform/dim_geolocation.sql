INSERT INTO public.dim_geolocation (
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
)

SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
	
FROM
    stg.geolocation
    
ON CONFLICT(geolocation_zip_code_prefix) 
DO UPDATE SET
    geolocation_lat = EXCLUDED.geolocation_lat,
    geolocation_lng = EXCLUDED.geolocation_lng,
    geolocation_city = EXCLUDED.geolocation_city,
    geolocation_state = EXCLUDED.geolocation_state,
    updated_at = CURRENT_TIMESTAMP
WHERE 
    public.dim_geolocation.geolocation_lat IS DISTINCT FROM EXCLUDED.geolocation_lat
    OR public.dim_geolocation.geolocation_lng IS DISTINCT FROM EXCLUDED.geolocation_lng
    OR public.dim_geolocation.geolocation_city IS DISTINCT FROM EXCLUDED.geolocation_city
    OR public.dim_geolocation.geolocation_state IS DISTINCT FROM EXCLUDED.geolocation_state;