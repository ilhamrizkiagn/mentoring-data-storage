INSERT INTO stg.customers 
    (customer_id, 
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state) 

SELECT
    customer_id, 
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM extract.customers

ON CONFLICT(customer_id) 
DO UPDATE SET
    customer_city = EXCLUDED.customer_city,
    customer_state = EXCLUDED.customer_state,
    customer_zip_code_prefix = EXCLUDED.customer_zip_code_prefix,
    updated_at = CURRENT_TIMESTAMP
WHERE 
    stg.customers.customer_city IS DISTINCT FROM EXCLUDED.customer_city
    OR stg.customers.customer_state IS DISTINCT FROM EXCLUDED.customer_state
    OR stg.customers.customer_zip_code_prefix IS DISTINCT FROM EXCLUDED.customer_zip_code_prefix;