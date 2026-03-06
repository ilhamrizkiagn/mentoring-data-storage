BEGIN;

-- Update data lama pada product_category_name
WITH stg_product_category as (
    SELECT 
        p.id,
        p.product_id,
        pcn.product_category_name_english,
        p.product_name_lenght,
        p.product_description_lenght,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm
    FROM stg.products p
    JOIN stg.product_category_name_translation pcn
        ON pcn.product_category_name = p.product_category_name
)

UPDATE dim_product d
SET
    updated_at = CURRENT_TIMESTAMP,
    is_current = false
FROM stg_product_category spc
WHERE d.product_nk = spc.product_id
    AND d.is_current = true
    AND d.product_category_name IS DISTINCT FROM spc.product_category_name_english;

-- INSERT product baru atau versi product_category baru
WITH stg_product_category as (
    SELECT 
        p.id,
        p.product_id,
        pcn.product_category_name_english,
        p.product_name_lenght,
        p.product_description_lenght,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm
    FROM stg.products p
    JOIN stg.product_category_name_translation pcn
        ON pcn.product_category_name = p.product_category_name
)

INSERT INTO public.dim_product(
    product_id,
    product_nk,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    is_current
)

SELECT
    spc.id,
    spc.product_id,
    spc.product_category_name_english,
    spc.product_name_lenght,
    spc.product_description_lenght,
    spc.product_photos_qty,
    spc.product_weight_g,
    spc.product_length_cm,
    spc.product_height_cm,
    spc.product_width_cm,
    TRUE
FROM stg_product_category spc
LEFT JOIN dim_product dp
    ON spc.product_id = dp.product_nk
    AND dp.is_current = TRUE
WHERE
    dp.product_nk IS NULL
    OR dp.product_category_name IS DISTINCT FROM spc.product_category_name_english;

-- UPDATE data selain product_category_name
WITH stg_product_category as (
    SELECT 
        p.id,
        p.product_id,
        pcn.product_category_name_english,
        p.product_name_lenght,
        p.product_description_lenght,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm
    FROM stg.products p
    JOIN stg.product_category_name_translation pcn
        ON pcn.product_category_name = p.product_category_name
)

UPDATE dim_product dp
SET
    product_name_length = spc.product_name_lenght,
    product_description_length = spc.product_description_lenght,
    product_photos_qty = spc.product_photos_qty,
    product_weight_g = spc.product_weight_g,
    product_length_cm = spc.product_length_cm,
    product_height_cm = spc.product_height_cm,
    product_width_cm = spc.product_width_cm
FROM stg_product_category spc
WHERE dp.product_nk = spc.product_id
    AND dp.is_current = TRUE
    AND (
        dp.product_length_cm IS DISTINCT FROM spc.product_length_cm
        OR dp.product_name_length IS DISTINCT FROM spc.product_name_lenght
        OR dp.product_description_length IS DISTINCT FROM spc.product_description_lenght
        OR dp.product_photos_qty IS DISTINCT FROM spc.product_photos_qty
        OR dp.product_weight_g IS DISTINCT FROM spc.product_weight_g
        OR dp.product_height_cm IS DISTINCT FROM spc.product_height_cm
        OR dp.product_width_cm IS DISTINCT FROM spc.product_width_cm
        );
        
COMMIT;