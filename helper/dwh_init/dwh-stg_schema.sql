CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE stg.customers (
	id UUID NOT NULL DEFAULT uuid_generate_v4(),
	customer_id text PRIMARY KEY,
    customer_unique_id text,
    customer_zip_code_prefix integer,
    customer_city text,
    customer_state text,
	created_at 	TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at 	TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE stg.customers OWNER TO postgres;

CREATE TABLE stg.geolocation (
	id UUID NOT NULL DEFAULT uuid_generate_v4(),
    geolocation_zip_code_prefix integer PRIMARY KEY ,
    geolocation_lat real,
    geolocation_lng real,
    geolocation_city text,
    geolocation_state text,
	created_at 	TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at 	TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE stg.geolocation OWNER TO postgres;

CREATE TABLE stg.order_items (
    order_id text,
    order_item_id integer,
    product_id text,
    seller_id text,
    shipping_limit_date text,
    price real,
    freight_value real,
	PRIMARY KEY (order_id, order_item_id)
);


ALTER TABLE stg.order_items OWNER TO postgres;

CREATE TABLE stg.order_payments (
    order_id text,
    payment_sequential integer,
    payment_type text,
    payment_installments integer,
    payment_value real,
	PRIMARY KEY (order_id, payment_sequential)
);


ALTER TABLE stg.order_payments OWNER TO postgres;

CREATE TABLE stg.orders (
	id UUID NOT NULL DEFAULT uuid_generate_v4(),
    order_id text PRIMARY KEY ,
    customer_id text,
    order_status text,
    order_purchase_timestamp text,
    order_approved_at text,
    order_delivered_carrier_date text,
    order_delivered_customer_date text,
    order_estimated_delivery_date text,
	created_at 	TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at 	TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE stg.orders OWNER TO postgres;


CREATE TABLE stg.product_category_name_translation (
    product_category_name text PRIMARY KEY,
    product_category_name_english text
);


ALTER TABLE stg.product_category_name_translation OWNER TO postgres;

CREATE TABLE stg.products (
	id UUID NOT NULL DEFAULT uuid_generate_v4(),
    product_id text PRIMARY KEY ,
    product_category_name text,
    product_name_lenght real,
    product_description_length real,
    product_photos_qty real,
    product_weight_g real,
    product_length_cm real,
    product_height_cm real,
    product_width_cm real,
	created_at 	TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at 	TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE stg.products OWNER TO postgres;

CREATE TABLE stg.sellers (
	id UUID NOT NULL DEFAULT uuid_generate_v4(),
    seller_id text PRIMARY KEY ,
    seller_zip_code_prefix integer,
    seller_city text,
    seller_state text,
	created_at 	TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at 	TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE stg.sellers OWNER TO postgres;

ALTER TABLE stg.customers
    ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP,
	ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE stg.products
	ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP,
	ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE stg.orders
	ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP,
	ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE stg.sellers
	ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP,
	ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE stg.geolocation
	ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP,
	ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE stg.order_items
    ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	ADD COLUMN updated_at 	TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE stg.order_payments
    ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	ADD COLUMN updated_at 	TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE stg.product_category_name_translation
    ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	ADD COLUMN updated_at 	TIMESTAMP DEFAULT CURRENT_TIMESTAMP;