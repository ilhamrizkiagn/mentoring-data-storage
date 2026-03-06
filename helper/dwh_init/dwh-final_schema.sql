CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- create table dim_date
DROP TABLE if exists dim_date CASCADE;
CREATE TABLE dim_date
(
	date_id 		INTEGER PRIMARY KEY,
	date_actual 	DATE NOT NULL,
	day_name 		VARCHAR(9),
	day_of_year 	INTEGER,
	week_of_month 	INTEGER,
	month_actual 	INTEGER,
	month_name 		VARCHAR(9),
	quarter_actual 	INTEGER,
	quarter_name 	VARCHAR(9),
	year_actual 	INTEGER,
	mmyyyy 			CHAR(6),
	mmddyyyy 		CHAR(8)
);

-- Create Table dim_geolocation
DROP TABLE if exists dim_geolocation CASCADE;
CREATE TABLE dim_geolocation (
	geolocation_zip_code_prefix INTEGER PRIMARY KEY,
	geolocation_lat 			REAL NOT NULL,
	geolocation_lng 			REAL NOT NULL,
	geolocation_city 			TEXT NOT NULL,
	geolocation_state 			TEXT NOT NULL,
	created_at 					TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at 					TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- create table dim_seller
DROP TABLE if exists dim_seller CASCADE;
CREATE TABLE dim_seller (
	seller_id 				UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	seller_nk 				TEXT,
	seller_zip_code_prefix 	INTEGER,
	seller_city				TEXT,
	seller_state			TEXT,
	created_at 				TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at 				TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT fk_seller_geolocation FOREIGN KEY (seller_zip_code_prefix) REFERENCES dim_geolocation(geolocation_zip_code_prefix)
);

-- create table dim_product
DROP TABLE if exists dim_product CASCADE;
CREATE TABLE dim_product (
	product_id 					UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	product_nk 					TEXT NOT NULL,
	product_category_name		TEXT NOT NULL,
	product_name_length 		REAL,
	product_description_length 	REAL,
	product_photos_qty	 		REAL,
	product_weight_g 			REAL,
	product_length_cm 			REAL,
	product_height_cm 			REAL,
	product_width_cm 			REAL,
	is_current					BOOLEAN NOT NULL,
	created_at 					TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at 					TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- create table dim_customer
DROP TABLE if exists dim_customer CASCADE;
CREATE TABLE dim_customer (
	customer_id 				UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	customer_nk					TEXT NOT NULL,
	customer_zip_code_prefix 	INTEGER,
	customer_city 				TEXT,
	customer_state 				TEXT,
	created_at 					TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at 					TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT fk_customer_geolocation FOREIGN KEY (customer_zip_code_prefix) REFERENCES dim_geolocation(geolocation_zip_code_prefix)
);

DROP TABLE if exists fct_purchase_order CASCADE;
CREATE TABLE fct_purchase_order
(
	purchase_order_id 	UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	order_id			TEXT NOT NULL,
	customer_id			UUID NOT NULL,
	product_id			UUID NOT NULL,
	order_purchase_date	INT NOT NULL,
	price				REAL NOT NULL,
	freight_value		REAL,
	created_at 			TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at 			TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT fk_purchase_customer FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
	CONSTRAINT fk_purchase_product FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
	CONSTRAINT fk_purchase_date FOREIGN KEY (order_purchase_date) REFERENCES dim_date,
	CONSTRAINT fct_purchase_order_unique UNIQUE (order_id, product_id)
);

DROP TABLE if exists fct_shipment_status CASCADE;
CREATE TABLE fct_shipment_status
(
	shipment_status_id				UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	order_id						TEXT NOT NULL,
	customer_id						UUID NOT NULL,
	seller_id						UUID NOT NULL,
	order_status					TEXT NOT NULL,
	order_purchase_date				INT NOT NULL,
	order_approved_at				INT NOT NULL,
	order_delivered_carrier_date	INT NOT NULL,
	order_delivered_customer_date	INT NOT NULL,
	order_estimated_delivery_date	INT NOT NULL,
	shipping_limit_date				INT NOT NULL,
	created_at 						TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at 						TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT fk_shipment_customer FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
	CONSTRAINT fk_shipment_purchase FOREIGN KEY (order_purchase_date) REFERENCES dim_date,
	CONSTRAINT fk_shipment_approved FOREIGN KEY (order_approved_at) REFERENCES dim_date,
	CONSTRAINT fk_shipment_carrier FOREIGN KEY (order_delivered_carrier_date) REFERENCES dim_date,
	CONSTRAINT fk_shipment_delivered_customer FOREIGN KEY (order_delivered_customer_date) 
		REFERENCES dim_date,
	CONSTRAINT fk_shipment_estimated FOREIGN KEY (order_estimated_delivery_date) REFERENCES dim_date,
	CONSTRAINT fct_shipment_status_unique UNIQUE (order_id, seller_id)
);

ALTER TABLE fct_shipment_status
ADD CONSTRAINT fk_shipment_seller FOREIGN KEY (seller_id) REFERENCES dim_seller(seller_id);

DROP TABLE if exists fct_spend_delivery CASCADE;
CREATE TABLE fct_spend_delivery
(
	spend_delivery_id					UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
	order_id							TEXT NOT NULL,
	product_id							UUID NOT NULL,
	customer_id							UUID NOT NULL,
	seller_id							UUID NOT NULL,
	order_purchase_date					INT NOT NULL,
	order_delivered_customer_date		INT NOT NULL,
	order_estimated_delivery_date		INT NOT NULL,
	order_purchase_delivered_spend_day	INT NOT NULL,
	order_purchase_estimated_delivered_spend_day	INT NOT NULL,
	created_at 							TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at 							TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT fk_spend_product FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
	CONSTRAINT fk_spend_customer FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
	CONSTRAINT fk_spend_seller FOREIGN KEY (seller_id) REFERENCES dim_seller(seller_id),
	CONSTRAINT fk_spend_purchase FOREIGN KEY (order_purchase_date) REFERENCES dim_date,
	CONSTRAINT fk_spend_delivered_customer FOREIGN KEY (order_delivered_customer_date) 
		REFERENCES dim_date,
	CONSTRAINT fk_spend_estimated FOREIGN KEY (order_estimated_delivery_date) REFERENCES dim_date,
	CONSTRAINT fct_spend_unique UNIQUE (order_id, product_id, seller_id)
);

-- Insert dim_date value
INSERT INTO public.dim_date
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_id,
       datum AS date_actual,
       TO_CHAR(datum, 'TMDay') AS day_name,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'TMMonth') AS month_name,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(YEAR FROM datum) AS year_actual,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy
FROM (SELECT '1998-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;


ALTER TABLE dim_customer
	ADD CONSTRAINT unique_dim_customer UNIQUE(customer_nk);

ALTER TABLE dim_product
	ADD CONSTRAINT unique_dim_product UNIQUE(product_nk);

ALTER TABLE dim_seller
	ADD CONSTRAINT unique_dim_seller UNIQUE(seller_nk);