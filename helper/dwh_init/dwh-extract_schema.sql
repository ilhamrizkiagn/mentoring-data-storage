CREATE TABLE extract.customers (
    customer_id text NOT NULL,
    customer_unique_id text,
    customer_zip_code_prefix integer,
    customer_city text,
    customer_state text
);


ALTER TABLE extract.customers OWNER TO postgres;

--
-- Name: geolocation; Type: TABLE; Schema: extract; Owner: postgres
--

CREATE TABLE extract.geolocation (
    geolocation_zip_code_prefix integer NOT NULL,
    geolocation_lat real,
    geolocation_lng real,
    geolocation_city text,
    geolocation_state text
);


ALTER TABLE extract.geolocation OWNER TO postgres;

--
-- Name: order_items; Type: TABLE; Schema: extract; Owner: postgres
--

CREATE TABLE extract.order_items (
    order_id text NOT NULL,
    order_item_id integer NOT NULL,
    product_id text,
    seller_id text,
    shipping_limit_date text,
    price real,
    freight_value real
);


ALTER TABLE extract.order_items OWNER TO postgres;

--
-- Name: order_payments; Type: TABLE; Schema: extract; Owner: postgres
--

CREATE TABLE extract.order_payments (
    order_id text NOT NULL,
    payment_sequential integer NOT NULL,
    payment_type text,
    payment_installments integer,
    payment_value real
);


ALTER TABLE extract.order_payments OWNER TO postgres;


--
-- Name: orders; Type: TABLE; Schema: extract; Owner: postgres
--

CREATE TABLE extract.orders (
    order_id text NOT NULL,
    customer_id text,
    order_status text,
    order_purchase_timestamp text,
    order_approved_at text,
    order_delivered_carrier_date text,
    order_delivered_customer_date text,
    order_estimated_delivery_date text
);


ALTER TABLE extract.orders OWNER TO postgres;

--
-- Name: product_category_name_translation; Type: TABLE; Schema: extract; Owner: postgres
--

CREATE TABLE extract.product_category_name_translation (
    product_category_name text NOT NULL,
    product_category_name_english text
);


ALTER TABLE extract.product_category_name_translation OWNER TO postgres;

--
-- Name: products; Type: TABLE; Schema: extract; Owner: postgres
--

CREATE TABLE extract.products (
    product_id text NOT NULL,
    product_category_name text,
    product_name_lenght real,
    product_description_lenght real,
    product_photos_qty real,
    product_weight_g real,
    product_length_cm real,
    product_height_cm real,
    product_width_cm real
);


ALTER TABLE extract.products OWNER TO postgres;

--
-- Name: sellers; Type: TABLE; Schema: extract; Owner: postgres
--

CREATE TABLE extract.sellers (
    seller_id text NOT NULL,
    seller_zip_code_prefix integer,
    seller_city text,
    seller_state text
);


ALTER TABLE extract.sellers OWNER TO postgres;