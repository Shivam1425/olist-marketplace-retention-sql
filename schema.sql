/* =========================================================
SCHEMA: E-Commerce Customer Behavior, Retention and Profitability Analysis
AUTHOR: Shivam Kumar
DIALECT: MySQL 8+
PURPOSE: Production-style base schema for the Olist dataset
========================================================= */

CREATE DATABASE IF NOT EXISTS ecommerce
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE ecommerce;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

/* =========================================================
DROP TABLES
The drop order is reversed from the dependency order.
========================================================= */
DROP TABLE IF EXISTS geolocation;
DROP TABLE IF EXISTS product_category_name_translation;
DROP TABLE IF EXISTS order_reviews;
DROP TABLE IF EXISTS order_payments;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS sellers;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;

SET FOREIGN_KEY_CHECKS = 1;

/* =========================================================
RAW DATASET NOTES
- Column names such as product_name_lenght are intentionally preserved
  to match the source CSV headers exactly.
- geolocation has no natural primary key because the raw file contains
  multiple latitude/longitude rows for the same zip prefix.
- customer_unique_id is the true person-level key for analytics, while
  customer_id is the transactional key referenced by orders.
========================================================= */

/* =========================================================
DIMENSION TABLES
========================================================= */
CREATE TABLE customers (
    customer_id VARCHAR(50) NOT NULL,
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state CHAR(2),
    PRIMARY KEY (customer_id)
) ENGINE = InnoDB;

CREATE TABLE products (
    product_id VARCHAR(50) NOT NULL,
    product_category_name VARCHAR(100),
    product_name_lenght INT COMMENT 'Source spelling preserved for CSV compatibility',
    product_description_lenght INT COMMENT 'Source spelling preserved for CSV compatibility',
    product_photos_qty INT,
    product_weight_g DECIMAL(12,2),
    product_length_cm DECIMAL(12,2),
    product_height_cm DECIMAL(12,2),
    product_width_cm DECIMAL(12,2),
    PRIMARY KEY (product_id),
    CONSTRAINT chk_products_dimensions_nonnegative CHECK (
        COALESCE(product_weight_g, 0) >= 0
        AND COALESCE(product_length_cm, 0) >= 0
        AND COALESCE(product_height_cm, 0) >= 0
        AND COALESCE(product_width_cm, 0) >= 0
    )
) ENGINE = InnoDB;

CREATE TABLE sellers (
    seller_id VARCHAR(50) NOT NULL,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(100),
    seller_state CHAR(2),
    PRIMARY KEY (seller_id)
) ENGINE = InnoDB;

CREATE TABLE product_category_name_translation (
    product_category_name VARCHAR(100) NOT NULL,
    product_category_name_english VARCHAR(100),
    PRIMARY KEY (product_category_name)
) ENGINE = InnoDB;

CREATE TABLE geolocation (
    geolocation_zip_code_prefix INT,
    geolocation_lat DECIMAL(12,8),
    geolocation_lng DECIMAL(12,8),
    geolocation_city VARCHAR(100),
    geolocation_state CHAR(2)
) ENGINE = InnoDB;

/* =========================================================
FACT TABLES
========================================================= */
CREATE TABLE orders (
    order_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    order_status VARCHAR(30),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    PRIMARY KEY (order_id),
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
) ENGINE = InnoDB;

CREATE TABLE order_items (
    order_id VARCHAR(50) NOT NULL,
    order_item_id INT NOT NULL,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date DATETIME,
    price DECIMAL(12,2),
    freight_value DECIMAL(12,2),
    PRIMARY KEY (order_id, order_item_id),
    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CONSTRAINT fk_order_items_product
        FOREIGN KEY (product_id) REFERENCES products(product_id),
    CONSTRAINT fk_order_items_seller
        FOREIGN KEY (seller_id) REFERENCES sellers(seller_id),
    CONSTRAINT chk_order_items_money_nonnegative CHECK (
        COALESCE(price, 0) >= 0
        AND COALESCE(freight_value, 0) >= 0
    )
) ENGINE = InnoDB;

CREATE TABLE order_payments (
    order_id VARCHAR(50) NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(30),
    payment_installments INT,
    payment_value DECIMAL(12,2),
    PRIMARY KEY (order_id, payment_sequential),
    CONSTRAINT fk_order_payments_order
        FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CONSTRAINT chk_order_payments_values CHECK (
        COALESCE(payment_sequential, 0) > 0
        AND COALESCE(payment_installments, 0) >= 0
        AND COALESCE(payment_value, 0) >= 0
    )
) ENGINE = InnoDB;

CREATE TABLE order_reviews (
    review_id VARCHAR(50),
    order_id VARCHAR(50) NOT NULL,
    review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME,
    PRIMARY KEY (order_id),
    CONSTRAINT fk_order_reviews_order
        FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CONSTRAINT chk_review_score_range CHECK (
        review_score IS NULL OR review_score BETWEEN 1 AND 5
    )
) ENGINE = InnoDB;

/* =========================================================
ANALYTICAL INDEXES
These indexes are tuned for the joins, filters, cohorts, and windowed
aggregations used in analysis.sql.
========================================================= */
CREATE INDEX idx_customers_unique_id
    ON customers (customer_unique_id);

CREATE INDEX idx_customers_state_city
    ON customers (customer_state, customer_city);

CREATE INDEX idx_products_category
    ON products (product_category_name);

CREATE INDEX idx_sellers_state_city
    ON sellers (seller_state, seller_city);

CREATE INDEX idx_geolocation_zip_state_city
    ON geolocation (geolocation_zip_code_prefix, geolocation_state, geolocation_city);

CREATE INDEX idx_orders_customer_status_purchase
    ON orders (customer_id, order_status, order_purchase_timestamp);

CREATE INDEX idx_orders_purchase_status
    ON orders (order_purchase_timestamp, order_status);

CREATE INDEX idx_orders_delivery_dates
    ON orders (order_estimated_delivery_date, order_delivered_customer_date);

CREATE INDEX idx_order_items_product
    ON order_items (product_id);

CREATE INDEX idx_order_items_seller
    ON order_items (seller_id);

CREATE INDEX idx_order_items_shipping_limit
    ON order_items (shipping_limit_date);

CREATE INDEX idx_order_payments_type_installments
    ON order_payments (payment_type, payment_installments);

CREATE INDEX idx_order_reviews_score_creation
    ON order_reviews (review_score, review_creation_date);

/* =========================================================
RECOMMENDED LOAD ORDER
1. customers
2. sellers
3. products
4. product_category_name_translation
5. geolocation
6. orders
7. order_items
8. order_payments
9. order_reviews

Tip: During bulk CSV imports, temporarily disable foreign_key_checks if
your loader inserts out of order, then re-enable them after validation.
========================================================= */
