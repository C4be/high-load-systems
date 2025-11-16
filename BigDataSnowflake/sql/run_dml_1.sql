-- ===============================
-- Заполняет таблицы измерений из таблицы mock_data
-- ===============================

INSERT INTO dim_customer (first_name, last_name, age, email, country, postal_code)
SELECT DISTINCT
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country,
    customer_postal_code
FROM mock_data md 
WHERE customer_email IS NOT NULL;

INSERT INTO dim_pet (customer_id, pet_type, pet_name, pet_breed, pet_category)
SELECT DISTINCT
    c.customer_id,
    r.customer_pet_type,
    r.customer_pet_name,
    r.customer_pet_breed,
    r.pet_category
FROM mock_data r
JOIN dim_customer c
  ON r.customer_email = c.email;

INSERT INTO dim_seller (first_name, last_name, email, country, postal_code)
SELECT DISTINCT
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
FROM mock_data;


INSERT INTO dim_product (
    name, category, price, weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date
)
SELECT DISTINCT
    product_name,
    product_category,
    product_price,
    product_weight,
    product_color,
    product_size,
    product_brand,
    product_material,
    product_description,
    product_rating,
    product_reviews,
    TO_DATE(NULLIF(product_release_date, ''), 'MM/DD/YYYY'),
    TO_DATE(NULLIF(product_expiry_date, ''), 'MM/DD/YYYY')
FROM mock_data;

INSERT INTO dim_store (name, location, city, state, country, phone, email)
SELECT DISTINCT
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
FROM mock_data;

INSERT INTO dim_supplier (name, contact, email, phone, address, city, country)
SELECT DISTINCT
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM mock_data;

INSERT INTO dim_date (full_date, year, month, day, quarter)
SELECT DISTINCT
    TO_DATE(NULLIF(sale_date, ''), 'MM/DD/YYYY'),
    EXTRACT(YEAR FROM TO_DATE(NULLIF(sale_date, ''), 'MM/DD/YYYY')),
    EXTRACT(MONTH FROM TO_DATE(NULLIF(sale_date, ''), 'MM/DD/YYYY')),
    EXTRACT(DAY FROM TO_DATE(NULLIF(sale_date, ''), 'MM/DD/YYYY')),
    EXTRACT(QUARTER FROM TO_DATE(NULLIF(sale_date, ''), 'MM/DD/YYYY'))
FROM mock_data;