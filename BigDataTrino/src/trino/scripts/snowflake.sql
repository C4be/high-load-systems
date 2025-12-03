INSERT INTO clickhouse.bober_ch_db.mock_data
SELECT
    CAST(id AS bigint) as id,
    CAST(COALESCE(customer_first_name, '') AS varbinary) as customer_first_name,
    CAST(COALESCE(customer_last_name, '') AS varbinary) as customer_last_name,
    CAST(COALESCE(customer_age, 0) AS smallint) as customer_age,
    CAST(COALESCE(customer_email, '') AS varbinary) as customer_email,
    CAST(COALESCE(customer_country, '') AS varbinary) as customer_country,
    CAST(COALESCE(customer_postal_code, '') AS varbinary) as customer_postal_code,
    CAST(COALESCE(customer_pet_type, '') AS varbinary) as customer_pet_type,
    CAST(COALESCE(customer_pet_name, '') AS varbinary) as customer_pet_name,
    CAST(COALESCE(customer_pet_breed, '') AS varbinary) as customer_pet_breed,
    CAST(COALESCE(seller_first_name, '') AS varbinary) as seller_first_name,
    CAST(COALESCE(seller_last_name, '') AS varbinary) as seller_last_name,
    CAST(COALESCE(seller_email, '') AS varbinary) as seller_email,
    CAST(COALESCE(seller_country, '') AS varbinary) as seller_country,
    CAST(COALESCE(seller_postal_code, '') AS varbinary) as seller_postal_code,
    CAST(COALESCE(product_name, '') AS varbinary) as product_name,
    CAST(COALESCE(product_category, '') AS varbinary) as product_category,
    CAST(COALESCE(product_price, 0) AS decimal(18,2)) as product_price,
    CAST(COALESCE(product_quantity, 0) AS bigint) as product_quantity,
    CAST(sale_date AS date) as sale_date,
    CAST(COALESCE(sale_customer_id, 0) AS bigint) as sale_customer_id,
    CAST(COALESCE(sale_seller_id, 0) AS bigint) as sale_seller_id,
    CAST(COALESCE(sale_product_id, 0) AS bigint) as sale_product_id,
    CAST(COALESCE(sale_quantity, 0) AS bigint) as sale_quantity,
    CAST(COALESCE(sale_total_price, 0) AS decimal(18,2)) as sale_total_price,
    CAST(COALESCE(store_name, '') AS varbinary) as store_name,
    CAST(COALESCE(store_location, '') AS varbinary) as store_location,
    CAST(COALESCE(store_city, '') AS varbinary) as store_city,
    CAST(COALESCE(store_state, '') AS varbinary) as store_state,
    CAST(COALESCE(store_country, '') AS varbinary) as store_country,
    CAST(COALESCE(store_phone, '') AS varbinary) as store_phone,
    CAST(COALESCE(store_email, '') AS varbinary) as store_email,
    CAST(COALESCE(pet_category, '') AS varbinary) as pet_category,
    CAST(COALESCE(product_weight, 0) AS decimal(18,2)) as product_weight,
    CAST(COALESCE(product_color, '') AS varbinary) as product_color,
    CAST(COALESCE(product_size, '') AS varbinary) as product_size,
    CAST(COALESCE(product_brand, '') AS varbinary) as product_brand,
    CAST(COALESCE(product_material, '') AS varbinary) as product_material,
    CAST(COALESCE(product_description, '') AS varbinary) as product_description,
    CAST(COALESCE(product_rating, 0) AS decimal(9,1)) as product_rating,
    CAST(COALESCE(product_reviews, 0) AS bigint) as product_reviews,
    CAST(product_release_date AS date) as product_release_date,
    CAST(product_expiry_date AS date) as product_expiry_date,
    CAST(COALESCE(supplier_name, '') AS varbinary) as supplier_name,
    CAST(COALESCE(supplier_contact, '') AS varbinary) as supplier_contact,
    CAST(COALESCE(supplier_email, '') AS varbinary) as supplier_email,
    CAST(COALESCE(supplier_phone, '') AS varbinary) as supplier_phone,
    CAST(COALESCE(supplier_address, '') AS varbinary) as supplier_address,
    CAST(COALESCE(supplier_city, '') AS varbinary) as supplier_city,
    CAST(COALESCE(supplier_country, '') AS varbinary) as supplier_country
FROM postgresql.public.mock_data;


INSERT INTO clickhouse.bober_ch_db.dim_customers (customer_id, first_name, last_name, age, email, country, postal_code)
SELECT
    rand() * 10000000000 as customer_id,
    customer_first_name as first_name,
    customer_last_name as last_name,
    customer_age as age,
    customer_email as email,
    customer_country as country,
    customer_postal_code as postal_code
FROM clickhouse.bober_ch_db.mock_data
GROUP BY
    customer_first_name,
    customer_last_name,
    customer_age,
    customer_email,
    customer_country,
    customer_postal_code;



-- Заполнение измерения питомцев
INSERT INTO clickhouse.bober_ch_db.dim_pets (pet_id, customer_id, pet_type, pet_name, pet_breed)
SELECT
    rand() * 10000000000 as pet_id,
    c.customer_id,
    m.customer_pet_type as pet_type,
    m.customer_pet_name as pet_name,
    m.customer_pet_breed as pet_breed
FROM clickhouse.bober_ch_db.mock_data m
JOIN clickhouse.bober_ch_db.dim_customers c ON
    m.customer_first_name = c.first_name AND
    m.customer_last_name = c.last_name AND
    m.customer_email = c.email AND
    m.customer_age = c.age AND
    m.customer_country = c.country
GROUP BY
    c.customer_id,
    m.customer_pet_type,
    m.customer_pet_name,
    m.customer_pet_breed,
    m.customer_first_name,
    m.customer_last_name,
    m.customer_email,
    m.customer_age,
    m.customer_country;




-- Заполнение измерения продавцов
INSERT INTO clickhouse.bober_ch_db.dim_sellers (seller_id, first_name, last_name, email, country, postal_code)
SELECT
    rand() * 10000000000  as seller_id,
    seller_first_name as first_name,
    seller_last_name as last_name,
    seller_email as email,
    seller_country as country,
    seller_postal_code as postal_code
FROM clickhouse.bober_ch_db.mock_data
GROUP BY
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code;




-- Заполнение измерения продуктов
INSERT INTO clickhouse.bober_ch_db.dim_products (product_id, name, category, price, weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date, pet_category)
SELECT
    rand() * 10000000000  as product_id,
    product_name as name,
    product_category as category,
    product_price as price,
    product_weight as weight,
    product_color as color,
    product_size as size,
    product_brand as brand,
    product_material as material,
    product_description as description,
    product_rating as rating,
    product_reviews as reviews,
    product_release_date as release_date,
    product_expiry_date as expiry_date,
    pet_category
FROM clickhouse.bober_ch_db.mock_data
GROUP BY
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
    product_release_date,
    product_expiry_date,
    pet_category;




-- Заполнение измерения магазинов
INSERT INTO clickhouse.bober_ch_db.dim_stores (store_id, name, location, city, state, country, phone, email)
SELECT
    rand() * 10000000000  as store_id,
    store_name as name,
    store_location as location,
    store_city as city,
    store_state as state,
    store_country as country,
    store_phone as phone,
    store_email as email
FROM clickhouse.bober_ch_db.mock_data
GROUP BY
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email;




-- Заполнение измерения поставщиков
INSERT INTO clickhouse.bober_ch_db.dim_suppliers (supplier_id, name, contact, email, phone, address, city, country)
SELECT
    rand() * 10000000000  as supplier_id,
    supplier_name as name,
    supplier_contact as contact,
    supplier_email as email,
    supplier_phone as phone,
    supplier_address as address,
    supplier_city as city,
    supplier_country as country
FROM clickhouse.bober_ch_db.mock_data
GROUP BY
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country;




-- Заполнение таблицы фактов
INSERT INTO clickhouse.bober_ch_db.fact_sales (sale_id, customer_id, seller_id, product_id, store_id, supplier_id, sale_date, quantity, total_price, unit_price)
SELECT
    m.id as sale_id,
    c.customer_id,
    s.seller_id,
    p.product_id,
    st.store_id,
    sup.supplier_id,
    m.sale_date,
    m.sale_quantity as quantity,
    m.sale_total_price as total_price,
    m.sale_total_price / NULLIF(m.sale_quantity, 0) as unit_price
FROM clickhouse.bober_ch_db.mock_data m
JOIN clickhouse.bober_ch_db.dim_customers c ON
    m.customer_first_name = c.first_name AND
    m.customer_last_name = c.last_name AND
    m.customer_email = c.email AND
    m.customer_age = c.age AND
    m.customer_country = c.country
JOIN clickhouse.bober_ch_db.dim_sellers s ON
    m.seller_first_name = s.first_name AND
    m.seller_last_name = s.last_name AND
    m.seller_email = s.email AND
    m.seller_country = s.country
JOIN clickhouse.bober_ch_db.dim_products p ON
    m.product_name = p.name AND
    m.product_brand = p.brand AND
    m.product_category = p.category AND
    m.product_price = p.price AND
    m.product_weight = p.weight AND
    m.product_material = p.material
JOIN clickhouse.bober_ch_db.dim_stores st ON
    m.store_name = st.name AND
    m.store_location = st.location AND
    m.store_city = st.city AND
    m.store_country = st.country
JOIN clickhouse.bober_ch_db.dim_suppliers sup ON
    m.supplier_name = sup.name AND
    m.supplier_contact = sup.contact AND
    m.supplier_email = sup.email;
