--Проверим что записей 10000
select count(*) from mock_data md 


--И id все уникальные
select count(distinct id) from mock_data


--Убедимся, что sale_customer_id и другие у каждого нового файла начинаются с 1 
select count(distinct sale_customer_id),
count(distinct sale_seller_id),
count(distinct sale_product_id)
from mock_data

--Посмотрим что за данные
select customer_last_name from mock_data where sale_customer_id = 1


select distinct pet_category from mock_data 


select sale_date from mock_data limit 10


-- Убедимся что нет полных дубликатов
SELECT 'mock_data - полные дубликаты' as check_type,
       COUNT(*) as duplicate_count
FROM (
    SELECT 
        customer_first_name, customer_last_name, customer_age, customer_email,
        customer_country, customer_postal_code, customer_pet_type, customer_pet_name,
        customer_pet_breed, seller_first_name, seller_last_name, seller_email,
        seller_country, seller_postal_code, product_name, product_category,
        product_price, product_quantity, sale_date, sale_customer_id,
        sale_seller_id, sale_product_id, sale_quantity, sale_total_price,
        store_name, store_location, store_city, store_state, store_country,
        store_phone, store_email, pet_category, product_weight, product_color,
        product_size, product_brand, product_material, product_description,
        product_rating, product_reviews, product_release_date, product_expiry_date,
        supplier_name, supplier_contact, supplier_email, supplier_phone,
        supplier_address, supplier_city, supplier_country,
        COUNT(*) as cnt
    FROM mock_data
    GROUP BY 
        customer_first_name, customer_last_name, customer_age, customer_email,
        customer_country, customer_postal_code, customer_pet_type, customer_pet_name,
        customer_pet_breed, seller_first_name, seller_last_name, seller_email,
        seller_country, seller_postal_code, product_name, product_category,
        product_price, product_quantity, sale_date, sale_customer_id,
        sale_seller_id, sale_product_id, sale_quantity, sale_total_price,
        store_name, store_location, store_city, store_state, store_country,
        store_phone, store_email, pet_category, product_weight, product_color,
        product_size, product_brand, product_material, product_description,
        product_rating, product_reviews, product_release_date, product_expiry_date,
        supplier_name, supplier_contact, supplier_email, supplier_phone,
        supplier_address, supplier_city, supplier_country
    HAVING COUNT(*) > 1
) AS duplicates;


-- Убедимся что нет полных дубликатов в данных для будущих таблиц измерений (вероятно в других измерениях их тоже нет)
SELECT 'dim_customers - дубликаты ключам' as check_type,
       COUNT(*) as duplicate_count
FROM (
    SELECT customer_first_name, customer_last_name, 
    customer_email, customer_age, customer_country,
    customer_postal_code, COUNT(*) as cnt
    FROM mock_data
    GROUP BY customer_first_name, customer_last_name, 
    customer_email, customer_age, customer_country,
    customer_postal_code
    HAVING COUNT(*) > 1
) AS duplicates;