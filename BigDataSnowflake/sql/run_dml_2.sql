-- ===============================
-- Расширим таблицу mock_data для подготовки данных к загрузке в таблицу фактов
-- ===============================

-- 1. Преобразуем дату в тип DATE
ALTER TABLE mock_data ADD COLUMN sale_date_parsed DATE;
UPDATE mock_data
SET sale_date_parsed = TO_DATE(NULLIF(sale_date, ''), 'MM/DD/YYYY');

-- 2. Получаем ID клиентов
ALTER TABLE mock_data ADD COLUMN customer_id INT;
UPDATE mock_data r
SET customer_id = c.customer_id
FROM dim_customer c
WHERE r.customer_email = c.email;

-- 3. Получаем ID питомца (можно NULL)
ALTER TABLE mock_data ADD COLUMN pet_id INT;
UPDATE mock_data r
SET pet_id = p.pet_id
FROM dim_pet p
WHERE r.customer_id = p.customer_id
  AND r.customer_pet_name = p.pet_name;

-- 4. ID продавца
ALTER TABLE mock_data ADD COLUMN seller_id INT;
UPDATE mock_data r
SET seller_id = s.seller_id
FROM dim_seller s
WHERE r.seller_email = s.email;

-- 5. ID продукта
ALTER TABLE mock_data ADD COLUMN product_id INT;
UPDATE mock_data r
SET product_id = pr.product_id
FROM dim_product pr
WHERE r.product_name = pr.name;

-- 6. ID магазина
ALTER TABLE mock_data ADD COLUMN store_id INT;
UPDATE mock_data r
SET store_id = st.store_id
FROM dim_store st
WHERE r.store_name = st.name;

-- 7. ID поставщика
ALTER TABLE mock_data ADD COLUMN supplier_id INT;
UPDATE mock_data r
SET supplier_id = sp.supplier_id
FROM dim_supplier sp
WHERE r.supplier_name = sp.name;

-- 8. ID даты
ALTER TABLE mock_data ADD COLUMN date_id INT;
UPDATE mock_data r
SET date_id = d.date_id
FROM dim_date d
WHERE r.sale_date_parsed = d.full_date;

-- Наконец, заполним таблицу фактов
INSERT INTO fact_sales (
    customer_id, pet_id, seller_id, product_id, store_id, supplier_id, date_id, sale_quantity, sale_total_price
)
SELECT
    customer_id,
    pet_id,
    seller_id,
    product_id,
    store_id,
    supplier_id,
    date_id,
    sale_quantity,
    sale_total_price
FROM mock_data
WHERE customer_id IS NOT NULL
  AND seller_id IS NOT NULL
  AND product_id IS NOT NULL
  AND store_id IS NOT NULL
  AND supplier_id IS NOT NULL
  AND date_id IS NOT NULL;



