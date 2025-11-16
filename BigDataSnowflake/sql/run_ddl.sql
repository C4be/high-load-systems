-- =====================================
-- Таблицы измерений (Dimensions)
-- =====================================

-- Клиенты
CREATE TABLE dim_customer (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age INT,
    email VARCHAR(255),
    country VARCHAR(100),
    postal_code VARCHAR(20)
);

-- Домашние питомцы клиентов
CREATE TABLE dim_pet (
    pet_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES dim_customer(customer_id),
    pet_type VARCHAR(100),
    pet_name VARCHAR(100),
    pet_breed VARCHAR(100),
    pet_category VARCHAR(100)
);

-- Продавцы
CREATE TABLE dim_seller (
    seller_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    country VARCHAR(100),
    postal_code VARCHAR(20)
);

-- Товары
CREATE TABLE dim_product (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(100),
    price NUMERIC(10,2),
    weight NUMERIC(10,2),
    color VARCHAR(50),
    size VARCHAR(50),
    brand VARCHAR(100),
    material VARCHAR(100),
    description TEXT,
    rating NUMERIC(3,2),
    reviews INT,
    release_date DATE,
    expiry_date DATE
);

-- Магазины
CREATE TABLE dim_store (
    store_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    location VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    phone VARCHAR(50),
    email VARCHAR(255)
);

-- Поставщики
CREATE TABLE dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    contact VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    address VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100)
);

-- Даты
CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    day INT,
    quarter INT
);

-- =====================================
-- Таблица фактов (Fact)
-- =====================================

CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES dim_customer(customer_id),
    pet_id INT REFERENCES dim_pet(pet_id),
    seller_id INT REFERENCES dim_seller(seller_id),
    product_id INT REFERENCES dim_product(product_id),
    store_id INT REFERENCES dim_store(store_id),
    supplier_id INT REFERENCES dim_supplier(supplier_id),
    date_id INT REFERENCES dim_date(date_id),
    sale_quantity INT,
    sale_total_price NUMERIC(10,2)
);

-- Индексы для ускорения аналитических запросов
CREATE INDEX idx_fact_sales_date ON fact_sales(date_id);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_id);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX idx_customer_email ON dim_customer(email);
CREATE INDEX idx_pet_customer_name ON dim_pet(customer_id, pet_name);
CREATE INDEX idx_seller_email ON dim_seller(email);
CREATE INDEX idx_product_name ON dim_product(name);
CREATE INDEX idx_store_name ON dim_store(name);
CREATE INDEX idx_supplier_name ON dim_supplier(name);
CREATE INDEX idx_date_full_date ON dim_date(full_date);