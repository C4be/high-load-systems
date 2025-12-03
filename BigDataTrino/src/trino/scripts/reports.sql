INSERT INTO clickhouse.bober_ch_db.top_10_products
WITH product_sales AS (
    SELECT
        fs.product_id,
        COUNT(*) as sales_count,
        SUM(fs.quantity) as total_quantity,
        SUM(fs.total_price) as total_revenue
    FROM clickhouse.bober_ch_db.fact_sales fs
    GROUP BY fs.product_id
)
SELECT
dp.product_id as product_id,
    dp.name,
    dp.category,
    CAST(ps.total_quantity AS BIGINT) as total_quantity,
    ps.total_revenue as total_revenue,
    CAST(ps.sales_count AS BIGINT) as sales_count,
    CAST(dp.rating AS DECIMAL(5,3)) as rating,
    CAST(dp.reviews AS BIGINT) as reviews,
    CURRENT_DATE as created_date
FROM product_sales ps
JOIN clickhouse.bober_ch_db.dim_products dp ON ps.product_id = dp.product_id
ORDER BY ps.total_quantity DESC
LIMIT 10;




-- Доход по категориям
INSERT INTO clickhouse.bober_ch_db.category_revenue_mart
SELECT
    dp.category,
    SUM(fs.total_price) as category_revenue,
    SUM(fs.quantity) as category_total_quantity,
    COUNT(DISTINCT fs.product_id) as products_count,
    CURRENT_DATE as created_date
FROM clickhouse.bober_ch_db.fact_sales fs
JOIN clickhouse.bober_ch_db.dim_products dp ON fs.product_id = dp.product_id
GROUP BY dp.category;



-- Рейтинги продуктов
INSERT INTO clickhouse.bober_ch_db.product_ratings_mart
SELECT
    dp.product_id,
    dp.name,
    dp.category,
    dp.rating as rating,
    dp.reviews as reviews,
    CURRENT_DATE as created_date
FROM clickhouse.bober_ch_db.dim_products dp;

-- 2. Витрина продаж по клиентам

-- Продажи по клиентам
INSERT INTO clickhouse.bober_ch_db.customer_sales_mart
WITH customer_stats AS (
    SELECT
        fs.customer_id,
        SUM(fs.total_price) as total_spent,
        COUNT(*) as purchase_count,
        AVG(fs.total_price) as avg_order_value
    FROM clickhouse.bober_ch_db.fact_sales fs
    GROUP BY fs.customer_id
)
SELECT
    dc.customer_id,
    dc.first_name,
    dc.last_name,
    dc.country,
    cs.total_spent,
    cs.purchase_count as purchase_count,
    cs.avg_order_value,
    CURRENT_DATE as created_date
FROM customer_stats cs
JOIN clickhouse.bober_ch_db.dim_customers dc ON cs.customer_id = dc.customer_id;

-- Топ-10 клиентов
INSERT INTO clickhouse.bober_ch_db.top_10_customers
SELECT
    dc.customer_id,
    dc.first_name,
    dc.last_name,
    dc.country,
    SUM(fs.total_price) as total_spent,
    COUNT(*) as purchase_count,
    AVG(fs.total_price) as avg_order_value,
    CURRENT_DATE as created_date
FROM clickhouse.bober_ch_db.fact_sales fs
JOIN clickhouse.bober_ch_db.dim_customers dc ON fs.customer_id = dc.customer_id
GROUP BY dc.customer_id, dc.first_name, dc.last_name, dc.country
ORDER BY total_spent DESC
LIMIT 10;

-- Распределение по странам
INSERT INTO clickhouse.bober_ch_db.customer_country_distribution
SELECT
    dc.country,
    COUNT(DISTINCT dc.customer_id) as customer_count,
    SUM(fs.total_price) as total_spent_by_country,
    CURRENT_DATE as created_date
FROM clickhouse.bober_ch_db.fact_sales fs
JOIN clickhouse.bober_ch_db.dim_customers dc ON fs.customer_id = dc.customer_id
GROUP BY dc.country;

-- 3. Витрина продаж по времени



-- Годовые тренды
INSERT INTO clickhouse.bober_ch_db.yearly_trends_mart
SELECT
    YEAR(fs.sale_date) as year,
    SUM(fs.total_price) as yearly_revenue,
    COUNT(*) as yearly_orders,
    AVG(fs.total_price) as avg_order_size_yearly,
    CURRENT_DATE as created_date
FROM clickhouse.bober_ch_db.fact_sales fs
GROUP BY YEAR(fs.sale_date);



-- 4. Витрина продаж по магазинам

-- Продажи по магазинам
INSERT INTO clickhouse.bober_ch_db.store_sales_mart
WITH store_stats AS (
    SELECT
        fs.store_id,
        SUM(fs.total_price) as total_revenue,
        COUNT(*) as total_orders,
        COUNT(DISTINCT fs.customer_id) as unique_customers,
        SUM(fs.quantity) as total_quantity_sold,
        AVG(fs.total_price) as avg_order_value
    FROM clickhouse.bober_ch_db.fact_sales fs
    GROUP BY fs.store_id
)
SELECT
    ds.store_id,
    ds.name,
    ds.city,
    ds.country,
    ss.total_revenue,
    ss.total_orders as total_orders,
    ss.unique_customers as unique_customers,
    ss.total_quantity_sold as total_quantity_sold,
    ss.avg_order_value,
    CURRENT_DATE as created_date
FROM store_stats ss
JOIN clickhouse.bober_ch_db.dim_stores ds ON ss.store_id = ds.store_id;

-- Топ-5 магазинов
INSERT INTO clickhouse.bober_ch_db.top_5_stores
SELECT
    ds.store_id,
    ds.name,
    ds.city,
    ds.country,
    SUM(fs.total_price) as total_revenue,
    COUNT(*) as total_orders,
    COUNT(DISTINCT fs.customer_id) as unique_customers,
    SUM(fs.quantity) as total_quantity_sold,
    AVG(fs.total_price) as avg_order_value,
    CURRENT_DATE as created_date
FROM clickhouse.bober_ch_db.fact_sales fs
JOIN clickhouse.bober_ch_db.dim_stores ds ON fs.store_id = ds.store_id
GROUP BY ds.store_id, ds.name, ds.city, ds.country
ORDER BY total_revenue DESC
LIMIT 5;

-- Геораспределение магазинов
INSERT INTO clickhouse.bober_ch_db.store_geo_distribution
SELECT
    ds.country,
    ds.city,
    SUM(fs.total_price) as total_revenue_by_location,
    COUNT(DISTINCT ds.store_id) as store_count,
    COUNT(*) as total_orders_by_location,
    AVG(fs.total_price) as avg_order_value_by_location,
    CURRENT_DATE as created_date
FROM clickhouse.bober_ch_db.fact_sales fs
JOIN clickhouse.bober_ch_db.dim_stores ds ON fs.store_id = ds.store_id
GROUP BY ds.country, ds.city;

-- 5. Витрина продаж по поставщикам

-- Продажи по поставщикам
INSERT INTO clickhouse.bober_ch_db.supplier_sales_mart
WITH supplier_stats AS (
    SELECT
        fs.supplier_id,
        SUM(fs.total_price) as total_revenue,
        COUNT(*) as total_sales,
        COUNT(DISTINCT fs.product_id) as unique_products_sold,
        SUM(fs.quantity) as total_quantity_sold,
        AVG(fs.unit_price) as avg_product_price,
        MIN(fs.unit_price) as min_product_price,
        MAX(fs.unit_price) as max_product_price
    FROM clickhouse.bober_ch_db.fact_sales fs
    GROUP BY fs.supplier_id
)
SELECT
    ds.supplier_id,
    ds.name,
    ds.country,
    ds.contact,
    ss.total_revenue,
    ss.total_sales as total_sales,
    ss.unique_products_sold as unique_products_sold,
    ss.total_quantity_sold as total_quantity_sold,
    ss.avg_product_price,
    ss.min_product_price,
    ss.max_product_price,
    CURRENT_DATE as created_date
FROM supplier_stats ss
JOIN clickhouse.bober_ch_db.dim_suppliers ds ON ss.supplier_id = ds.supplier_id;

-- Топ-5 поставщиков
INSERT INTO clickhouse.bober_ch_db.top_5_suppliers
SELECT
    ds.supplier_id,
    ds.name,
    ds.country,
    ds.contact,
    SUM(fs.total_price) as total_revenue,
    COUNT(*) as total_sales,
    COUNT(DISTINCT fs.product_id) as unique_products_sold,
    SUM(fs.quantity) as total_quantity_sold,
    AVG(fs.unit_price) as avg_product_price,
    MIN(fs.unit_price) as min_product_price,
    MAX(fs.unit_price) as max_product_price,
    CURRENT_DATE as created_date
FROM clickhouse.bober_ch_db.fact_sales fs
JOIN clickhouse.bober_ch_db.dim_suppliers ds ON fs.supplier_id = ds.supplier_id
GROUP BY ds.supplier_id, ds.name, ds.country, ds.contact
ORDER BY total_revenue DESC
LIMIT 5;

-- Распределение поставщиков по странам
INSERT INTO clickhouse.bober_ch_db.supplier_country_distribution
SELECT
    ds.country,
    SUM(fs.total_price) as total_revenue_by_country,
    COUNT(DISTINCT ds.supplier_id) as supplier_count,
    COUNT(*) as total_sales_by_country,
    AVG(fs.unit_price) as avg_product_price_by_country,
    CURRENT_DATE as created_date
FROM clickhouse.bober_ch_db.fact_sales fs
JOIN clickhouse.bober_ch_db.dim_suppliers ds ON fs.supplier_id = ds.supplier_id
GROUP BY ds.country;

-- 6. Витрина качества продукции

-- Качество продукции
INSERT INTO clickhouse.bober_ch_db.product_quality_mart
WITH product_sales AS (
    SELECT
        product_id,
        SUM(quantity) as total_sold,
        SUM(total_price) as total_revenue,
        COUNT(*) as sales_count,
        AVG(unit_price) as avg_sale_price
    FROM clickhouse.bober_ch_db.fact_sales
    GROUP BY product_id
)
SELECT
    dp.product_id,
    dp.name,
    dp.category,
    dp.rating as rating,
    dp.reviews as reviews,
    ps.total_sold as total_sold,
    ps.total_revenue,
    ps.sales_count as sales_count,
    ps.avg_sale_price,
    CURRENT_DATE as created_date
FROM clickhouse.bober_ch_db.dim_products dp
JOIN product_sales ps ON dp.product_id = ps.product_id;

-- Продукты с наивысшим рейтингом
INSERT INTO clickhouse.bober_ch_db.highest_rated_products
SELECT
    dp.product_id,
    dp.name,
    dp.category,
    dp.rating as rating,
    dp.reviews as reviews,
    SUM(fs.quantity) as total_sold,
    SUM(fs.total_price) as total_revenue,
    COUNT(*) as sales_count,
    AVG(fs.unit_price) as avg_sale_price,
    CURRENT_DATE as created_date
FROM clickhouse.bober_ch_db.fact_sales fs
JOIN clickhouse.bober_ch_db.dim_products dp ON fs.product_id = dp.product_id
WHERE dp.rating >= 4.0
GROUP BY dp.product_id, dp.name, dp.category, dp.rating, dp.reviews
ORDER BY dp.rating DESC, dp.reviews DESC
LIMIT 20;

-- Продукты с наинизшим рейтингом
INSERT INTO clickhouse.bober_ch_db.lowest_rated_products
SELECT
    dp.product_id,
    dp.name,
    dp.category,
    dp.rating as rating,
    dp.reviews as reviews,
    SUM(fs.quantity) as total_sold,
    SUM(fs.total_price) as total_revenue,
    COUNT(*) as sales_count,
    AVG(fs.unit_price) as avg_sale_price,
    CURRENT_DATE as created_date
FROM clickhouse.bober_ch_db.fact_sales fs
JOIN clickhouse.bober_ch_db.dim_products dp ON fs.product_id = dp.product_id
WHERE dp.rating < 3.0 AND dp.reviews > 10
GROUP BY dp.product_id, dp.name, dp.category, dp.rating, dp.reviews
ORDER BY dp.rating ASC, dp.reviews DESC
LIMIT 20;

-- Продукты с наибольшим количеством отзывов
INSERT INTO clickhouse.bober_ch_db.most_reviewed_products
SELECT
    dp.product_id,
    dp.name,
    dp.category,
    dp.rating as rating,
    dp.reviews as reviews,
    SUM(fs.quantity) as total_sold,
    SUM(fs.total_price) as total_revenue,
    COUNT(*) as sales_count,
    AVG(fs.unit_price) as avg_sale_price,
    CURRENT_DATE as created_date
FROM clickhouse.bober_ch_db.fact_sales fs
JOIN clickhouse.bober_ch_db.dim_products dp ON fs.product_id = dp.product_id
GROUP BY dp.product_id, dp.name, dp.category, dp.rating, dp.reviews
ORDER BY dp.reviews DESC
LIMIT 20;

-- Анализ корреляции
INSERT INTO clickhouse.bober_ch_db.correlation_analysis_mart
WITH product_stats AS (
    SELECT
        dp.rating,
        dp.reviews,
        SUM(fs.quantity) as total_sold,
        SUM(fs.total_price) as total_revenue
    FROM clickhouse.bober_ch_db.fact_sales fs
    JOIN clickhouse.bober_ch_db.dim_products dp ON fs.product_id = dp.product_id
    GROUP BY dp.product_id, dp.rating, dp.reviews
)
SELECT
    CORR(rating, total_sold) as corr_rating_sales,
    CORR(rating, total_revenue) as corr_rating_revenue,
    CORR(rating, reviews) as corr_rating_reviews,
    CURRENT_DATE as analysis_date
FROM product_stats;