-- Создание таблиц измерений

CREATE TABLE bober_ch_db.dim_customers
(
    customer_id UInt64,
    first_name String,
    last_name String,
    age UInt8,
    email String,
    country String,
    postal_code String,
    _version UInt32 DEFAULT 1,
    _timestamp DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (customer_id)
PRIMARY KEY (customer_id)
SETTINGS index_granularity = 8192;

CREATE TABLE bober_ch_db.dim_pets
(
    pet_id UInt64,
    customer_id UInt64,
    pet_type String,
    pet_name String,
    pet_breed String,
    _version UInt32 DEFAULT 1,
    _timestamp DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (pet_id)
PRIMARY KEY (pet_id)
SETTINGS index_granularity = 8192;

CREATE TABLE bober_ch_db.dim_sellers
(
    seller_id UInt64,
    first_name String,
    last_name String,
    email String,
    country String,
    postal_code String,
    _version UInt32 DEFAULT 1,
    _timestamp DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (seller_id)
PRIMARY KEY (seller_id)
SETTINGS index_granularity = 8192;

CREATE TABLE bober_ch_db.dim_products
(
    product_id UInt64,
    name String,
    category String,
    price Decimal64(2),
    weight Decimal64(2),
    color String,
    size String,
    brand String,
    material String,
    description String,
    rating Decimal32(1),
    reviews UInt32,
    release_date Date,
    expiry_date Date,
    pet_category String,
    _version UInt32 DEFAULT 1,
    _timestamp DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (product_id)
PRIMARY KEY (product_id)
SETTINGS index_granularity = 8192;

CREATE TABLE bober_ch_db.dim_stores
(
    store_id UInt64,
    name String,
    location String,
    city String,
    state String,
    country String,
    phone String,
    email String,
    _version UInt32 DEFAULT 1,
    _timestamp DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (store_id)
PRIMARY KEY (store_id)
SETTINGS index_granularity = 8192;

CREATE TABLE bober_ch_db.dim_suppliers
(
    supplier_id UInt64,
    name String,
    contact String,
    email String,
    phone String,
    address String,
    city String,
    country String,
    _version UInt32 DEFAULT 1,
    _timestamp DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (supplier_id)
PRIMARY KEY (supplier_id)
SETTINGS index_granularity = 8192;

-- Создание таблицы фактов
CREATE TABLE bober_ch_db.fact_sales
(
    sale_id UInt64,
    customer_id UInt64,
    seller_id UInt64,
    product_id UInt64,
    store_id UInt64,
    supplier_id UInt64,
    sale_date Date,
    quantity UInt32,
    total_price Decimal64(2),
    unit_price Decimal64(2),
    _timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (sale_date, sale_id)
PARTITION BY toYYYYMM(sale_date)
SETTINGS index_granularity = 8192;