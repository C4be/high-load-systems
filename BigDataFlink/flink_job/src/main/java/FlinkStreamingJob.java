import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import java.sql.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class FlinkStreamingJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Настройка Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("mock_data_topic")
            .setGroupId("flink-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // Создание потока данных из Kafka
        DataStream<String> kafkaStream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );

        // Парсинг JSON и преобразование в объект
        DataStream<MockData> parsedStream = kafkaStream.map(new MapFunction<String, MockData>() {
            private transient ObjectMapper objectMapper;

            @Override
            public MockData map(String value) throws Exception {
                if (objectMapper == null) {
                    objectMapper = new ObjectMapper();
                }
                return MockData.fromJson(objectMapper.readTree(value));
            }
        });

        // Преобразование в снежинку и вставка в измерения
        DataStream<SnowflakeData> snowflakeStream = parsedStream.map(new RichMapFunction<MockData, SnowflakeData>() {
            private transient Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                Class.forName("org.postgresql.Driver");
                connection = DriverManager.getConnection(
                    "jdbc:postgresql://highload_db:5432/highload_db",
                    "postgres",
                    "highload"
                );
            }

            @Override
            public SnowflakeData map(MockData data) throws Exception {
                SnowflakeData snowflake = new SnowflakeData();

                // Вставка в dim_customers и получение customer_id
                snowflake.customerId = insertOrGetCustomer(connection, data);

                // Вставка в dim_pets и получение pet_id
                snowflake.petId = insertOrGetPet(connection, snowflake.customerId, data);

                // Вставка в dim_sellers и получение seller_id
                snowflake.sellerId = insertOrGetSeller(connection, data);

                // Вставка в dim_products и получение product_id
                snowflake.productId = insertOrGetProduct(connection, data);

                // Вставка в dim_stores и получение store_id
                snowflake.storeId = insertOrGetStore(connection, data);

                // Вставка в dim_suppliers и получение supplier_id
                snowflake.supplierId = insertOrGetSupplier(connection, data);

                // Сохранение данных для фактовой таблицы
                snowflake.saleDate = data.sale_date;
                snowflake.quantity = data.sale_quantity;
                snowflake.totalPrice = data.sale_total_price;
                if (data.sale_quantity != 0) {
                    snowflake.unitPrice = data.sale_total_price.divide(BigDecimal.valueOf(data.sale_quantity), 2, BigDecimal.ROUND_HALF_UP);
                } else {
                    snowflake.unitPrice = BigDecimal.ZERO;
                }

                return snowflake;
            }

            @Override
            public void close() throws Exception {
                if (connection != null) {
                    connection.close();
                }
            }
        });

        // Вставка в фактовую таблицу
        snowflakeStream.addSink(JdbcSink.sink(
            "INSERT INTO fact_sales (customer_id, seller_id, product_id, store_id, supplier_id, sale_date, quantity, total_price, unit_price) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (statement, snowflake) -> {
                statement.setInt(1, snowflake.customerId);
                statement.setInt(2, snowflake.sellerId);
                statement.setInt(3, snowflake.productId);
                statement.setInt(4, snowflake.storeId);
                statement.setInt(5, snowflake.supplierId);
                statement.setDate(6, java.sql.Date.valueOf(snowflake.saleDate));
                statement.setInt(7, snowflake.quantity);
                statement.setBigDecimal(8, snowflake.totalPrice);
                statement.setBigDecimal(9, snowflake.unitPrice);
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://highload_db:5432/highload_db")
                .withDriverName("org.postgresql.Driver")
                .withUsername("postgres")
                .withPassword("highload")
                .build()
        ));

        env.execute("Kafka to Snowflake Model Job");
    }

    // Методы для работы с измерениями
    private static int insertOrGetCustomer(Connection connection, MockData data) throws SQLException {
        String insertSql = "INSERT INTO dim_customers (first_name, last_name, age, email, country, postal_code) " +
                         "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (first_name, last_name, email, age, country) DO NOTHING";

        try (PreparedStatement insertStmt = connection.prepareStatement(insertSql)) {
            insertStmt.setString(1, data.customer_first_name);
            insertStmt.setString(2, data.customer_last_name);
            insertStmt.setInt(3, data.customer_age);
            insertStmt.setString(4, data.customer_email);
            insertStmt.setString(5, data.customer_country);
            insertStmt.setString(6, data.customer_postal_code);
            insertStmt.executeUpdate();
        }

        String selectSql = "SELECT customer_id FROM dim_customers WHERE first_name = ? AND last_name = ? AND email = ? AND age = ? AND country = ?";
        try (PreparedStatement selectStmt = connection.prepareStatement(selectSql)) {
            selectStmt.setString(1, data.customer_first_name);
            selectStmt.setString(2, data.customer_last_name);
            selectStmt.setString(3, data.customer_email);
            selectStmt.setInt(4, data.customer_age);
            selectStmt.setString(5, data.customer_country);
            ResultSet rs = selectStmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("customer_id");
            }
        }
        throw new SQLException("Failed to get customer_id");
    }

    // Новый метод для вставки в dim_pets
    private static int insertOrGetPet(Connection connection, int customerId, MockData data) throws SQLException {
        // Проверяем, есть ли данные о питомце
        if (data.customer_pet_name == null || data.customer_pet_name.trim().isEmpty()) {
            return 0; // или какое-то значение по умолчанию для отсутствующего питомца
        }

        String insertSql = "INSERT INTO dim_pets (customer_id, pet_type, pet_name, pet_breed) " +
                         "VALUES (?, ?, ?, ?) ON CONFLICT (customer_id, pet_name, pet_breed) DO NOTHING";

        try (PreparedStatement insertStmt = connection.prepareStatement(insertSql)) {
            insertStmt.setInt(1, customerId);
            insertStmt.setString(2, data.customer_pet_type);
            insertStmt.setString(3, data.customer_pet_name);
            insertStmt.setString(4, data.customer_pet_breed);
            insertStmt.executeUpdate();
        }

        String selectSql = "SELECT pet_id FROM dim_pets WHERE customer_id = ? AND pet_name = ? AND pet_breed = ?";
        try (PreparedStatement selectStmt = connection.prepareStatement(selectSql)) {
            selectStmt.setInt(1, customerId);
            selectStmt.setString(2, data.customer_pet_name);
            selectStmt.setString(3, data.customer_pet_breed);
            ResultSet rs = selectStmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("pet_id");
            }
        }
        throw new SQLException("Failed to get pet_id");
    }

    private static int insertOrGetSeller(Connection connection, MockData data) throws SQLException {
        String insertSql = "INSERT INTO dim_sellers (first_name, last_name, email, country, postal_code) " +
                         "VALUES (?, ?, ?, ?, ?) ON CONFLICT (first_name, last_name, email, country) DO NOTHING";

        try (PreparedStatement insertStmt = connection.prepareStatement(insertSql)) {
            insertStmt.setString(1, data.seller_first_name);
            insertStmt.setString(2, data.seller_last_name);
            insertStmt.setString(3, data.seller_email);
            insertStmt.setString(4, data.seller_country);
            insertStmt.setString(5, data.seller_postal_code);
            insertStmt.executeUpdate();
        }

        String selectSql = "SELECT seller_id FROM dim_sellers WHERE first_name = ? AND last_name = ? AND email = ? AND country = ?";
        try (PreparedStatement selectStmt = connection.prepareStatement(selectSql)) {
            selectStmt.setString(1, data.seller_first_name);
            selectStmt.setString(2, data.seller_last_name);
            selectStmt.setString(3, data.seller_email);
            selectStmt.setString(4, data.seller_country);
            ResultSet rs = selectStmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("seller_id");
            }
        }
        throw new SQLException("Failed to get seller_id");
    }

    private static int insertOrGetProduct(Connection connection, MockData data) throws SQLException {
        String insertSql = "INSERT INTO dim_products (name, category, price, weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date, pet_category) " +
                         "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (name, category, price, weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date) DO NOTHING";

        try (PreparedStatement insertStmt = connection.prepareStatement(insertSql)) {
            insertStmt.setString(1, data.product_name);
            insertStmt.setString(2, data.product_category);
            insertStmt.setBigDecimal(3, data.product_price);
            insertStmt.setBigDecimal(4, data.product_weight);
            insertStmt.setString(5, data.product_color);
            insertStmt.setString(6, data.product_size);
            insertStmt.setString(7, data.product_brand);
            insertStmt.setString(8, data.product_material);
            insertStmt.setString(9, data.product_description);
            insertStmt.setBigDecimal(10, data.product_rating);
            insertStmt.setInt(11, data.product_reviews);
            insertStmt.setDate(12, java.sql.Date.valueOf(data.product_release_date));
            insertStmt.setDate(13, java.sql.Date.valueOf(data.product_expiry_date));
            insertStmt.setString(14, data.pet_category);
            insertStmt.executeUpdate();
        }

        String selectSql = "SELECT product_id FROM dim_products WHERE name = ? AND category = ? AND price = ? AND weight = ? AND color = ? AND size = ? AND brand = ? AND material = ?";
        try (PreparedStatement selectStmt = connection.prepareStatement(selectSql)) {
            selectStmt.setString(1, data.product_name);
            selectStmt.setString(2, data.product_category);
            selectStmt.setBigDecimal(3, data.product_price);
            selectStmt.setBigDecimal(4, data.product_weight);
            selectStmt.setString(5, data.product_color);
            selectStmt.setString(6, data.product_size);
            selectStmt.setString(7, data.product_brand);
            selectStmt.setString(8, data.product_material);
            ResultSet rs = selectStmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("product_id");
            }
        }
        throw new SQLException("Failed to get product_id");
    }

    private static int insertOrGetStore(Connection connection, MockData data) throws SQLException {
        String insertSql = "INSERT INTO dim_stores (name, location, city, state, country, phone, email) " +
                         "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (name, location, city, country) DO NOTHING";

        try (PreparedStatement insertStmt = connection.prepareStatement(insertSql)) {
            insertStmt.setString(1, data.store_name);
            insertStmt.setString(2, data.store_location);
            insertStmt.setString(3, data.store_city);
            insertStmt.setString(4, data.store_state);
            insertStmt.setString(5, data.store_country);
            insertStmt.setString(6, data.store_phone);
            insertStmt.setString(7, data.store_email);
            insertStmt.executeUpdate();
        }

        String selectSql = "SELECT store_id FROM dim_stores WHERE name = ? AND location = ? AND city = ? AND country = ?";
        try (PreparedStatement selectStmt = connection.prepareStatement(selectSql)) {
            selectStmt.setString(1, data.store_name);
            selectStmt.setString(2, data.store_location);
            selectStmt.setString(3, data.store_city);
            selectStmt.setString(4, data.store_country);
            ResultSet rs = selectStmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("store_id");
            }
        }
        throw new SQLException("Failed to get store_id");
    }

    private static int insertOrGetSupplier(Connection connection, MockData data) throws SQLException {
        String insertSql = "INSERT INTO dim_suppliers (name, contact, email, phone, address, city, country) " +
                         "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (name, contact, email) DO NOTHING";

        try (PreparedStatement insertStmt = connection.prepareStatement(insertSql)) {
            insertStmt.setString(1, data.supplier_name);
            insertStmt.setString(2, data.supplier_contact);
            insertStmt.setString(3, data.supplier_email);
            insertStmt.setString(4, data.supplier_phone);
            insertStmt.setString(5, data.supplier_address);
            insertStmt.setString(6, data.supplier_city);
            insertStmt.setString(7, data.supplier_country);
            insertStmt.executeUpdate();
        }

        String selectSql = "SELECT supplier_id FROM dim_suppliers WHERE name = ? AND contact = ? AND email = ?";
        try (PreparedStatement selectStmt = connection.prepareStatement(selectSql)) {
            selectStmt.setString(1, data.supplier_name);
            selectStmt.setString(2, data.supplier_contact);
            selectStmt.setString(3, data.supplier_email);
            ResultSet rs = selectStmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("supplier_id");
            }
        }
        throw new SQLException("Failed to get supplier_id");
    }

    // Класс для хранения данных из Kafka (JSON версия)
    public static class MockData {
        public int id;
        public String customer_first_name;
        public String customer_last_name;
        public int customer_age;
        public String customer_email;
        public String customer_country;
        public String customer_postal_code;
        public String customer_pet_type;
        public String customer_pet_name;
        public String customer_pet_breed;
        public String seller_first_name;
        public String seller_last_name;
        public String seller_email;
        public String seller_country;
        public String seller_postal_code;
        public String product_name;
        public String product_category;
        public BigDecimal product_price;
        public int product_quantity;
        public LocalDate sale_date;
        public int sale_customer_id;
        public int sale_seller_id;
        public int sale_product_id;
        public int sale_quantity;
        public BigDecimal sale_total_price;
        public String store_name;
        public String store_location;
        public String store_city;
        public String store_state;
        public String store_country;
        public String store_phone;
        public String store_email;
        public String pet_category;
        public BigDecimal product_weight;
        public String product_color;
        public String product_size;
        public String product_brand;
        public String product_material;
        public String product_description;
        public BigDecimal product_rating;
        public int product_reviews;
        public LocalDate product_release_date;
        public LocalDate product_expiry_date;
        public String supplier_name;
        public String supplier_contact;
        public String supplier_email;
        public String supplier_phone;
        public String supplier_address;
        public String supplier_city;
        public String supplier_country;

        public static MockData fromJson(JsonNode jsonNode) {
            MockData data = new MockData();

            // Создаем форматтер для дат в формате "M/d/yyyy"
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("M/d/yyyy");

            data.id = getInt(jsonNode, "id");
            data.customer_first_name = getString(jsonNode, "customer_first_name");
            data.customer_last_name = getString(jsonNode, "customer_last_name");
            data.customer_age = getInt(jsonNode, "customer_age");
            data.customer_email = getString(jsonNode, "customer_email");
            data.customer_country = getString(jsonNode, "customer_country");
            data.customer_postal_code = getString(jsonNode, "customer_postal_code");
            data.customer_pet_type = getString(jsonNode, "customer_pet_type");
            data.customer_pet_name = getString(jsonNode, "customer_pet_name");
            data.customer_pet_breed = getString(jsonNode, "customer_pet_breed");
            data.seller_first_name = getString(jsonNode, "seller_first_name");
            data.seller_last_name = getString(jsonNode, "seller_last_name");
            data.seller_email = getString(jsonNode, "seller_email");
            data.seller_country = getString(jsonNode, "seller_country");
            data.seller_postal_code = getString(jsonNode, "seller_postal_code");
            data.product_name = getString(jsonNode, "product_name");
            data.product_category = getString(jsonNode, "product_category");
            data.product_price = getBigDecimal(jsonNode, "product_price");
            data.product_quantity = getInt(jsonNode, "product_quantity");

            // Парсим даты с правильным форматом
            String saleDateStr = getString(jsonNode, "sale_date");
            data.sale_date = parseDate(saleDateStr, dateFormatter);

            data.sale_customer_id = getInt(jsonNode, "sale_customer_id");
            data.sale_seller_id = getInt(jsonNode, "sale_seller_id");
            data.sale_product_id = getInt(jsonNode, "sale_product_id");
            data.sale_quantity = getInt(jsonNode, "sale_quantity");
            data.sale_total_price = getBigDecimal(jsonNode, "sale_total_price");
            data.store_name = getString(jsonNode, "store_name");
            data.store_location = getString(jsonNode, "store_location");
            data.store_city = getString(jsonNode, "store_city");
            data.store_state = getString(jsonNode, "store_state");
            data.store_country = getString(jsonNode, "store_country");
            data.store_phone = getString(jsonNode, "store_phone");
            data.store_email = getString(jsonNode, "store_email");
            data.pet_category = getString(jsonNode, "pet_category");
            data.product_weight = getBigDecimal(jsonNode, "product_weight");
            data.product_color = getString(jsonNode, "product_color");
            data.product_size = getString(jsonNode, "product_size");
            data.product_brand = getString(jsonNode, "product_brand");
            data.product_material = getString(jsonNode, "product_material");
            data.product_description = getString(jsonNode, "product_description");
            data.product_rating = getBigDecimal(jsonNode, "product_rating");
            data.product_reviews = getInt(jsonNode, "product_reviews");

            // Парсим остальные даты
            String releaseDateStr = getString(jsonNode, "product_release_date");
            data.product_release_date = parseDate(releaseDateStr, dateFormatter);

            String expiryDateStr = getString(jsonNode, "product_expiry_date");
            data.product_expiry_date = parseDate(expiryDateStr, dateFormatter);

            data.supplier_name = getString(jsonNode, "supplier_name");
            data.supplier_contact = getString(jsonNode, "supplier_contact");
            data.supplier_email = getString(jsonNode, "supplier_email");
            data.supplier_phone = getString(jsonNode, "supplier_phone");
            data.supplier_address = getString(jsonNode, "supplier_address");
            data.supplier_city = getString(jsonNode, "supplier_city");
            data.supplier_country = getString(jsonNode, "supplier_country");

            return data;
        }

        private static String getString(JsonNode node, String fieldName) {
            JsonNode field = node.get(fieldName);
            return field != null && !field.isNull() ? field.asText() : "";
        }

        private static int getInt(JsonNode node, String fieldName) {
            JsonNode field = node.get(fieldName);
            return field != null && !field.isNull() ? field.asInt() : 0;
        }

        private static BigDecimal getBigDecimal(JsonNode node, String fieldName) {
            JsonNode field = node.get(fieldName);
            if (field != null && !field.isNull()) {
                try {
                    return new BigDecimal(field.asText());
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing decimal for field " + fieldName + ": " + field.asText());
                }
            }
            return BigDecimal.ZERO;
        }

        private static LocalDate parseDate(String dateStr, DateTimeFormatter formatter) {
            if (dateStr == null || dateStr.trim().isEmpty()) {
                return LocalDate.now();
            }
            try {
                return LocalDate.parse(dateStr.trim(), formatter);
            } catch (Exception e) {
                System.err.println("Error parsing date: " + dateStr);
                // Попробуем другие возможные форматы
                try {
                    return LocalDate.parse(dateStr.trim(), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                } catch (Exception e2) {
                    try {
                        return LocalDate.parse(dateStr.trim(), DateTimeFormatter.ofPattern("MM/dd/yyyy"));
                    } catch (Exception e3) {
                        System.err.println("All date parsing attempts failed for: " + dateStr);
                        return LocalDate.now();
                    }
                }
            }
        }
    }

    // Класс для хранения данных снежинки
    public static class SnowflakeData {
        public int customerId;
        public int petId; // Добавлено поле для pet_id
        public int sellerId;
        public int productId;
        public int storeId;
        public int supplierId;
        public LocalDate saleDate;
        public int quantity;
        public BigDecimal totalPrice;
        public BigDecimal unitPrice;
    }
}
