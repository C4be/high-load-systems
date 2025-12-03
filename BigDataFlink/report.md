# Отчет о выполненной работе - `BigDataFlink`

**Цель работы:** познакомиться с инструментом `Flink`.

## **Структура Проекта**

- `docker-compose.yml` — оркестрация всех сервисов: Zookeeper, Kafka, Kafka UI, PostgreSQL, Flink JobManager/TaskManager, Python‑продюсер Kafka.

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on:
      - kafka
    ports:
      - "8080:8080"

  db:
    image: postgres:15
    container_name: highload_db
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: highload
      POSTGRES_DB: highload_db
    volumes:
      - ./init:/docker-entrypoint-initdb.d

  jobmanager:
    build: ./flink-job
    command: jobmanager
    ports:
      - "8081:8081"

  taskmanager:
    build: ./flink-job
    command: taskmanager

  kafka-producer:
    build: ./kafka-producer
    depends_on:
      - kafka
```

- `flink_job/` — Flink streaming job на Java:
  - `src/main/java/FlinkStreamingJob.java` — основной пайплайн: источник Kafka, парсинг JSON, upsert в измерения, sink в факты.

```java
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("kafka:9092")
    .setTopics("mock_data_topic")
    .setGroupId("flink-group")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();

DataStream<String> kafkaStream = env.fromSource(
    source,
    WatermarkStrategy.noWatermarks(),
    "Kafka Source"
);
```

```java
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
```

```java
snowflakeStream.addSink(JdbcSink.sink(
    "INSERT INTO fact_sales (customer_id, seller_id, product_id, store_id, supplier_id, sale_date, quantity, total_price, unit_price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
```
  - `Dockerfile` — многоэтапная сборка JAR и финальный образ на `flink:1.17.1-java11`.

```dockerfile
FROM maven:3.8.4-openjdk-11 as builder
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src ./src/
RUN mvn clean package -DskipTests

FROM flink:1.17.1-java11
COPY --from=builder /app/target/flink-kafka-postgres-1.0-SNAPSHOT.jar /opt/flink/usrlib/
RUN wget -P /opt/flink/lib/ https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
```

- `kafka-producer/` — Python‑приложение, читающее CSV и шьющее JSON в Kafka:
  - `kafka_producer.py` — создание продюсера, чтение `MOCK_DATA*.csv`, отправка строк в `mock_data_topic`.

```python
from kafka import KafkaProducer
import pandas as pd
import json, glob, os, time

def create_producer():
    return KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        batch_size=16384,
        linger_ms=10,
        compression_type='gzip'
    )

def read_and_send_data():
    producer = create_producer()
    topic_name = 'mock_data_topic'
    data_dir = '/app/data'
    csv_files = glob.glob(os.path.join(data_dir, 'MOCK_DATA*.csv'))

    for file_path in csv_files:
        df = pd.read_csv(file_path)
        for _, row in df.iterrows():
            record = {k: (v if pd.notna(v) else None) for k, v in row.to_dict().items()}
            if isinstance(record.get('sale_date'), (pd.Timestamp, pd._libs.tslibs.timestamps.Timestamp)):
                record['sale_date'] = record['sale_date'].strftime('%Y-%m-%d')
            producer.send(topic_name, value=record)
            time.sleep(0.01)

    producer.flush()
```

  - `create_topic.py` — создание топика `mock_data_topic` (3 партиции) с ретраями при старте.

```python
from kafka.admin import KafkaAdminClient, NewTopic
import time

def create_topic():
    for i in range(30):
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=['kafka:9092'], client_id='topic_creator')
            admin_client.create_topics(new_topics=[NewTopic(name="mock_data_topic", num_partitions=3, replication_factor=1)])
            print("Topic 'mock_data_topic' created successfully")
            break
        except Exception as e:
            print(f"Attempt {i + 1}: Kafka not ready yet - {e}")
            time.sleep(5)
```

  - `requirements.txt` — используемые зависимости.

```text
kafka-python==2.0.2
pandas==1.5.3
numpy==1.21.6
psycopg2-binary==2.9.6
```

  - `data/` — набор CSV: `MOCK_DATA.csv`, `MOCK_DATA (1).csv` … `MOCK_DATA (9).csv`.

- `init/02-DDl.sql` — DDL PostgreSQL: таблицы измерений (`dim_*`) и факт `fact_sales`.

```sql
CREATE TABLE dim_customers (
  customer_id SERIAL PRIMARY KEY,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  age INT,
  email VARCHAR(150),
  country VARCHAR(100),
  postal_code VARCHAR(20),
  UNIQUE(first_name, last_name, email, age, country)
);

CREATE TABLE fact_sales (
  sale_id SERIAL PRIMARY KEY,
  customer_id INT REFERENCES dim_customers(customer_id),
  seller_id INT REFERENCES dim_sellers(seller_id),
  product_id INT REFERENCES dim_products(product_id),
  store_id INT REFERENCES dim_stores(store_id),
  supplier_id INT REFERENCES dim_suppliers(supplier_id),
  sale_date DATE,
  quantity INT,
  total_price DECIMAL(10,2),
  unit_price DECIMAL(10,2)
);
```

- `start.sh` — отправка Flink job через CLI в контейнере `jobmanager`.

```bash
docker exec jobmanager /opt/flink/bin/flink run -d -c FlinkStreamingJob /opt/flink/usrlib/flink-kafka-postgres-1.0-SNAPSHOT.jar
```

- `analys.sql` — проверки качества mock‑данных.

```sql
select count(*) from mock_data;
select count(distinct id) from mock_data;
select distinct pet_category from mock_data;
```

**Инструкция**
- Запуск инфраструктуры:
  - Выполнить `docker compose up -d` в корне проекта.
  - Поднимутся сервисы: Zookeeper, Kafka, Kafka UI (`http://localhost:8080`), PostgreSQL (`highload_db`), Flink (`http://localhost:8081`), Python‑продюсер.
- Запуск Flink‑job:
  - Через UI: открыть `http://localhost:8081`, Upload → выбрать JAR `/opt/flink/usrlib/flink-kafka-postgres-1.0-SNAPSHOT.jar` в контейнере; Main class `FlinkStreamingJob`; запустить.
  - Через CLI:

```bash
docker exec jobmanager /opt/flink/bin/flink run -d -c FlinkStreamingJob /opt/flink/usrlib/flink-kafka-postgres-1.0-SNAPSHOT.jar
```

- Запуск продюсера:
  - Автостарт как сервис `kafka-producer`. Команда контейнера: `python kafka_producer.py`.
  - Создание топика выполняется перед отправкой (`create_topic.py`).
- Проверка работы:
  - Kafka UI: `http://localhost:8080`, кластер `local`, топик `mock_data_topic`.
  - Kafka CLI:

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic mock_data_topic --from-beginning --max-messages 5
```

  - База данных:

```bash
docker exec -it highload_db psql -U postgres -d highload_db -c "SELECT COUNT(*) FROM fact_sales;"
```

## **Ответы На Вопросы**

- Как запускать Flink‑job и продюсер:

```bash
docker exec jobmanager /opt/flink/bin/flink run -d -c FlinkStreamingJob /opt/flink/usrlib/flink-kafka-postgres-1.0-SNAPSHOT.jar
```

```bash
# Продюсер запускается автоматически как контейнер
```

- Что делает Flink streaming код: источник, преобразования, приемники.

```java
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("kafka:9092")
    .setTopics("mock_data_topic")
    .setGroupId("flink-group")
    .build();
```

```java
DataStream<MockData> parsedStream = kafkaStream.map(new MapFunction<String, MockData>() { /* ... */ });
```

```java
snowflakeStream.addSink(JdbcSink.sink("INSERT INTO fact_sales (...) VALUES (...)", /* ... */));
```
