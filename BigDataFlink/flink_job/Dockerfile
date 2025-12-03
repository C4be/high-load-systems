FROM maven:3.8.4-openjdk-11 as builder

WORKDIR /app
COPY pom.xml .

RUN mvn dependency:go-offline

COPY src ./src/

# Скачиваем зависимости и собираем проект

RUN mvn clean package -DskipTests

# Финальный образ
FROM flink:1.17.1-java11

# Устанавливаем netcat для health checks
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

# Копируем собранный JAR из builder stage
COPY --from=builder /app/target/flink-kafka-postgres-1.0-SNAPSHOT.jar /opt/flink/usrlib/

# Добавляем PostgreSQL драйвер
RUN wget -P /opt/flink/lib/ https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Создаем директорию для пользовательских JAR файлов если не существует
RUN mkdir -p /opt/flink/usrlib


# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD nc -z localhost 6123 || exit 1