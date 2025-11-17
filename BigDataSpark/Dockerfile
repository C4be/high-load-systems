FROM apache/spark:latest

USER root

# Установка Python
RUN apt-get update && \
    apt-get install -y python3 python3-pip curl && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    ln -sf /usr/bin/pip3 /usr/bin/pip

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# --- 1. Устанавливаем только requirements.txt (кэшируемый слой) ---
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# --- 2. Добавляем тяжёлые JARы (часто меняются) ---
# Добавление JDBC-драйверов
RUN curl -L -o /opt/spark/jars/postgresql-42.7.4.jar \
      https://jdbc.postgresql.org/download/postgresql-42.7.4.jar
      
# Clickhouse
RUN curl -L -o /opt/spark/jars/clickhouse-jdbc-0.6.0.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0/clickhouse-jdbc-0.6.0.jar

# Apache HttpComponents — необходимые зависимости ClickHouse JDBC
RUN curl -L -o /opt/spark/jars/httpcore5-5.2.1.jar \
    https://repo1.maven.org/maven2/org/apache/httpcomponents.core5/httpcore5/5.2.1/httpcore5-5.2.1.jar

RUN curl -L -o /opt/spark/jars/httpclient5-5.2.1.jar \
    https://repo1.maven.org/maven2/org/apache/httpcomponents.client5/httpclient5/5.2.1/httpclient5-5.2.1.jar

RUN curl -L -o /opt/spark/jars/spark-cassandra-connector_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.5.0/spark-cassandra-connector_2.12-3.5.0.jar

RUN curl -L -o /opt/spark/jars/mongo-spark-connector_2.12-10.3.0.jar \
      https://repo1.maven.org/maven2/com/mongodb/spark/mongo-spark-connector_2.12/10.3.0/mongo-spark-connector_2.12-10.3.0.jar

RUN curl -L -o /opt/spark/jars/neo4j-connector-apache-spark_2.12-5.3.0.jar \
      https://repo1.maven.org/maven2/org/neo4j/neo4j-connector-apache-spark_2.12/5.3.0/neo4j-connector-apache-spark_2.12-5.3.0.jar

RUN curl -L -o /opt/spark/jars/spark-redis_2.12-3.1.0.jar \
      https://repo1.maven.org/maven2/com/redislabs/spark-redis_2.12/3.1.0/spark-redis_2.12-3.1.0.jar

# --- 3. Только теперь копируем приложение (самый низкий слой) ---
WORKDIR /app
COPY . /app

EXPOSE 8888
