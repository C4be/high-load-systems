FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

# Создаем топик при запуске
# COPY create_topic.py .
# COPY kafka_producer.py .

CMD ["python", "kafka_producer.py"]