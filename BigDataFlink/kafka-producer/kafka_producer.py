import json
import time
import pandas as pd
from kafka import KafkaProducer
import glob
import os


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

    # Читаем все CSV файлы из папки data
    data_dir = '/app/data'
    csv_files = glob.glob(os.path.join(data_dir, 'MOCK_DATA*.csv'))

    if not csv_files:
        print(f"No CSV files found in {data_dir}")
        return

    total_records = 0

    for file_path in csv_files:
        print(f"Processing file: {file_path}")

        try:
            # Читаем CSV файл
            df = pd.read_csv(file_path)

            # Преобразуем каждую строку в JSON и отправляем в Kafka
            for _, row in df.iterrows():
                record = row.to_dict()

                # Обработка NaN значений
                record = {k: (v if pd.notna(v) else None) for k, v in record.items()}

                # Конвертируем типы для JSON сериализации
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif isinstance(value, (pd.Timestamp, pd._libs.tslibs.timestamps.Timestamp)):
                        record[key] = value.strftime('%Y-%m-%d')
                    elif isinstance(value, (float, int)) and pd.isna(value):
                        record[key] = None

                # Отправка в Kafka
                producer.send(topic_name, value=record)
                total_records += 1

                # Выводим прогресс каждые 100 записей
                if total_records % 100 == 0:
                    print(f"Sent {total_records} records...")

                # Небольшая задержка для эмуляции реального потока
                time.sleep(0.01)

            print(f"Finished processing {file_path}")

        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            continue

    producer.flush()
    print(f"All data sent to Kafka. Total records: {total_records}")


if __name__ == "__main__":
    
    from create_topic import create_topic

    create_topic()

    read_and_send_data()