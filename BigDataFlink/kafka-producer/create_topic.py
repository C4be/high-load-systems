from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import time


def create_topic():
    # Ждем пока Kafka станет доступна
    for i in range(30):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=['kafka:9092'],
                client_id='topic_creator'
            )

            topic_list = [
                NewTopic(
                    name="mock_data_topic",
                    num_partitions=3,
                    replication_factor=1
                )
            ]

            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print("Topic 'mock_data_topic' created successfully")
            break
        except Exception as e:
            print(f"Attempt {i + 1}: Kafka not ready yet - {e}")
            time.sleep(5)


if __name__ == "__main__":
    create_topic()