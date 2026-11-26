"""
Крок 0. Створення Kafka-топіків: вхідного (результати змагань) і вихідного (агрегати).
Запуск: python create_topics.py
"""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config import INPUT_TOPIC, OUTPUT_TOPIC, kafka_python_config

admin = KafkaAdminClient(client_id="fp_topic_creator", **kafka_python_config())

for name in (INPUT_TOPIC, OUTPUT_TOPIC):
    try:
        admin.create_topics([NewTopic(name=name, num_partitions=2, replication_factor=1)])
        print(f"Топік '{name}' створено")
    except TopicAlreadyExistsError:
        print(f"Топік '{name}' вже існує")

print("Наші топіки:", [t for t in admin.list_topics() if t in (INPUT_TOPIC, OUTPUT_TOPIC)])
admin.close()
