"""
Перевірка результату: читає вихідний Kafka-топік і друкує повідомлення на екран
(для скриншоту «принт даних з Kafka-топіку»).

Запуск: python consume_output.py   (зупинка — Ctrl+C)
"""
import json

from kafka import KafkaConsumer

from config import OUTPUT_TOPIC, kafka_python_config

consumer = KafkaConsumer(
    OUTPUT_TOPIC,
    **kafka_python_config(),
    group_id="fp_output_reader",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"Читаємо топік '{OUTPUT_TOPIC}'...")
try:
    for msg in consumer:
        print(msg.value)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
