"""
Етап 3 (частина 1): зчитати дані з MySQL-таблиці athlete_event_results
і записати їх у Kafka-топік athlete_event_results у форматі JSON.

Запуск: python produce_event_results.py
"""
from pyspark.sql import functions as F

from config import INPUT_TOPIC, get_spark, jdbc_read_options, spark_kafka_options

spark = get_spark("FP_Produce_Event_Results")

# Етап 3.а): читання athlete_event_results з MySQL
events_df = spark.read.format("jdbc").options(**jdbc_read_options("athlete_event_results")).load()
print(f"Зчитано рядків з MySQL athlete_event_results: {events_df.count()}")
events_df.show(5, truncate=False)

# Етап 3.б): серіалізація кожного рядка в JSON і запис у Kafka-топік
# Усі поля приводимо до string, щоб схема на боці споживача була простою та стабільною.
kafka_df = events_df.select(
    F.col("athlete_id").cast("string").alias("key"),
    F.to_json(F.struct(*[F.col(c).cast("string").alias(c) for c in events_df.columns])).alias("value"),
)

(
    kafka_df.write.format("kafka")
    .options(**spark_kafka_options())
    .option("topic", INPUT_TOPIC)
    .save()
)

print(f"Дані записано в Kafka-топік '{INPUT_TOPIC}'")
spark.stop()
