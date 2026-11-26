"""
Частина 1. End-to-End Streaming Pipeline.

Етапи 1, 2, 3 (читання з Kafka), 4, 5, 6.
Перед запуском виконайте create_topics.py та produce_event_results.py.

Запуск: python streaming_pipeline.py   (зупинка — Ctrl+C)
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from config import (
    CHECKPOINT_DIR,
    INPUT_TOPIC,
    JDBC_OUTPUT_URL,
    MYSQL_DRIVER,
    MYSQL_OUTPUT_TABLE,
    MYSQL_PASSWORD,
    MYSQL_USER,
    OUTPUT_TOPIC,
    get_spark,
    jdbc_read_options,
    spark_kafka_options,
)

spark = get_spark("FP_Streaming_Pipeline")

# --------------------------------------------------------------------------
# Етап 1: зчитування фізичних показників атлетів з MySQL olympic_dataset.athlete_bio
# --------------------------------------------------------------------------
bio_raw_df = spark.read.format("jdbc").options(**jdbc_read_options("athlete_bio")).load()

# --------------------------------------------------------------------------
# Етап 2: фільтрація — відкидаємо рядки, де зріст/вага порожні або не є числами.
# cast("double") повертає NULL для порожніх рядків і нечислових значень.
# --------------------------------------------------------------------------
bio_df = (
    bio_raw_df
    .withColumn("height", F.col("height").cast("double"))
    .withColumn("weight", F.col("weight").cast("double"))
    .filter(F.col("height").isNotNull() & F.col("weight").isNotNull())
    # country_noc беремо з результатів змагань, тож тут прибираємо, щоб уникнути дубля колонки
    .select("athlete_id", "sex", "height", "weight")
)
print(f"Атлетів з коректними зростом і вагою: {bio_df.count()}")

# --------------------------------------------------------------------------
# Етап 3: читання результатів змагань з Kafka-топіку athlete_event_results
# та перетворення JSON → DataFrame (кожне поле JSON — окрема колонка).
# --------------------------------------------------------------------------
event_schema = StructType([
    StructField("edition", StringType()),
    StructField("edition_id", StringType()),
    StructField("country_noc", StringType()),
    StructField("sport", StringType()),
    StructField("event", StringType()),
    StructField("result_id", StringType()),
    StructField("athlete", StringType()),
    StructField("athlete_id", StringType()),
    StructField("pos", StringType()),
    StructField("medal", StringType()),
    StructField("isTeamSport", StringType()),
])

kafka_stream_df = (
    spark.readStream.format("kafka")
    .options(**spark_kafka_options())
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "50000")
    .option("failOnDataLoss", "false")
    .load()
)

events_stream_df = (
    kafka_stream_df
    .select(F.from_json(F.col("value").cast("string"), event_schema).alias("data"))
    .select("data.*")
    .withColumn("athlete_id", F.col("athlete_id").cast("int"))
    # відсутність медалі позначаємо явно, щоб група була читабельною
    .withColumn(
        "medal",
        F.when(F.col("medal").isNull() | (F.trim(F.col("medal")) == "") | (F.lower(F.col("medal")) == "nan"),
               F.lit("No medal")).otherwise(F.col("medal")),
    )
)

# --------------------------------------------------------------------------
# Етап 4: об'єднання стріму результатів з біологічними даними за athlete_id
# --------------------------------------------------------------------------
joined_df = events_stream_df.join(bio_df, on="athlete_id", how="inner")

# --------------------------------------------------------------------------
# Етап 5: середній зріст і вага для кожної комбінації sport, medal, sex, country_noc
# + timestamp, коли розрахунок було зроблено.
# --------------------------------------------------------------------------
agg_df = (
    joined_df
    .groupBy("sport", "medal", "sex", "country_noc")
    .agg(
        F.round(F.avg("height"), 2).alias("avg_height"),
        F.round(F.avg("weight"), 2).alias("avg_weight"),
    )
    .withColumn("timestamp", F.current_timestamp())
)


# --------------------------------------------------------------------------
# Етап 6: стрим даних за допомогою foreachBatch у Kafka-топік і в базу даних
# --------------------------------------------------------------------------
def foreach_batch_function(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    batch_df = batch_df.persist()
    print(f"===== Batch {batch_id}: {batch_df.count()} рядків =====")
    batch_df.orderBy("sport", "medal").show(10, truncate=False)

    # Етап 6.а): запис у вихідний Kafka-топік
    (
        batch_df
        .select(F.to_json(F.struct(*batch_df.columns)).alias("value"))
        .write.format("kafka")
        .options(**spark_kafka_options())
        .option("topic", OUTPUT_TOPIC)
        .save()
    )

    # Етап 6.b): запис у базу даних MySQL
    (
        batch_df.write.format("jdbc")
        .option("url", JDBC_OUTPUT_URL)
        .option("driver", MYSQL_DRIVER)
        .option("dbtable", MYSQL_OUTPUT_TABLE)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .mode("append")
        .save()
    )

    batch_df.unpersist()


query = (
    agg_df.writeStream
    .outputMode("complete")  # агрегація без watermark → complete
    .foreachBatch(foreach_batch_function)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/streaming_pipeline")
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()
