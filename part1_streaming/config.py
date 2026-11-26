"""
Спільна конфігурація для Частини 1 (Streaming Pipeline).

Усі логіни/паролі/адреси беруться зі змінних оточення (файл .env у корені проєкту).
"""
import os
from pathlib import Path

from dotenv import load_dotenv

# .env лежить у корені репозиторію (на рівень вище від part1_streaming)
load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Змінна оточення {name} не задана. Заповніть файл .env")
    return value


# ---------------------------------------------------------------- MySQL ----
MYSQL_HOST = _env("MYSQL_HOST")
MYSQL_PORT = _env("MYSQL_PORT", "3306")
MYSQL_USER = _env("MYSQL_USER")
MYSQL_PASSWORD = _env("MYSQL_PASSWORD")
MYSQL_DB = _env("MYSQL_DB", "olympic_dataset")
# БД, куди пишемо результат (може збігатися з MYSQL_DB, якщо є права на запис)
MYSQL_OUTPUT_DB = os.getenv("MYSQL_OUTPUT_DB") or MYSQL_DB
MYSQL_OUTPUT_TABLE = _env("MYSQL_OUTPUT_TABLE", "athlete_enriched_avg")

MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"
JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
JDBC_OUTPUT_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_OUTPUT_DB}"

# ---------------------------------------------------------------- Kafka ----
KAFKA_BOOTSTRAP_SERVERS = _env("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")

# Префікс (наприклад, ваше ім'я), щоб не конфліктувати з іншими студентами на спільному Kafka
_prefix = os.getenv("KAFKA_TOPIC_PREFIX", "").strip()
_p = f"{_prefix}_" if _prefix else ""
INPUT_TOPIC = f"{_p}athlete_event_results"
OUTPUT_TOPIC = f"{_p}athlete_enriched_agg"

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "checkpoint")


def spark_kafka_options() -> dict:
    """Опції для Spark readStream/write format('kafka')."""
    opts = {"kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
    if KAFKA_SECURITY_PROTOCOL.upper() != "PLAINTEXT":
        opts.update({
            "kafka.security.protocol": KAFKA_SECURITY_PROTOCOL,
            "kafka.sasl.mechanism": KAFKA_SASL_MECHANISM,
            "kafka.sasl.jaas.config": (
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
            ),
        })
    return opts


def kafka_python_config() -> dict:
    """Опції для бібліотеки kafka-python (admin / consumer)."""
    cfg = {"bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS.split(",")}
    if KAFKA_SECURITY_PROTOCOL.upper() != "PLAINTEXT":
        cfg.update({
            "security_protocol": KAFKA_SECURITY_PROTOCOL,
            "sasl_mechanism": KAFKA_SASL_MECHANISM,
            "sasl_plain_username": KAFKA_USERNAME,
            "sasl_plain_password": KAFKA_PASSWORD,
        })
    return cfg


def jdbc_read_options(table: str) -> dict:
    return {
        "url": JDBC_URL,
        "driver": MYSQL_DRIVER,
        "dbtable": table,
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
    }


def get_spark(app_name: str):
    """SparkSession з підключеними пакетами MySQL JDBC та Spark-Kafka."""
    import pyspark
    from pyspark.sql import SparkSession

    spark_ver = pyspark.__version__
    packages = ",".join([
        f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_ver}",
        "com.mysql:mysql-connector-j:8.0.33",
    ])
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", packages)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
