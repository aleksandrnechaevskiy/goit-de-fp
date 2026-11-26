"""
Етап 2. Bronze → Silver.

- зчитує таблицю з bronze/{table},
- чистить текст у всіх текстових колонках,
- робить дедублікацію рядків,
- записує результат у silver/{table}.
"""
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

TABLES = ["athlete_bio", "athlete_event_results"]
BRONZE_DIR = "bronze"
SILVER_DIR = "silver"


def clean_text(text):
    """Залишає лише латинські літери, цифри та символи , . \\ " '"""
    if text is None:
        return None
    return re.sub(r"[^a-zA-Z0-9,.\\\"\' ]", "", str(text)).strip()


clean_text_udf = F.udf(clean_text, StringType())


def main() -> None:
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    for table in TABLES:
        df = spark.read.parquet(os.path.join(BRONZE_DIR, table))

        # Чистка тексту для всіх колонок типу string
        text_columns = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
        for col_name in text_columns:
            df = df.withColumn(col_name, clean_text_udf(F.col(col_name)))

        # Дедублікація
        rows_before = df.count()
        df = df.dropDuplicates()
        print(f"{table}: {rows_before} → {df.count()} rows after deduplication")

        output_path = os.path.join(SILVER_DIR, table)
        df.write.mode("overwrite").parquet(output_path)
        print(f"Saved silver/{table}")

        # Фінальний DataFrame для скриншоту
        spark.read.parquet(output_path).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
