"""
Етап 1. Landing → Bronze.

- завантажує CSV з FTP-сервера в оригінальному форматі (папка landing/),
- читає CSV за допомогою Spark і зберігає у форматі parquet у bronze/{table}.
"""
import os

import requests
from pyspark.sql import SparkSession

BASE_URL = "https://ftp.goit.study/neoversity/"
TABLES = ["athlete_bio", "athlete_event_results"]
LANDING_DIR = "landing"
BRONZE_DIR = "bronze"


def download_data(table: str) -> str:
    """Завантажує {table}.csv з FTP і повертає локальний шлях до файлу."""
    url = f"{BASE_URL}{table}.csv"
    os.makedirs(LANDING_DIR, exist_ok=True)
    local_path = os.path.join(LANDING_DIR, f"{table}.csv")

    print(f"Downloading from {url}")
    response = requests.get(url, timeout=120)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to download {url}. Status code: {response.status_code}")

    with open(local_path, "wb") as f:
        f.write(response.content)
    print(f"File saved to {local_path}")
    return local_path


def main() -> None:
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    for table in TABLES:
        csv_path = download_data(table)

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .option("escape", '"')
            .csv(csv_path)
        )

        output_path = os.path.join(BRONZE_DIR, table)
        df.write.mode("overwrite").parquet(output_path)
        print(f"Saved bronze/{table}: {df.count()} rows")

        # Фінальний DataFrame для скриншоту
        spark.read.parquet(output_path).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
