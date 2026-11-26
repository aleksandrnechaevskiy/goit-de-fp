"""
Етап 3. Silver → Gold.

- зчитує silver/athlete_bio та silver/athlete_event_results,
- робить join за athlete_id,
- для кожної комбінації sport, medal, sex, country_noc рахує середні weight і height,
- додає колонку timestamp з часом виконання,
- записує в gold/avg_stats.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_DIR = "silver"
GOLD_DIR = "gold"


def main() -> None:
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    bio_df = spark.read.parquet(os.path.join(SILVER_DIR, "athlete_bio"))
    events_df = spark.read.parquet(os.path.join(SILVER_DIR, "athlete_event_results"))

    # Зріст і вага → числа; нечислові/порожні значення відкидаємо.
    # country_noc є в обох таблицях — беремо його з результатів змагань.
    bio_df = (
        bio_df
        .withColumn("height", F.col("height").cast("double"))
        .withColumn("weight", F.col("weight").cast("double"))
        .filter(F.col("height").isNotNull() & F.col("weight").isNotNull())
        .select("athlete_id", "sex", "height", "weight")
    )

    events_df = events_df.select("athlete_id", "sport", "medal", "country_noc")

    joined_df = events_df.join(bio_df, on="athlete_id", how="inner")

    gold_df = (
        joined_df
        .groupBy("sport", "medal", "sex", "country_noc")
        .agg(
            F.round(F.avg("weight"), 2).alias("avg_weight"),
            F.round(F.avg("height"), 2).alias("avg_height"),
        )
        .withColumn("timestamp", F.current_timestamp())
    )

    output_path = os.path.join(GOLD_DIR, "avg_stats")
    gold_df.write.mode("overwrite").parquet(output_path)
    print(f"Saved gold/avg_stats: {gold_df.count()} rows")

    # Фінальний DataFrame для скриншоту
    spark.read.parquet(output_path).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
