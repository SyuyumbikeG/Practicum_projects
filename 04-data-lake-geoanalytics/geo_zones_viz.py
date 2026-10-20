import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import math


def main():
    if len(sys.argv) < 4:
        print("""
        Usage: spark-submit geo_zones_viz.py <events_path> <cities_path> <output_path>
        """)
        sys.exit(1)

    events_path = sys.argv[1]
    cities_path = sys.argv[2]
    output_path = sys.argv[3]

    spark = (
        SparkSession.builder.appName("GeoZonesVisits")
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "104857600")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    print("Загружаем данные")

    # 1. Загружаем города
    cities_df = spark.read.parquet(cities_path)
    cities = cities_df.select(
        F.col("city"), F.col("lat").alias("city_lat"), F.col("lng").alias("city_lon")
    )

    # 2. Загружаем события (май 2022 + sample + limit)
    events = (
        spark.read.parquet(events_path)
        .filter(F.col("date").between("2022-05-01", "2022-05-31"))
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
        .sample(0.1)
        .limit(1000)
        .select("*", F.monotonically_increasing_id().alias("row_id"))
    )

    print(f"Событий для расчёта: {events.count()}")

    if events.count() == 0:
        print("Нет данных!")
        spark.stop()
        return

    # 3. Определяем город через Broadcast Join
    print("Определяем город")
    cross = events.crossJoin(F.broadcast(cities))

    lat_rad = F.radians(F.col("lat"))
    lon_rad = F.radians(F.col("lon"))
    city_lat_rad = F.radians(F.col("city_lat"))
    city_lon_rad = F.radians(F.col("city_lon"))

    delta_lat = (city_lat_rad - lat_rad) / 2
    delta_lon = (city_lon_rad - lon_rad) / 2

    a = F.pow(F.sin(delta_lat), 2) + F.cos(lat_rad) * F.cos(city_lat_rad) * F.pow(
        F.sin(delta_lon), 2
    )

    distance = F.lit(2) * F.lit(6371) * F.asin(F.sqrt(a))

    window_city = Window.partitionBy("row_id").orderBy(distance.asc())

    events_with_city = (
        cross.withColumn("rank", F.row_number().over(window_city))
        .filter(F.col("rank") == 1)
        .drop("rank", "city_lat", "city_lon", "row_id")
        .withColumnRenamed("city", "city_name")
    )

    print(f"Город определён для {events_with_city.count()} событий")

    # 4. Добавляем неделю и месяц
    events_with_date = events_with_city.withColumn(
        "week",
        F.concat(
            F.year("date").cast("string"),
            F.lit("-"),
            F.weekofyear("date").cast("string"),
        ),
    ).withColumn("month", F.date_format("date", "yyyy-MM"))

    # 5. Агрегация по неделям
    print("Агрегируем по неделям")
    weekly_stats = events_with_date.groupBy("month", "week", "city_name").agg(
        F.count(F.when(F.col("event_type") == "message", 1)).alias("week_message"),
        F.count(F.when(F.col("event_type") == "reaction", 1)).alias("week_reaction"),
        F.count(F.when(F.col("event_type") == "subscription", 1)).alias(
            "week_subscription"
        ),
        F.countDistinct(
            F.when(F.col("event_type") == "message", F.col("event.message_from"))
        ).alias("week_user"),
    )

    # 6. Добавляем месячные суммы
    print("Добавляем месячные суммы...")
    window_month = Window.partitionBy("month", "city_name")

    monthly_stats = (
        weekly_stats.withColumn(
            "month_message", F.sum("week_message").over(window_month)
        )
        .withColumn("month_reaction", F.sum("week_reaction").over(window_month))
        .withColumn("month_subscription", F.sum("week_subscription").over(window_month))
        .withColumn("month_user", F.sum("week_user").over(window_month))
    )

    # 7. Сохраняем
    print(f"Сохраняем в {output_path}")
    monthly_stats.write.mode("overwrite").partitionBy("month").parquet(output_path)

    print(f"Результат сохранён: {output_path}")
    print(f"   Количество записей: {monthly_stats.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
