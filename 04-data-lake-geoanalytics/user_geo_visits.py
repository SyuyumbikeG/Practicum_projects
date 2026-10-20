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
        Usage: spark-submit user_geo_visits.py <events_path> <cities_path> <output_path>
        """)
        sys.exit(1)

    events_path = sys.argv[1]
    cities_path = sys.argv[2]
    output_path = sys.argv[3]

    spark = (
        SparkSession.builder.appName("UserGeoVisits")
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    print("Загружаем данные")

    # 1. Загружаем города
    cities_df = spark.read.parquet(cities_path)
    cities_list = [(row.city, row.lat, row.lng) for row in cities_df.collect()]
    print(f"   ✅ Городов: {len(cities_list)}")

    # 2. UDF
    def find_city(lat, lon):
        if lat is None or lon is None:
            return None
        min_dist = float("inf")
        nearest = None
        for city, c_lat, c_lon in cities_list:
            lat_rad = math.radians(lat)
            lon_rad = math.radians(lon)
            c_lat_rad = math.radians(c_lat)
            c_lon_rad = math.radians(c_lon)
            dlat = (c_lat_rad - lat_rad) / 2
            dlon = (c_lon_rad - lon_rad) / 2
            a = (
                math.sin(dlat) ** 2
                + math.cos(lat_rad) * math.cos(c_lat_rad) * math.sin(dlon) ** 2
            )
            dist = 2 * 6371 * math.asin(math.sqrt(a))
            if dist < min_dist:
                min_dist = dist
                nearest = city
        return nearest

    find_city_udf = udf(find_city, StringType())

    # 3. Загружаем события
    events = (
        spark.read.parquet(events_path)
        .filter(F.col("date").between("2022-05-01", "2022-05-31"))
        .filter(F.col("event_type") == "message")
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
        .sample(0.1)
        .limit(1000)
    )

    print(f"   ✅ Событий для расчёта: {events.count()}")

    if events.count() == 0:
        print("Нет данных!")
        spark.stop()
        return

    # 4. Определяем город
    print("Определяем город")
    events_with_city = events.withColumn(
        "city_name", find_city_udf(F.col("lat"), F.col("lon"))
    )

    # 5. act_city (используем event.message_from!)
    print("act_city")
    window_last = Window.partitionBy("event.message_from").orderBy(
        F.desc("event.message_ts")
    )
    act_city = (
        events_with_city.withColumn("rank", F.row_number().over(window_last))
        .filter(F.col("rank") == 1)
        .select(
            F.col("event.message_from").alias("user_id"),
            F.col("city_name").alias("act_city"),
        )
    )

    # 6. home_city
    print("home_city")
    messages_daily = events_with_city.select(
        F.col("event.message_from").alias("user_id"),
        F.col("city_name"),
        F.to_date(F.col("event.message_ts")).alias("event_date"),
    ).distinct()

    window_streak = Window.partitionBy("user_id", "city_name").orderBy("event_date")
    messages_with_streak = (
        messages_daily.withColumn("prev_date", F.lag("event_date").over(window_streak))
        .withColumn("day_diff", F.datediff(F.col("event_date"), F.col("prev_date")))
        .withColumn(
            "streak_id",
            F.sum(F.when(F.col("day_diff") > 1, 1).otherwise(0)).over(window_streak),
        )
    )

    home_city = (
        messages_with_streak.groupBy("user_id", "city_name", "streak_id")
        .agg(F.count("*").alias("days"))
        .filter(F.col("days") >= 27)
        .groupBy("user_id")
        .agg(F.first("city_name").alias("home_city"))
    )

    # 7. travel
    print("travel")
    window_travel = Window.partitionBy("user_id").orderBy("event_date")
    travel_data = (
        messages_daily.withColumn("prev_city", F.lag("city_name").over(window_travel))
        .withColumn(
            "is_new_city",
            F.when(F.col("city_name") != F.col("prev_city"), 1).otherwise(0),
        )
        .filter(F.col("is_new_city") == 1)
    )

    travel_stats = travel_data.groupBy("user_id").agg(
        F.count("*").alias("travel_count"),
        F.collect_list("city_name").alias("travel_array"),
    )

    # 8. Объединяем
    print("Объединяем")
    result = (
        act_city.join(home_city, "user_id", "left")
        .join(travel_stats, "user_id", "left")
        .fillna({"home_city": "Unknown", "travel_count": 0})
    )

    result = result.withColumn(
        "travel_array",
        F.when(F.col("travel_array").isNull(), F.array()).otherwise(
            F.col("travel_array")
        ),
    )

    result = result.withColumn("date", F.current_date())

    # 9. Сохраняем
    print(f"Сохраняем в {output_path}")
    result.write.mode("overwrite").partitionBy("date").parquet(output_path)

    print(f"езультат сохранён: {output_path}")
    print(f"Количество записей: {result.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
