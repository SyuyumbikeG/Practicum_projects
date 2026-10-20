import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main():
    if len(sys.argv) < 3:
        print("""
        Usage: spark-submit friends_recommendations.py <events_path> <output_path>
        """)
        sys.exit(1)

    events_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = (
        SparkSession.builder.appName("FriendsRecommendations")
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    print("Загружаем данные")

    # 1. Загружаем события (май 2022, 5% выборка, limit 1000)
    events = (
        spark.read.parquet(events_path)
        .filter(F.col("date").between("2022-05-01", "2022-05-31"))
        .sample(0.1)
        .limit(1000)
    )

    print(f"Событий для расчёта: {events.count()}")

    if events.count() == 0:
        print("Нет данных!")
        spark.stop()
        return

    # 2. Подписки
    print("Находим подписки")
    subscriptions = (
        events.filter("event_type = 'subscription'")
        .select(
            F.col("event.user").alias("user_id"),
            F.col("event.subscription_channel").alias("channel_id"),
        )
        .distinct()
    )

    print(f"Подписок: {subscriptions.count()}")

    # 3. Пары в одном канале
    print("🔗 Находим пары в одном канале...")
    channel_pairs = (
        subscriptions.alias("s1")
        .join(
            subscriptions.alias("s2"),
            (F.col("s1.channel_id") == F.col("s2.channel_id"))
            & (F.col("s1.user_id") < F.col("s2.user_id")),
            "inner",
        )
        .select(
            F.col("s1.user_id").alias("user_left"),
            F.col("s2.user_id").alias("user_right"),
        )
        .distinct()
    )

    print(f"Пар в одном канале: {channel_pairs.count()}")

    if channel_pairs.count() == 0:
        print("Нет пар в одном канале.")
        from pyspark.sql.types import (
            StructType,
            StructField,
            LongType,
            StringType,
            TimestampType,
        )

        schema = StructType(
            [
                StructField("user_left", LongType(), True),
                StructField("user_right", LongType(), True),
                StructField("processed_dttm", TimestampType(), True),
                StructField("zone_id", StringType(), True),
                StructField("date", StringType(), True),
            ]
        )
        recommendations = spark.createDataFrame([], schema)
    else:
        # 4. Исключаем переписывавшихся
        print("Исключаем переписывавшихся")
        messages = events.filter("event_type = 'message'")

        chat_pairs = (
            messages.select(
                F.col("event.message_from").alias("user1"),
                F.col("event.message_to").alias("user2"),
            )
            .filter("user1 IS NOT NULL AND user2 IS NOT NULL")
            .distinct()
        )

        chat_pairs_normalized = chat_pairs.select(
            F.least("user1", "user2").alias("user_left"),
            F.greatest("user1", "user2").alias("user_right"),
        ).distinct()

        candidates = channel_pairs.join(
            chat_pairs_normalized, ["user_left", "user_right"], "left_anti"
        )

        print(f"Кандидатов: {candidates.count()}")

        # 5. Координаты
        print("Находим координаты...")
        messages_with_coords = events.filter(
            (F.col("event_type") == "message")
            & (F.col("lat").isNotNull())
            & (F.col("lon").isNotNull())
        )

        window_user = Window.partitionBy("event.message_from").orderBy(
            F.desc("event.message_ts")
        )

        user_coords = (
            messages_with_coords.select(
                F.col("event.message_from").alias("user_id"), F.col("lat"), F.col("lon")
            )
            .withColumn("rank", F.row_number().over(window_user))
            .filter(F.col("rank") == 1)
            .select("user_id", "lat", "lon")
        )

        print(f"Пользователей с координатами: {user_coords.count()}")

        # 6. Кандидаты с координатами
        candidates_with_coords = (
            candidates.join(
                user_coords.alias("u1"),
                candidates.user_left == F.col("u1.user_id"),
                "inner",
            )
            .join(
                user_coords.alias("u2"),
                candidates.user_right == F.col("u2.user_id"),
                "inner",
            )
            .select(
                "user_left",
                "user_right",
                F.col("u1.lat").alias("lat1"),
                F.col("u1.lon").alias("lon1"),
                F.col("u2.lat").alias("lat2"),
                F.col("u2.lon").alias("lon2"),
            )
        )

        print(f"Кандидатов с координатами: {candidates_with_coords.count()}")

        # 7. Расстояние
        print("Вычисляем расстояние")
        lat1_rad = F.radians(F.col("lat1"))
        lon1_rad = F.radians(F.col("lon1"))
        lat2_rad = F.radians(F.col("lat2"))
        lon2_rad = F.radians(F.col("lon2"))

        delta_lat = (lat2_rad - lat1_rad) / 2
        delta_lon = (lon2_rad - lon1_rad) / 2

        a = F.pow(F.sin(delta_lat), 2) + F.cos(lat1_rad) * F.cos(lat2_rad) * F.pow(
            F.sin(delta_lon), 2
        )

        distance = F.lit(2) * F.lit(6371) * F.asin(F.sqrt(a))

        recommendations = candidates_with_coords.filter(distance < 1).select(
            "user_left", "user_right"
        )

        print(f"Рекомендаций: {recommendations.count()}")

        recommendations = (
            recommendations.withColumn("processed_dttm", F.current_timestamp())
            .withColumn("zone_id", F.lit("Sydney"))
            .withColumn("date", F.current_date())
        )

    print(f"Сохраняем в {output_path}")
    recommendations.write.mode("overwrite").partitionBy("date").parquet(output_path)

    print(f"Результат сохранён: {output_path}")
    print(f"Количество рекомендаций: {recommendations.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
