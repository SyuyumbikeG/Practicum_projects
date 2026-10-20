import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json,
    to_json,
    col,
    lit,
    struct,
    broadcast,
    current_timestamp,
    unix_timestamp,
)
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Добавляем путь для импорта конфигурации
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import (
    KAFKA_BOOTSTRAP,
    KAFKA_USER,
    KAFKA_PASSWORD,
    TOPIC_IN,
    TOPIC_OUT,
    POSTGRES_HOST_READ,
    POSTGRES_PORT_READ,
    POSTGRES_DB_READ,
    POSTGRES_USER_READ,
    POSTGRES_PASSWORD_READ,
    POSTGRES_HOST_WRITE,
    POSTGRES_PORT_WRITE,
    POSTGRES_DB_WRITE,
    POSTGRES_USER_WRITE,
    POSTGRES_PASSWORD_WRITE,
)

# 1. КОНСТАНТЫ И СХЕМЫ


def get_incoming_message_schema() -> StructType:
    """Возвращает схему входного сообщения из Kafka"""
    return StructType(
        [
            StructField("restaurant_id", StringType(), True),
            StructField("adv_campaign_id", StringType(), True),
            StructField("adv_campaign_content", StringType(), True),
            StructField("adv_campaign_owner", StringType(), True),
            StructField("adv_campaign_owner_contact", StringType(), True),
            StructField("adv_campaign_datetime_start", LongType(), True),
            StructField("adv_campaign_datetime_end", LongType(), True),
            StructField("datetime_created", LongType(), True),
        ]
    )


def get_spark_jars_packages() -> str:
    return ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )


# 2. СПАРК СЕССИЯ


def create_spark_session() -> SparkSession:
    """Создает и возвращает Spark сессию с необходимыми настройками"""
    spark_jars_packages = get_spark_jars_packages()

    return (
        SparkSession.builder.appName("RestaurantSubscribeStreamingService")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", spark_jars_packages)
        .config("spark.jars.repositories", "https://maven.aliyun.com/repository/public")
        .getOrCreate()
    )


# 3. ЧТЕНИЕ ДАННЫХ


def read_stream_from_kafka(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("kafka.security.protocol", "SASL_SSL")
        .option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";',
        )
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("subscribe", TOPIC_IN)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def read_subscribers_from_postgres(spark: SparkSession) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option(
            "url",
            f"jdbc:postgresql://{POSTGRES_HOST_READ}:{POSTGRES_PORT_READ}/{POSTGRES_DB_READ}",
        )
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "subscribers_restaurants")
        .option("user", POSTGRES_USER_READ)
        .option("password", POSTGRES_PASSWORD_READ)
        .load()
    )


# 4. ОБРАБОТКА ДАННЫХ


def parse_and_filter_messages(stream_df: DataFrame) -> DataFrame:
    schema = get_incoming_message_schema()

    parsed_df = (
        stream_df.withColumn("value", col("value").cast("string"))
        .withColumn("data", from_json(col("value"), schema))
        .select(col("data.*"))
    )

    # Фильтруем активные акции через Spark функции
    return parsed_df.filter(
        (col("adv_campaign_datetime_start") <= unix_timestamp(current_timestamp()))
        & (col("adv_campaign_datetime_end") >= unix_timestamp(current_timestamp()))
    )


def join_stream_with_subscribers(
    filtered_stream_df: DataFrame, subscribers_df: DataFrame
) -> DataFrame:
    broadcast_subscribers = broadcast(subscribers_df)

    return filtered_stream_df.join(
        broadcast_subscribers,
        filtered_stream_df.restaurant_id == broadcast_subscribers.restaurant_id,
        "inner",
    ).select(
        filtered_stream_df.restaurant_id,
        filtered_stream_df.adv_campaign_id,
        filtered_stream_df.adv_campaign_content,
        filtered_stream_df.adv_campaign_owner,
        filtered_stream_df.adv_campaign_owner_contact,
        filtered_stream_df.adv_campaign_datetime_start,
        filtered_stream_df.adv_campaign_datetime_end,
        filtered_stream_df.datetime_created,
        broadcast_subscribers.client_id,
    )


# 5. ЗАПИСЬ ДАННЫХ
def write_to_postgres(df: DataFrame) -> None:
    postgres_url = f"jdbc:postgresql://{POSTGRES_HOST_WRITE}:{POSTGRES_PORT_WRITE}/{POSTGRES_DB_WRITE}"

    df.write.format("jdbc").mode("append").option("url", postgres_url).option(
        "driver", "org.postgresql.Driver"
    ).option("dbtable", "subscribers_feedback").option(
        "user", POSTGRES_USER_WRITE
    ).option(
        "password", POSTGRES_PASSWORD_WRITE
    ).save()


def write_to_kafka(df: DataFrame) -> None:
    # Определяем колонки для выходного сообщения
    output_columns = [
        "restaurant_id",
        "adv_campaign_id",
        "adv_campaign_content",
        "adv_campaign_owner",
        "adv_campaign_owner_contact",
        "adv_campaign_datetime_start",
        "adv_campaign_datetime_end",
        "datetime_created",
        "client_id",
        "trigger_datetime_created",
    ]

    kafka_df = df.select(to_json(struct(*output_columns)).alias("value"))

    kafka_df.write.format("kafka").mode("append").option(
        "kafka.bootstrap.servers", KAFKA_BOOTSTRAP
    ).option("kafka.security.protocol", "SASL_SSL").option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";',
    ).option(
        "kafka.sasl.mechanism", "SCRAM-SHA-512"
    ).option(
        "topic", TOPIC_OUT
    ).save()


# 6. FOREACH_BATCH ФУНКЦИЯ


def foreach_batch_function(df: DataFrame, epoch_id: int) -> None:
    # Кешируем микробатч
    df.cache()

    # Добавляем время триггера
    trigger_time = int(datetime.utcnow().timestamp())
    df_with_trigger = df.withColumn("trigger_datetime_created", lit(trigger_time))

    # 1. Запись в PostgreSQL
    df_for_postgres = df_with_trigger.withColumn(
        "feedback", lit(None).cast(StringType())
    )
    write_to_postgres(df_for_postgres)

    # 2. Отправка в Kafka
    write_to_kafka(df_with_trigger)

    # 3. Очистка памяти
    df.unpersist()


# 7. ЗАПУСК СТРИМИНГА


def main() -> None:
    print("=" * 60)
    print("Запуск сервиса потоковой обработки")
    print("=" * 60)

    # Создаем Spark сессию
    print("1. Создание Spark сессии...")
    spark = create_spark_session()

    # Читаем поток из Kafka
    print("2. Чтение потока из Kafka...")
    stream_df = read_stream_from_kafka(spark)

    # Парсим и фильтруем сообщения
    print("3. Парсинг и фильтрация сообщений...")
    filtered_df = parse_and_filter_messages(stream_df)

    # Читаем подписчиков из PostgreSQL
    print("4. Чтение подписчиков из PostgreSQL...")
    subscribers_df = read_subscribers_from_postgres(spark)

    # Выполняем JOIN
    print("5. Выполнение JOIN...")
    joined_df = join_stream_with_subscribers(filtered_df, subscribers_df)

    # Запускаем стриминг
    print("6. Запуск стриминга...")
    query = (
        joined_df.writeStream.foreachBatch(foreach_batch_function)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoint_restaurant_subscribe")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("=" * 60)
    print("Стриминг запущен. Ожидание завершения...")
    print("=" * 60)

    query.awaitTermination()


if __name__ == "__main__":
    main()
