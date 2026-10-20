
| Файл | Описание |
|------|----------|
| `README.md` | Описание проекта |
| `user_geo_visits.py` | Скрипт витрины пользователей |
| `geo_zones_viz.py` | Скрипт витрины зон |
| `friends_recommendations.py` | Скрипт витрины рекомендаций |
| `geo_etl_dag.py` | DAG для Airflow |

## Зоны данных

| Зона | Путь | Формат | Описание |
|------|------|--------|----------|
| **Raw** | `/user/master/data/geo/events` | Parquet | Сырые события с геоданными |
| **Staging** | `/user/s18151825/data/events` | JSON | Очищенные события |
| **Analytics** | `/user/s18151825/data/analytics/` | Parquet | Готовые витрины |
| **Geo** | `/user/s18151825/data/geo/cities` | CSV | Геоданные городов |

## Витрины

| Витрина | Обновление | Описание |
|---------|-----------|----------|
| **user_geo_viz** | Ежедневно | act_city, home_city, travel_count, travel_array |
| **geo_zones_viz** | Еженедельно | Статистика по городам (неделя/месяц) |
| **friends_recommendations** | Ежедневно | Пары пользователей для рекомендаций |

## Форматы данных

- **JSON**: Сырые события (сохраняем оригинальную структуру)
- **Parquet**: Аналитические витрины (сжатие, быстрый доступ)
- **CSV**: Справочники (геоданные)

## Скрипты

| Скрипт | Назначение |
|--------|------------|
| `user_geo_visits.py` | Расчёт витрины пользователей |
| `geo_zones_viz.py` | Расчёт статистики по зонам |
| `friends_recommendations.py` | Расчёт рекомендаций друзей |

## Автоматизация (Airflow)

DAG `geo_etl_dag.py` запускает все три скрипта ежедневно в 00:00.

## Результаты

| Витрина | Количество записей |
|---------|-------------------|
| user_geo_viz | 21 |
| geo_zones_viz | 15 |
| friends_recommendations | 0 |


В Jupyter / скриптах используется:

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ProjectName") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

Загрузка geo.csv:

hdfs dfs -mkdir -p /user/s18151825/data/geo/cities
hdfs dfs -copyFromLocal geo.csv /user/s18151825/data/geo/cities/
hdfs dfs -ls /user/s18151825/data/geo/cities/

# Проверка городов
cities_df = spark.read.parquet("/user/s18151825/data/geo/cities_parquet")
print(f"Городов: {cities_df.count()}")
cities_df.show(5, truncate=False)

Проверка витрины пользователей (user_geo_viz):

# Витрина пользователей
user_viz = spark.read.parquet("/user/s18151825/data/analytics/user_geo_viz")
print(f"Записей: {user_viz.count()}")
user_viz.show(10, truncate=False)
user_viz.printSchema()

Проверка витрины зон (geo_zones_viz):

# Витрина зон
zones_viz = spark.read.parquet("/user/s18151825/data/analytics/geo_zones_viz")
print(f"Записей: {zones_viz.count()}")
zones_viz.show(20, truncate=False)
zones_viz.printSchema()

Проверка витрины рекомендаций (friends_recommendations):

# Витрина рекомендаций
friends = spark.read.parquet("/user/s18151825/data/analytics/friends_recommendations")
print(f"Рекомендаций: {friends.count()}")
friends.show(10, truncate=False)


Проверка через HDFS (терминал)

# Проверка структуры папок
hdfs dfs -ls /user/s18151825/data/analytics/

# Проверка каждой витрины
hdfs dfs -ls /user/s18151825/data/analytics/user_geo_viz/
hdfs dfs -ls /user/s18151825/data/analytics/geo_zones_viz/
hdfs dfs -ls /user/s18151825/data/analytics/friends_recommendations/

# Количество записей в витринах (через wc -l)
hdfs dfs -cat /user/s18151825/data/analytics/user_geo_viz/date=*/part-*.parquet | wc -l
hdfs dfs -cat /user/s18151825/data/analytics/geo_zones_viz/month=*/part-*.parquet | wc -l
hdfs dfs -cat /user/s18151825/data/analytics/friends_recommendations/date=*/part-*.parquet | wc -l


Команды запуска скриптов


# Запуск витрины пользователей
spark-submit --master local[*] /lessons/user_geo_visits.py \
    /user/master/data/geo/events \
    /user/s18151825/data/geo/cities_parquet \
    /user/s18151825/data/analytics/user_geo_viz

# Запуск витрины зон
spark-submit --master local[*] /lessons/geo_zones_viz.py \
    /user/master/data/geo/events \
    /user/s18151825/data/geo/cities_parquet \
    /user/s18151825/data/analytics/geo_zones_viz

# Запуск витрины рекомендаций
spark-submit --master local[*] /lessons/friends_recommendations.py \
    /user/master/data/geo/events \
    /user/s18151825/data/analytics/friends_recommendations

