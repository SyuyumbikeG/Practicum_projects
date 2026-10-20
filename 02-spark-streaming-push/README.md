# Spark Structured Streaming для push-уведомлений об акциях ресторанов

Стриминговое приложение на **PySpark (Spark Structured Streaming)** для доставки push-уведомлений подписчикам ресторанов об акциях с ограниченным сроком действия. Проект реализует чтение из Kafka в реальном времени, stream-static JOIN с PostgreSQL и запись результатов в два сто́ка.

## 🎯 Задача

Агрегатор доставки еды вводит подписку: пользователи могут добавлять рестораны в избранное и получать уведомления об акциях. Нужно построить сервис, который:

1. Читает сообщения об акциях из **Kafka** в режиме реального времени.
2. Фильтрует акции по времени: текущее время должно быть между `adv_campaign_datetime_start` и `adv_campaign_datetime_end`.
3. Находит пользователей, у которых ресторан в избранном (из **PostgreSQL**).
4. Отправляет результаты в **два сто́ка**:
   - **PostgreSQL** (`subscribers_feedback`) — для последующей аналитики фидбэка;
   - **Kafka** (выходной топик) — для сервиса push-уведомлений.

## 🏗️ Архитектура
Kafka (входной топик: сообщения об акциях)
│
▼
┌─────────────────────┐
│ PySpark Streaming │
│ • десериализация │
│ • фильтр по времени│
└─────────┬───────────┘
│
▼
┌─────────────────────┐
│ Stream-static JOIN │ ← PostgreSQL: subscribers_restaurants
│ по restaurant_id │
└─────────┬───────────┘
│
▼
┌─────────────────────┐
│ foreachBatch │
│ • persist() │
└─────────┬───────────┘
│
┌─────┴─────┐
▼ ▼
PostgreSQL Kafka
(feedback) (push)
│ │
▼ ▼
unpersist()

text

## 🛠️ Стек

- Python
- Apache Spark (PySpark, Spark Structured Streaming)
- Apache Kafka
- PostgreSQL
- Docker
- `kcat` (kafkacat) — для отладки

## 📂 Структура репозитория
.
├── app/
│ └── streaming_app.py # Spark Structured Streaming приложение
├── sql/
│ ├── ddl_subscribers_restaurants.sql
│ └── ddl_subscribers_feedback.sql
├── docker/
│ └── docker-compose.yaml # PostgreSQL в Docker
└── README.md

text

## 🔄 Потоки данных

### Входное сообщение из Kafka (JSON)
```json
{
  "restaurant_id": "123e4567-e89b-12d3-a456-426614174000",
  "adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003",
  "adv_campaign_content": "first campaign",
  "adv_campaign_owner": "Ivanov Ivan Ivanovich",
  "adv_campaign_owner_contact": "iiivanov@restaurant.ru",
  "adv_campaign_datetime_start": 1659203516,
  "adv_campaign_datetime_end": 2659207116,
  "datetime_created": 1659131516
}
Выходное сообщение в Kafka (JSON)
Добавляются поля:

client_id — UUID подписчика из PostgreSQL;

trigger_datetime_created — время создания выходного сообщения.

📊 Таблицы PostgreSQL
subscribers_restaurants (входная)
Поле	Тип
id	serial4
client_id	varchar
restaurant_id	varchar
subscribers_feedback (выходная)
Содержит все поля акции + client_id, trigger_datetime_created, feedback (NULL, заполняется позже).

🧮 Логика обработки
Чтение из Kafka — spark.readStream.format('kafka') с SASL_SSL.

Десериализация JSON — from_json по схеме.

Фильтр по времени — current_timestamp между start и end.

Stream-static JOIN — с таблицей subscribers_restaurants по restaurant_id.

foreachBatch — для каждого микробатча:

persist() — кеширование, чтобы не пересчитывать JOIN дважды;

запись в PostgreSQL с полем feedback;

сериализация в JSON и запись в Kafka (без feedback);

unpersist() — очистка памяти.

🚀 Как запустить
Развернуть PostgreSQL в Docker:

bash
docker run -d --name postgres-de -p 5432:5432 \
  -e POSTGRES_USER=jovyan -e POSTGRES_PASSWORD=jovyan -e POSTGRES_DB=de \
  postgres:15
Применить DDL:

bash
psql -h localhost -U jovyan -d de -f sql/ddl_subscribers_restaurants.sql
psql -h localhost -U jovyan -d de -f sql/ddl_subscribers_feedback.sql
Настроить подключение к Kafka (bootstrap servers, SASL credentials).

Отправить тестовое сообщение в Kafka через kcat:

bash
kcat -b <kafka-broker>:9091 \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-512 \
  -X sasl.username="<user>" \
  -X sasl.password="<password>" \
  -t <input_topic> -K: -P
Запустить приложение:

bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.4.0 \
  app/streaming_app.py
Проверить результаты в subscribers_feedback и в выходном топике Kafka.

📌 Особенности реализации
persist() / unpersist() — кеширование объединённого датафрейма между двумя записями.

Stream-static JOIN — PostgreSQL-таблица читается как статичный датафрейм.

foreachBatch — запись в два сто́ка из одного потока.

Идемпотентность — на уровне sink'ов (upsert / проверка ключей).

Docker — локальный PostgreSQL для отладки.
