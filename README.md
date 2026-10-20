# Data Engineering Portfolio

Привет! Меня зовут Сююмбике, я **Data Engineer**. 
Это портфолио проектов по проектированию хранилищ данных, разработке ETL-пайплайнов и потоковой обработке данных.

**Стек:** Python · SQL · Apache Airflow · Apache Spark · Kafka · PostgreSQL · Vertica · Docker · Kubernetes · Hadoop/HDFS · S3

---

## 🗂️ Ключевые проекты

### 1. [Микросервисы потоковой обработки в Kubernetes](./03-kafka-microservices-k8s/)
Разработка двух Kafka-микросервисов для заполнения DDS- и CDM-слоёв DWH. Упаковка в Docker, публикация в Container Registry и развёртывание в Yandex Managed Kubernetes. Обеспечена идемпотентность обработки сообщений.
`Kafka` `Docker` `Kubernetes` `Python` `PostgreSQL`

### 2. [Многослойное DWH для выплат курьерам](./06-dwh-courier-payments/)
Хранилище STG → DDS → CDM с моделью «снежинка». Интеграция с REST API (пагинация), расчёт выплат по рейтингу, чаевым и комиссиям. Учёт даты заказа при ночных доставках.
`Airflow` `PySpark` `PostgreSQL` `REST API` `DWH`

### 3. [Data Lake и геоаналитика для соцсети](./04-data-lake-geoanalytics/)
Построение геоаналитических витрин на PySpark в HDFS. Расчёт расстояний по формуле гаверсинусов, определение домашнего города, витрины для рекомендации друзей. Автоматизация в Airflow.
`PySpark` `HDFS` `Airflow` `Big Data`

### 4. [DWH финтех-стартапа на Vertica](./01-dwh-vertika-global-metrics/)
ETL-пайплайн для сбора транзакций и построения витрины `global_metrics` в MPP-СУБД Vertica. Проекции, сортировка и сегментирование по хеш-функции для оптимизации запросов.
`Vertica` `Airflow` `SQL` `DWH`

### 5. [Data Vault-хранилище](./05-data-vault-social-network/)
Моделирование DWH по методологии Data Vault 2.0 (хабы, линки, сателлиты). Загрузка из S3 в Vertica, аналитика конверсии в первое сообщение через CTE.
`Vertica` `Data Vault 2.0` `S3` `SQL`

### 6. [Spark Structured Streaming для push-уведомлений](./02-spark-streaming-push/)
Стриминговое приложение на PySpark: чтение из Kafka, stream-static JOIN с PostgreSQL, запись в два сто́ка через `foreachBatch`. Управление памятью через `persist()`/`unpersist()`.
`Spark Streaming` `Kafka` `PostgreSQL` `Docker`

### 7. [ETL e-commerce с витриной retention](./07-etl-ecommerce-retention/)
Инкрементальная загрузка данных из API и S3. Обработка статусов заказов (`shipped/refunded`), сохранение backward compatibility. Построение витрины `mart.f_customer_retention` для анализа возвращаемости клиентов.
`Airflow` `REST API` `S3` `PostgreSQL` `ETL`


## 📬 Контакты

- 📧 Email: [sf.gaskarova@gmail.com](mailto:sf.gaskarova@gmail.com)
- 💬 Telegram: [@SyuyumbikeG](https://t.me/SyuyumbikeG)
- 📄 Резюме на hh.ru: [Ссылка на резюме](https://kazan.hh.ru/resume/481f5cdcff113dcea00039ed1f747257396e4a?hhtmFrom=main)
- 📍 Казань, Россия

---
*Открыта к предложениям: Data Engineer · удалённо / гибрид / офис*
