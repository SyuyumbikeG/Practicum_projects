# ETL-пайплайн для e-commerce с витринами продаж и customer retention

Проект по доработке ETL-пайплайна для интернет-магазина. Включает обработку новых статусов заказов (`refunded`), сохранение обратной совместимости и построение витрины для анализа возвращаемости клиентов (customer retention) в разрезе недель.

## 🎯 Задача

Магазин добавил функционал отмены заказов и возврата средств. Требуется:
1. Обновить пайплайн: учесть в витрине `mart.f_sales` статусы `shipped` и `refunded` (исторические данные считаются `shipped`).
2. Сохранить **backward compatibility** (старые данные не должны сломаться).
3. Построить витрину `mart.f_customer_retention` для анализа возвращаемости клиентов.

## 🏗️ Архитектура
API (генерация отчёта)
│
▼
┌─────────────────────┐
│ S3 (файлы отчётов) │ ← customer_research, user_order_log, user_activity_log, price_log
└─────────┬───────────┘
│
▼
┌─────────────────────┐
│ ETL (Airflow) │
└─────────┬───────────┘
│
┌─────┴─────┐
▼ ▼
mart.f_sales mart.f_customer_retention
(обновлён) (новая витрина)

text

## 🛠️ Стек

- Python
- Apache Airflow
- PostgreSQL
- REST API (асинхронная генерация отчётов)
- S3 (Yandex Object Storage)
- Docker

## 📂 Структура репозитория
.
├── dags/
│ ├── etl_sales_pipeline.py # обновлённый пайплайн f_sales
│ └── etl_customer_retention.py # DAG для витрины retention
├── sql/
│ ├── ddl_mart_f_sales.sql # DDL витрины продаж
│ └── ddl_mart_f_customer_retention.sql # DDL витрины retention
├── docker/
│ └── docker-compose.yaml # локальное окружение
└── README.md

text

## 📊 Витрина `mart.f_customer_retention`

| Поле | Описание |
|------|----------|
| `new_customers_count` | количество новых клиентов (один заказ за период) |
| `returning_customers_count` | количество вернувшихся клиентов (несколько заказов) |
| `refunded_customer_count` | количество клиентов с возвратом |
| `period_name` | `weekly` |
| `period_id` | номер недели |
| `item_id` | идентификатор категории товара |
| `new_customers_revenue` | доход с новых клиентов |
| `returning_customers_revenue` | доход с вернувшихся клиентов |
| `customers_refunded` | количество возвратов |

## 🧮 Логика расчёта

1. **Инкрементальная загрузка** из API: генерация отчёта → получение `report_id` → скачивание файлов из S3 → получение инкрементов за недостающие даты.
2. **Обработка статусов**: `shipped` (успешные заказы) и `refunded` (возвраты). Исторические данные без статуса считаются `shipped`.
3. **Расчёт retention**: группировка по неделям (`period_id`), подсчёт новых / вернувшихся / вернувших клиентов, агрегация выручки по категориям товаров.

## 🔐 Переменные окружения

| Переменная | Назначение |
|------------|------------|
| `API_BASE_URL` | базовый URL API |
| `API_KEY` | ключ доступа |
| `API_NICKNAME` | ваш никнейм |
| `API_COHORT` | номер когорты |
| `S3_BUCKET` | бакет с отчётами |
| `POSTGRES_CONN_ID` | ID подключения к PostgreSQL в Airflow |

## 🚀 Как запустить

1. Развернуть окружение в Docker:
   ```bash
   docker run -d --rm -p 3000:3000 -p 15432:5432 \
     --name=de-project-sprint-3-server \
     cr.yandex/crp1r8pht0n0gl25aug1/project-sprint-3:latest
Применить DDL:

bash
psql -h localhost -p 15432 -U jovyan -d de -f sql/ddl_mart_f_sales.sql
psql -h localhost -p 15432 -U jovyan -d de -f sql/ddl_mart_f_customer_retention.sql
Задать переменные окружения.

Положить DAG'и в папку dags/ Airflow.

Включить DAG'и в UI (localhost:3000).

Проверить витрины:

sql
SELECT * FROM mart.f_sales;
SELECT * FROM mart.f_customer_retention;
