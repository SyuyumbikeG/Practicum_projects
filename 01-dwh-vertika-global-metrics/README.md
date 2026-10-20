# DWH финтех-стартапа: витрина `global_metrics` в Vertica

ETL-пайплайн для сбора транзакционной активности пользователей финтех-стартапа и построения витрины `global_metrics` в колоночной MPP-СУБД **Vertica**.

## 🎯 Задача

Собрать данные о транзакциях пользователей из разных источников и построить ежедневно обновляемую витрину, которая отвечает на вопросы бизнеса:

- какая ежедневная динамика сумм переводов в разных валютах;
- какое среднее количество транзакций на пользователя;
- какое количество уникальных пользователей совершают транзакции в валютах;
- какой общий оборот компании в единой валюте (USD).

## 🏗️ Архитектура
PostgreSQL (источник)
│
▼
┌─────────────────────┐
│ STAGING (Vertica) │ ← сырые данные + проекции
│ • transactions │
│ • currencies │
└─────────┬───────────┘
│
▼
┌─────────────────────┐
│ DWH (Vertica) │
│ • global_metrics │ ← витрина, инкремент по дням
└─────────────────────┘

text

**DAG 1 `1_data_import`** — Postgres → Vertica STAGING (transactions, currencies) с идемпотентностью (`DELETE` перед `INSERT`).

**DAG 2 `2_datamart_update`** — заполнение витрины `global_metrics` из STAGING.

## 🛠️ Стек

- **Python** — ETL-логика
- **Apache Airflow** — оркестрация (2 DAG'а, ежедневное расписание, `catchup=True`)
- **Vertica** — колоночная MPP-СУБД
- **PostgreSQL** — источник данных
- **SQL** — DDL, агрегации, CTE, оконные функции

## 📂 Структура репозитория
.
├── dags/
│ ├── 1_data_import.py # Postgres → Vertica STAGING
│ └── 2_datamart_update.py # STAGING → DWH: global_metrics
├── sql/
│ ├── 01_ddl_staging.sql # DDL STAGING + проекции
│ ├── 02_ddl_dwh.sql # DDL DWH + проекции
│ └── 03_global_metrics.sql # расчёт витрины
└── README.md

text

## 📊 Витрина `global_metrics`

| Поле | Описание |
|------|----------|
| `date_update` | дата расчёта |
| `currency_from` | код валюты транзакции |
| `amount_total` | общая сумма транзакций по валюте в USD |
| `cnt_transactions` | общее количество транзакций |
| `avg_transactions_per_account` | среднее количество транзакций на аккаунт |
| `cnt_accounts_make_transactions` | количество уникальных аккаунтов |

## 🧮 Логика расчёта

1. Берутся только **успешные** транзакции (`status = 'done'`).
2. Исключаются **тестовые аккаунты** (`account_number_from` и `account_number_to` > 0).
3. Суммы переводятся в **USD** по курсу из `currencies` (внутренний код USD = 420).
4. Считаются агрегаты: сумма, количество, среднее на аккаунт, уникальные аккаунты.
5. Запись **инкрементальная**: за каждый день добавляется новая строка.

## ⚙️ Особенности реализации Vertica

- **Проекции** (`CREATE PROJECTION`) — для ускорения запросов витрины.
- **Сортировка** (`ORDER BY`) по ключевым полям — оптимизация чтения.
- **Сегментирование по хеш-функции** (`SEGMENTED BY HASH(...)`) — равномерное распределение данных по узлам кластера.

## 🚀 Как запустить

1. Развернуть Airflow + Vertica (или использовать облачную Vertica).
2. Настроить Connections в Airflow:
   - `postgres_source` — подключение к PostgreSQL-источнику;
   - `vertica_dwh` — подключение к Vertica.
3. Применить DDL:
   ```bash
   vsql -f sql/01_ddl_staging.sql
   vsql -f sql/02_ddl_dwh.sql
Положить DAG'и в папку dags/ Airflow.

Включить DAG'и 1_data_import и 2_datamart_update в UI.

Проверить витрину:

sql
SELECT * FROM dwh.global_metrics ORDER BY date_update DESC;
