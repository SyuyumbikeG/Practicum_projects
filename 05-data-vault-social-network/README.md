# Data Vault-хранилище для аналитики соцсети

Хранилище данных, построенное по **методологии Data Vault 2.0**: хабы, линки и сателлиты с хеш-ключами. Данные загружаются из S3 в Vertica через Airflow, аналитика конверсии считается через CTE.

## 🎯 Задача

Определить группы (паблики) с высокой **конверсией в первое сообщение** — чтобы маркетинг мог выбрать сообщества для рекламы. Конверсия считается как доля пользователей, написавших хотя бы одно сообщение, от всех вступивших в группу.

Дополнительно — топ-10 самых старых групп с сортировкой по конверсии.

## 🏗️ Архитектура
S3 (group_log.csv)
│
▼
┌─────────────────────┐
│ STAGING (Vertica) │ ← сырые данные
│ • group_log │
└─────────┬───────────┘
│
▼
┌─────────────────────┐
│ DWH (Vertica) │ ← Data Vault 2.0
│ • h_users │ ← хаб пользователей
│ • h_groups │ ← хаб групп
│ • l_user_group_activity │ ← линк
│ • s_auth_history │ ← сателлит
└─────────┬───────────┘
│
▼
┌─────────────────────┐
│ Аналитика (CTE) │ ← конверсия в первое сообщение
└─────────────────────┘

text

## 🛠️ Стек

- Python
- Apache Airflow
- Vertica
- S3 (Yandex Object Storage)
- SQL (DDL, CTE, оконные функции)
- Pandas (для подготовки данных)

## 📂 Структура репозитория
.
├── dags/
│ ├── load_group_log.py # S3 → STAGING
│ └── load_dds.py # STAGING → Data Vault
├── sql/
│ ├── ddl_staging.sql # DDL STAGING
│ ├── ddl_hubs.sql # хабы
│ ├── ddl_link.sql # линк
│ ├── ddl_satellite.sql # сателлит
│ └── analytics_conversion.sql # аналитика конверсии
└── README.md

text

## 🗃️ Модель Data Vault 2.0

### Хабы (hubs)
- `h_users` — пользователи (хеш-ключ `hk_user_id`)
- `h_groups` — группы (хеш-ключ `hk_group_id`)

### Линк (link)
- `l_user_group_activity` — связь «пользователь ↔ группа» с хеш-ключом `hk_l_user_group_activity`

### Сателлит (satellite)
- `s_auth_history` — история событий: `event` (create / add / leave), `event_dt`, `user_id_from` (кто пригласил), `load_dt`, `load_src`

## 📊 Аналитика: конверсия в первое сообщение

Считается через два CTE:
- `user_group_log` — количество вступивших в группу (`cnt_added_users`);
- `user_group_messages` — количество участников, написавших хотя бы одно сообщение (`cnt_users_in_group_with_messages`).

Конверсия: `group_conversion = cnt_users_in_group_with_messages / cnt_added_users`.

Результат — топ-10 самых старых групп, отсортированных по убыванию конверсии.

## 🔐 Переменные окружения

Перед запуском задайте переменные окружения (или настройте Connections в Airflow):

| Переменная | Назначение |
|------------|------------|
| `AWS_ACCESS_KEY_ID` | ключ доступа к S3 |
| `AWS_SECRET_ACCESS_KEY` | секретный ключ S3 |
| `VERTICA_HOST` | хост Vertica |
| `VERTICA_PORT` | порт Vertica (по умолчанию 5433) |
| `VERTICA_USER` | пользователь Vertica |
| `VERTICA_PASSWORD` | пароль Vertica |
| `VERTICA_DATABASE` | имя БД (по умолчанию `dwh`) |

## 🚀 Как запустить

1. Развернуть Airflow и Vertica.
2. Задать переменные окружения (см. выше).
3. Применить DDL:
   ```bash
   vsql -f sql/ddl_staging.sql
   vsql -f sql/ddl_hubs.sql
   vsql -f sql/ddl_link.sql
   vsql -f sql/ddl_satellite.sql
Положить DAG'и в папку dags/ Airflow.

Включить DAG'и в UI.

Запустить аналитический запрос sql/analytics_conversion.sql.

📌 Особенности реализации
Data Vault 2.0 — продвинутая методология моделирования: хабы, линки, сателлиты, хеш-ключи.

Массовая загрузка через COPY — быстрее, чем построчный INSERT.

Обработка пропусков — приведение user_id_from к Int64 в Pandas, чтобы избежать конвертации в FLOAT.

CTE — аналитика конверсии без промежуточных таблиц.

Топ-10 групп — фильтр по самым старым (registration_dt в хабе h_groups), сортировка по конверсии.
