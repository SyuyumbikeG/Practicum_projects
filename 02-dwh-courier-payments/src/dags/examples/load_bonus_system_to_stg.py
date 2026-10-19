from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json

default_args = {
    "owner": "AirflowAdmin",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def load_ranks():
    """Загрузка таблицы ranks (полная загрузка)"""
    source_hook = PostgresHook(postgres_conn_id="PG_ORIGIN_BONUS_SYSTEM_CONNECTION")
    target_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")

    source_conn = source_hook.get_conn()
    source_cursor = source_conn.cursor()
    source_cursor.execute(
        "SELECT id, name, bonus_percent, min_payment_threshold FROM ranks;"
    )
    rows = source_cursor.fetchall()

    target_conn = target_hook.get_conn()
    target_cursor = target_conn.cursor()
    target_cursor.execute("TRUNCATE TABLE stg.bonussystem_ranks;")

    for row in rows:
        target_cursor.execute(
            """
            INSERT INTO stg.bonussystem_ranks (id, name, bonus_percent, min_payment_threshold)
            VALUES (%s, %s, %s, %s);
        """,
            row,
        )

    target_conn.commit()
    source_cursor.close()
    target_cursor.close()


def load_users():
    """Загрузка таблицы users (полная загрузка)"""
    source_hook = PostgresHook(postgres_conn_id="PG_ORIGIN_BONUS_SYSTEM_CONNECTION")
    target_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")

    source_conn = source_hook.get_conn()
    source_cursor = source_conn.cursor()
    source_cursor.execute("SELECT id, order_user_id FROM users;")
    rows = source_cursor.fetchall()

    target_conn = target_hook.get_conn()
    target_cursor = target_conn.cursor()
    target_cursor.execute("TRUNCATE TABLE stg.bonussystem_users;")

    for row in rows:
        target_cursor.execute(
            """
            INSERT INTO stg.bonussystem_users (id, order_user_id)
            VALUES (%s, %s);
        """,
            row,
        )

    target_conn.commit()
    source_cursor.close()
    target_cursor.close()


def load_events():
    """
    Инкрементальная загрузка таблицы outbox
    Использует курсор (последний загруженный id) из stg.srv_wf_settings
    """
    source_hook = PostgresHook(postgres_conn_id="PG_ORIGIN_BONUS_SYSTEM_CONNECTION")
    target_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")

    target_conn = target_hook.get_conn()
    target_cursor = target_conn.cursor()

    # 1. Получаем последний загруженный id из таблицы курсоров
    target_cursor.execute("""
        SELECT workflow_settings FROM stg.srv_wf_settings 
        WHERE workflow_key = 'outbox_loader'
    """)
    result = target_cursor.fetchone()

    last_loaded_id = 0
    if result:
        settings = result[0] if isinstance(result[0], dict) else json.loads(result[0])
        last_loaded_id = settings.get("last_loaded_id", 0)

    print(f"Последний загруженный ID: {last_loaded_id}")

    # 2. Читаем новые записи из источника (id > last_loaded_id)
    source_conn = source_hook.get_conn()
    source_cursor = source_conn.cursor()
    source_cursor.execute(
        """
        SELECT id, event_ts, event_type, event_value
        FROM outbox
        WHERE id > %s
        ORDER BY id ASC
    """,
        (last_loaded_id,),
    )
    rows = source_cursor.fetchall()

    if not rows:
        print("Нет новых событий для загрузки")
        source_cursor.close()
        target_cursor.close()
        return

    print(f"Найдено {len(rows)} новых событий")

    # 3. Вставляем данные в stg.bonussystem_events (в одной транзакции)
    max_id = last_loaded_id
    for row in rows:
        target_cursor.execute(
            """
            INSERT INTO stg.bonussystem_events (id, event_ts, event_type, event_value)
            VALUES (%s, %s, %s, %s);
        """,
            row,
        )
        max_id = max(max_id, row[0])

    # 4. Обновляем курсор в stg.srv_wf_settings
    new_settings = json.dumps({"last_loaded_id": max_id})

    target_cursor.execute(
        """
        INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
        VALUES ('outbox_loader', %s)
        ON CONFLICT (workflow_key) DO UPDATE SET workflow_settings = EXCLUDED.workflow_settings
    """,
        (new_settings,),
    )

    target_conn.commit()
    source_cursor.close()
    target_cursor.close()

    print(f"Загружено {len(rows)} событий. Последний ID: {max_id}")


# Создаём таблицу для курсоров (если не существует)
def create_cursor_table():
    """Создаёт таблицу stg.srv_wf_settings, если её нет"""
    target_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
    target_conn = target_hook.get_conn()
    target_cursor = target_conn.cursor()

    target_cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS stg;
        
        CREATE TABLE IF NOT EXISTS stg.srv_wf_settings (
            id SERIAL PRIMARY KEY,
            workflow_key VARCHAR(255) NOT NULL UNIQUE,
            workflow_settings JSON NOT NULL
        );
    """)
    target_conn.commit()
    target_cursor.close()


with DAG(
    dag_id="load_bonus_system_to_stg",
    schedule_interval="*/15 * * * *",
    default_args=default_args,
    catchup=False,
    description="Загрузка данных из бонусной системы (ranks, users, events) в слой stg",
) as dag:

    create_cursor = PythonOperator(
        task_id="create_cursor_table",
        python_callable=create_cursor_table,
    )

    load_ranks_task = PythonOperator(
        task_id="load_ranks",
        python_callable=load_ranks,
    )

    load_users_task = PythonOperator(
        task_id="load_users",
        python_callable=load_users,
    )

    load_events_task = PythonOperator(
        task_id="load_events",
        python_callable=load_events,
    )

    create_cursor >> load_ranks_task >> load_users_task >> load_events_task
