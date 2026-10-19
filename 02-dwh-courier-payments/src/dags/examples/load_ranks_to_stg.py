from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    "owner": "AirflowAdmin",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def load_ranks():
    # Подключаемся к источнику (читаем)
    source_hook = PostgresHook(postgres_conn_id="PG_ORIGIN_BONUS_SYSTEM_CONNECTION")
    # Подключаемся к хранилищу (пишем)
    target_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")

    # Читаем из источника
    source_conn = source_hook.get_conn()
    source_cursor = source_conn.cursor()
    source_cursor.execute(
        "SELECT id, name, bonus_percent, min_payment_threshold FROM ranks;"
    )
    rows = source_cursor.fetchall()

    # Очищаем таблицу в хранилище
    target_conn = target_hook.get_conn()
    target_cursor = target_conn.cursor()
    target_cursor.execute("TRUNCATE TABLE stg.bonussystem_ranks;")

    # Вставляем данные
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


with DAG(
    dag_id="load_ranks_to_stg",
    schedule_interval="*/15 * * * *",
    default_args=default_args,
    catchup=False,
) as dag:

    load_ranks_task = PythonOperator(task_id="load_ranks", python_callable=load_ranks)
