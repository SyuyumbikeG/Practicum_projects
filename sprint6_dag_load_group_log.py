from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import pendulum
import boto3
import os
import pandas as pd
import vertica_python
import io

# Конфигурация S3
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = "sprint6"
DATA_DIR = "/data"

# Конфигурация Vertica
VERTICA_CONN = {
    "host": os.getenv("VERTICA_HOST"),
    "port": int(os.getenv("VERTICA_PORT", 5433)),
    "user": os.getenv("VERTICA_USER"),
    "password": os.getenv("VERTICA_PASSWORD"),
    "database": os.getenv("VERTICA_DATABASE", "dwh"),
    "autocommit": True,
}
VERTICA_SCHEMA = "STAGING"
TABLE_NAME = "group_log"


def fetch_s3_file(bucket: str, key: str, data_dir: str = DATA_DIR):
    """Скачивает файл из S3 в указанную директорию"""
    os.makedirs(data_dir, exist_ok=True)

    session = boto3.session.Session()
    s3_client = session.client(
        service_name="s3",
        endpoint_url="https://storage.yandexcloud.net",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    local_path = os.path.join(data_dir, key)
    s3_client.download_file(Bucket=bucket, Key=key, Filename=local_path)
    print(f" Файл {key} скачан в {local_path}")


def load_group_log_to_vertica():
    """Загружает данные из CSV в таблицу group_log через COPY (быстро!)"""

    # 1. Читаем CSV
    print(" Чтение CSV-файла...")
    df = pd.read_csv(f"{DATA_DIR}/group_log.csv")
    print(f" Прочитано {len(df)} строк")

    # 2. Преобразуем user_id_from в Int64 (поддерживает NULL)
    df["user_id_from"] = pd.array(df["user_id_from"], dtype="Int64")

    # 3. Заполняем NaN (если есть)
    df = df.where(pd.notnull(df), None)

    # 4. Подключаемся к Vertica
    print(" Подключение к Vertica...")
    conn = vertica_python.connect(**VERTICA_CONN)
    cursor = conn.cursor()

    # 5. Очищаем таблицу перед загрузкой
    cursor.execute(f"TRUNCATE TABLE {VERTICA_SCHEMA}.{TABLE_NAME};")
    print(f" Таблица {VERTICA_SCHEMA}.{TABLE_NAME} очищена")

    # 6. Готовим данные для COPY
    print(" Подготовка данных для массовой загрузки...")
    output = io.StringIO()
    df.to_csv(output, index=False, header=False, na_rep="")
    output.seek(0)

    # 7. Массовая загрузка через COPY
    print(" Загрузка данных через COPY...")
    cursor.copy(
        f"""
        COPY {VERTICA_SCHEMA}.{TABLE_NAME} 
        (group_id, user_id, user_id_from, event, datetime)
        FROM STDIN
        DELIMITER ','
        ENCLOSED BY '"'
        NULL AS ''
    """,
        output,
    )

    conn.commit()
    print("Данные успешно загружены!")

    # 8. Проверяем результат
    cursor.execute(f"SELECT COUNT(*) FROM {VERTICA_SCHEMA}.{TABLE_NAME};")
    total = cursor.fetchone()[0]
    print(f"Всего загружено {total} записей")

    cursor.close()
    conn.close()


@dag(
    schedule_interval=None,
    start_date=pendulum.parse("2022-07-13"),
    catchup=False,
    tags=["sprint6", "project", "staging", "group_log"],
)
def sprint6_dag_load_group_log():

    # Задача 1: скачать файл из S3
    fetch_file = PythonOperator(
        task_id="fetch_group_log",
        python_callable=fetch_s3_file,
        op_kwargs={"bucket": BUCKET_NAME, "key": "group_log.csv", "data_dir": DATA_DIR},
    )

    # Задача 2: загрузить данные в Vertica (быстрая версия)
    load_data = PythonOperator(
        task_id="load_group_log_to_staging", python_callable=load_group_log_to_vertica
    )

    fetch_file >> load_data


sprint6_dag_load_group_log_dag = sprint6_dag_load_group_log()
