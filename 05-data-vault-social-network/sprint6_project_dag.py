from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import pendulum
import boto3
import os

# Конфигурация S3
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = "sprint6"
DATA_DIR = "/data"

# Список файлов для скачивания
BUCKET_FILES = ["group_log.csv"]


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
    print(f"Файл {key} скачан в {local_path}")


@dag(
    schedule_interval=None,
    start_date=pendulum.parse("2022-07-13"),
    catchup=False,
    tags=["sprint6", "s3", "download"],
)
def sprint6_project():

    fetch_tasks = []
    for file_name in BUCKET_FILES:
        task = PythonOperator(
            task_id=f'fetch_{file_name.replace(".", "_")}',
            python_callable=fetch_s3_file,
            op_kwargs={"bucket": BUCKET_NAME, "key": file_name, "data_dir": DATA_DIR},
        )
        fetch_tasks.append(task)

    print_10_lines_of_each = BashOperator(
        task_id="print_10_lines_of_each",
        bash_command="""
            echo "=== group_log.csv ==="
            head -10 /data/group_log.csv
        """,
    )

    fetch_tasks >> print_10_lines_of_each


sprint6_project_dag = sprint6_project()
