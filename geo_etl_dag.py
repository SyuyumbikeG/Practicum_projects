import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Настройка окружения
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHON_PATH"] = "/usr/bin/python3"

default_args = {
    "owner": "s18151825",
    "start_date": datetime(2026, 1, 1),
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "geo_etl_dag",
    default_args=default_args,
    description="ETL для гео-витрин",
    schedule_interval=None,
    catchup=False,
    tags=["geo", "datalake"],
)

# ═══════════════════════════════════════════════════════════════
# TASK 1: Витрина пользователей
# ═══════════════════════════════════════════════════════════════
user_geo_visits = SparkSubmitOperator(
    task_id="user_geo_visits",
    application="/lessons/user_geo_visits.py",
    application_args=[
        "/user/master/data/geo/events",
        "/user/s18151825/data/geo/cities_parquet",
        "/user/s18151825/data/analytics/user_geo_viz",
    ],
    conf={
        "spark.master": "local[*]",
        "spark.sql.shuffle.partitions": "10",
        "spark.dynamicAllocation.enabled": "false",
        "spark.executor.memory": "2g",
        "spark.driver.memory": "2g",
    },
    dag=dag,
)

# ═══════════════════════════════════════════════════════════════
# TASK 2: Витрина зон
# ═══════════════════════════════════════════════════════════════
geo_zones_visits = SparkSubmitOperator(
    task_id="geo_zones_visits",
    application="/lessons/geo_zones_viz.py",
    application_args=[
        "/user/master/data/geo/events",
        "/user/s18151825/data/geo/cities_parquet",
        "/user/s18151825/data/analytics/geo_zones_viz",
    ],
    conf={
        "spark.master": "local[*]",
        "spark.sql.shuffle.partitions": "10",
        "spark.dynamicAllocation.enabled": "false",
        "spark.executor.memory": "2g",
        "spark.driver.memory": "2g",
    },
    dag=dag,
)

# ═══════════════════════════════════════════════════════════════
# TASK 3: Витрина рекомендаций
# ═══════════════════════════════════════════════════════════════
friends_recommendations = SparkSubmitOperator(
    task_id="friends_recommendations",
    application="/lessons/friends_recommendations.py",
    application_args=[
        "/user/master/data/geo/events",
        "/user/s18151825/data/analytics/friends_recommendations",
    ],
    conf={
        "spark.master": "local[*]",
        "spark.sql.shuffle.partitions": "10",
        "spark.dynamicAllocation.enabled": "false",
        "spark.executor.memory": "2g",
        "spark.driver.memory": "2g",
    },
    dag=dag,
)

# Запускаем все задачи параллельно
[user_geo_visits, geo_zones_visits, friends_recommendations]
