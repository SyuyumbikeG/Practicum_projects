import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

import pendulum
import requests
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from lib.dict_util import json2str

log = logging.getLogger(__name__)

# Конфигурация API
API_BASE_URL = os.getenv("API_BASE_URL", "https://api.example.com")
API_KEY = os.getenv("API_KEY")
NICKNAME = os.getenv("API_NICKNAME", "your_nickname")
COHORT = os.getenv("API_COHORT", "your_cohort")
LIMIT = int(os.getenv("API_LIMIT", 50))


@dag(
    schedule_interval="0 1 * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint6", "stg", "api", "couriers", "deliveries"],
    is_paused_upon_creation=True,
)
def stg_courier_api_loader():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    def fetch_from_api(endpoint: str, params: Dict[str, Any] = None) -> List[Dict]:
        """Универсальная функция для запросов к API с пагинацией"""
        all_data = []
        offset = 0

        headers = {"X-Nickname": NICKNAME, "X-Cohort": COHORT, "X-API-KEY": API_KEY}

        while True:
            params = params or {}
            params.update(
                {
                    "sort_field": "_id",
                    "sort_direction": "asc",
                    "limit": LIMIT,
                    "offset": offset,
                }
            )

            url = f"{API_BASE_URL}/{endpoint}"

            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()

                if not data:
                    break

                all_data.extend(data)

                if len(data) < LIMIT:
                    break

                offset += LIMIT

            except requests.exceptions.RequestException as e:
                log.error(f"Error fetching from {endpoint}: {e}")
                break

        log.info(f"Fetched {len(all_data)} records from {endpoint}")
        return all_data

    @task()
    def load_couriers():
        """Загрузка курьеров из API в stg.api_couriers"""
        with dwh_pg_connect.connection() as conn:
            cur = conn.cursor()

            couriers = fetch_from_api("couriers")

            cur.execute("TRUNCATE TABLE stg.api_couriers;")

            inserted_count = 0
            for courier in couriers:
                cur.execute(
                    """
                    INSERT INTO stg.api_couriers (object_value, update_ts)
                    VALUES (%s, %s)
                    """,
                    (json2str(courier), datetime.now()),
                )
                inserted_count += 1

            conn.commit()
            log.info(f"Inserted {inserted_count} couriers into stg.api_couriers")

    @task()
    def load_deliveries():
        """Загрузка доставок из API в stg.api_deliveries"""
        with dwh_pg_connect.connection() as conn:
            cur = conn.cursor()

            from_date = (datetime.now() - timedelta(days=7)).strftime(
                "%Y-%m-%d 00:00:00"
            )
            to_date = datetime.now().strftime("%Y-%m-%d 23:59:59")

            deliveries = fetch_from_api(
                "deliveries", params={"from": from_date, "to": to_date}
            )

            cur.execute("TRUNCATE TABLE stg.api_deliveries;")

            inserted_count = 0
            for delivery in deliveries:
                cur.execute(
                    """
                    INSERT INTO stg.api_deliveries (object_value, update_ts)
                    VALUES (%s, %s)
                    """,
                    (json2str(delivery), datetime.now()),
                )
                inserted_count += 1

            conn.commit()
            log.info(f"Inserted {inserted_count} deliveries into stg.api_deliveries")

    @task()
    def load_restaurants():
        """Загрузка ресторанов из API в stg.api_restaurants"""
        with dwh_pg_connect.connection() as conn:
            cur = conn.cursor()

            restaurants = fetch_from_api("restaurants")

            cur.execute("TRUNCATE TABLE stg.api_restaurants;")

            inserted_count = 0
            for restaurant in restaurants:
                cur.execute(
                    """
                    INSERT INTO stg.api_restaurants (object_value, update_ts)
                    VALUES (%s, %s)
                    """,
                    (json2str(restaurant), datetime.now()),
                )
                inserted_count += 1

            conn.commit()
            log.info(f"Inserted {inserted_count} restaurants into stg.api_restaurants")

    couriers_task = load_couriers()
    deliveries_task = load_deliveries()
    restaurants_task = load_restaurants()

    [couriers_task, deliveries_task, restaurants_task]


stg_courier_api_loader_dag = stg_courier_api_loader()
