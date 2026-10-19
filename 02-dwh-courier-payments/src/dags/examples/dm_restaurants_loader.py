import logging
from datetime import datetime

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder, PgConnect
from lib.dict_util import json2str, str2json

log = logging.getLogger(__name__)


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "dds", "dm", "restaurants"],
    is_paused_upon_creation=True,
)
def dm_restaurants_loader():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_restaurants():
        with dwh_pg_connect.connection() as conn:
            cur = conn.cursor()

            # ОЧЕНЬ ВАЖНО: сортируем по update_ts
            cur.execute("""
                SELECT object_id, object_value, update_ts
                FROM stg.ordersystem_restaurants
                ORDER BY update_ts
            """)
            rows = cur.fetchall()

            log.info(f"Processing {len(rows)} restaurant updates")

            for row in rows:
                object_id = row[0]
                obj = str2json(row[1])
                update_ts = row[2]

                restaurant_id = obj.get("_id")
                restaurant_name = obj.get("name")

                if not restaurant_id:
                    log.warning(f"No restaurant_id in object {object_id}")
                    continue

                # Проверяем, есть ли уже активная запись для этого ресторана
                cur.execute(
                    """
                    SELECT id, restaurant_name, active_from 
                    FROM dds.dm_restaurants
                    WHERE restaurant_id = %s AND active_to = '2099-12-31'
                    """,
                    (restaurant_id,),
                )
                existing = cur.fetchone()

                if existing:
                    # Если название не изменилось - пропускаем
                    if existing[1] == restaurant_name:
                        log.info(f"Restaurant {restaurant_id} unchanged, skipping")
                        continue

                    # Название изменилось - закрываем старую версию
                    log.info(
                        f"Restaurant {restaurant_id} name changed from '{existing[1]}' to '{restaurant_name}'"
                    )
                    cur.execute(
                        """
                        UPDATE dds.dm_restaurants
                        SET active_to = %s
                        WHERE id = %s
                        """,
                        (update_ts, existing[0]),
                    )

                # Вставляем новую версию
                cur.execute(
                    """
                    INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%s, %s, %s, '2099-12-31')
                    """,
                    (restaurant_id, restaurant_name, update_ts),
                )
                log.info(f"Inserted new version for restaurant {restaurant_id}")

            conn.commit()

            # Проверяем результат
            cur.execute("""
                SELECT restaurant_id, COUNT(*) as versions, 
                       COUNT(CASE WHEN active_to = '2099-12-31' THEN 1 END) as active
                FROM dds.dm_restaurants
                GROUP BY restaurant_id
            """)
            stats = cur.fetchall()
            for stat in stats:
                log.info(f"Restaurant {stat[0]}: {stat[1]} versions, {stat[2]} active")

            cur.execute("SELECT COUNT(*) FROM dds.dm_restaurants;")
            count = cur.fetchone()[0]
            log.info(f"Total {count} records in dds.dm_restaurants")

    restaurants_loader = load_restaurants()
    restaurants_loader


dm_restaurants_loader_dag = dm_restaurants_loader()
