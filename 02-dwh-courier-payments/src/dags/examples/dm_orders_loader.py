import logging
from datetime import datetime

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from lib.dict_util import str2json

log = logging.getLogger(__name__)


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "dds", "dm", "orders"],
    is_paused_upon_creation=True,
)
def dm_orders_loader():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_orders():
        with dwh_pg_connect.connection() as conn:
            cur = conn.cursor()

            # Получаем заказы из STG
            cur.execute("""
                SELECT object_value, update_ts
                FROM stg.ordersystem_orders
                ORDER BY update_ts
            """)
            rows = cur.fetchall()
            log.info(f"Processing {len(rows)} orders")

            inserted_count = 0
            skipped_count = 0
            updated_count = 0

            for row in rows:
                obj = str2json(row[0])
                update_ts = row[1]

                # Получаем идентификаторы из заказа
                order_id = obj.get("_id")
                order_status = obj.get("final_status")
                order_date = obj.get("date")
                restaurant_id_source = obj.get("restaurant", {}).get("id")
                user_id_source = obj.get("user", {}).get("id")

                if not all([order_id, restaurant_id_source, user_id_source]):
                    log.warning(f"Missing required fields in order {order_id}")
                    skipped_count += 1
                    continue

                # 1. Находим ресторан
                cur.execute(
                    """
                    SELECT id FROM dds.dm_restaurants
                    WHERE restaurant_id = %s
                    LIMIT 1
                    """,
                    (restaurant_id_source,),
                )
                restaurant_row = cur.fetchone()

                if not restaurant_row:
                    log.warning(
                        f"Restaurant not found for {restaurant_id_source}, order {order_id}"
                    )
                    skipped_count += 1
                    continue

                restaurant_id = restaurant_row[0]

                # 2. Находим пользователя
                cur.execute(
                    """
                    SELECT id FROM dds.dm_users
                    WHERE user_id = %s
                    LIMIT 1
                    """,
                    (user_id_source,),
                )
                user_row = cur.fetchone()

                if not user_row:
                    log.warning(
                        f"User not found for {user_id_source}, order {order_id}"
                    )
                    skipped_count += 1
                    continue

                user_id = user_row[0]

                # 3. Находим timestamp
                timestamp_value = update_ts

                if order_date:
                    try:
                        if isinstance(order_date, str):
                            for fmt in [
                                "%Y-%m-%d %H:%M:%S",
                                "%Y-%m-%dT%H:%M:%S",
                                "%Y-%m-%d",
                            ]:
                                try:
                                    timestamp_value = datetime.strptime(order_date, fmt)
                                    break
                                except:
                                    continue
                        else:
                            timestamp_value = order_date
                    except Exception as e:
                        log.warning(
                            f"Could not parse date {order_date}, using update_ts: {e}"
                        )

                # Ищем существующий timestamp
                cur.execute(
                    """
                    SELECT id FROM dds.dm_timestamps WHERE ts = %s
                    """,
                    (timestamp_value,),
                )
                ts_row = cur.fetchone()

                if not ts_row:
                    log.warning(
                        f"Timestamp not found for {timestamp_value}, order {order_id}"
                    )
                    skipped_count += 1
                    continue

                timestamp_id = ts_row[0]

                # 4. Проверяем, существует ли уже заказ
                cur.execute(
                    """
                    SELECT id FROM dds.dm_orders WHERE order_key = %s
                    """,
                    (order_id,),
                )
                existing = cur.fetchone()

                if existing:
                    # Обновляем существующий заказ
                    cur.execute(
                        """
                        UPDATE dds.dm_orders
                        SET order_status = %s, restaurant_id = %s, timestamp_id = %s, user_id = %s
                        WHERE order_key = %s
                        """,
                        (order_status, restaurant_id, timestamp_id, user_id, order_id),
                    )
                    updated_count += 1
                    if updated_count % 100 == 0:
                        log.info(f"Updated {updated_count} orders so far")
                else:
                    # Вставляем новый заказ
                    cur.execute(
                        """
                        INSERT INTO dds.dm_orders (order_key, order_status, restaurant_id, timestamp_id, user_id)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (order_id, order_status, restaurant_id, timestamp_id, user_id),
                    )
                    inserted_count += 1
                    if inserted_count % 100 == 0:
                        log.info(f"Inserted {inserted_count} orders so far")

            conn.commit()

            # Проверяем результат
            cur.execute("SELECT COUNT(*) FROM dds.dm_orders")
            total = cur.fetchone()[0]
            log.info(
                f"Final: {total} orders in dm_orders (inserted: {inserted_count}, updated: {updated_count}, skipped: {skipped_count})"
            )

            # Выводим примеры для проверки
            cur.execute("""
                SELECT o.id, o.order_key, o.order_status, 
                       r.restaurant_name, u.user_name, t.ts
                FROM dds.dm_orders o
                JOIN dds.dm_restaurants r ON o.restaurant_id = r.id
                JOIN dds.dm_users u ON o.user_id = u.id
                JOIN dds.dm_timestamps t ON o.timestamp_id = t.id
                LIMIT 5
            """)
            samples = cur.fetchall()
            for sample in samples:
                log.info(
                    f"Sample order: id={sample[0]}, key={sample[1]}, status={sample[2]}, "
                    f"restaurant={sample[3]}, user={sample[4]}, timestamp={sample[5]}"
                )

    orders_loader = load_orders()
    orders_loader


dm_orders_loader_dag = dm_orders_loader()
