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
    tags=["sprint5", "dds", "dm", "products"],
    is_paused_upon_creation=True,
)
def dm_products_loader():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_products():
        with dwh_pg_connect.connection() as conn:
            cur = conn.cursor()
            
            # Получаем заказы из STG
            cur.execute("""
                SELECT object_value, update_ts
                FROM stg.ordersystem_orders
                WHERE object_value::json->>'final_status' IN ('CLOSED', 'CANCELLED')
                ORDER BY update_ts
            """)
            rows = cur.fetchall()
            log.info(f"Processing {len(rows)} orders")

            for row in rows:
                obj = str2json(row[0])
                update_ts = row[1]

                # Получаем restaurant_id из документа
                restaurant_id_source = obj.get("restaurant", {}).get("id")
                if not restaurant_id_source:
                    continue

                # Ищем ресторан - берем самую старую активную версию
                # (или любую, так как даты заказов могут быть раньше)
                cur.execute(
                    """
                    SELECT id FROM dds.dm_restaurants
                    WHERE restaurant_id = %s AND active_to = '2099-12-31'
                    ORDER BY active_from ASC
                    LIMIT 1
                    """,
                    (restaurant_id_source,),
                )
                restaurant_row = cur.fetchone()

                if not restaurant_row:
                    # Если нет активной версии, берем любую запись
                    cur.execute(
                        """
                        SELECT id FROM dds.dm_restaurants
                        WHERE restaurant_id = %s
                        ORDER BY active_from ASC
                        LIMIT 1
                        """,
                        (restaurant_id_source,),
                    )
                    restaurant_row = cur.fetchone()
                    
                if not restaurant_row:
                    log.warning(f"Restaurant not found: {restaurant_id_source}")
                    continue

                restaurant_id = restaurant_row[0]

                # Извлекаем продукты из заказа
                order_items = obj.get("order_items", [])
                for item in order_items:
                    product_id = item.get("id")
                    if not product_id:
                        continue

                    product_name = item.get("name")
                    product_price = float(item.get("price", 0))

                    # Проверяем, существует ли активная версия продукта
                    cur.execute(
                        """
                        SELECT id, product_price 
                        FROM dds.dm_products
                        WHERE product_id = %s AND restaurant_id = %s AND active_to = '2099-12-31'
                        """,
                        (product_id, restaurant_id),
                    )
                    active_product = cur.fetchone()

                    if active_product:
                        # Если цена изменилась - закрываем старую версию
                        if float(active_product[1]) != product_price:
                            cur.execute(
                                """
                                UPDATE dds.dm_products
                                SET active_to = %s
                                WHERE id = %s
                                """,
                                (update_ts, active_product[0]),
                            )
                            
                            # Вставляем новую версию
                            cur.execute(
                                """
                                INSERT INTO dds.dm_products 
                                (product_id, product_name, product_price, restaurant_id, active_from, active_to)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                """,
                                (product_id, product_name, product_price, restaurant_id, 
                                 update_ts, datetime(2099, 12, 31)),
                            )
                            log.info(f"Updated product {product_id}: price changed")
                    else:
                        # Новый продукт
                        cur.execute(
                            """
                            INSERT INTO dds.dm_products 
                            (product_id, product_name, product_price, restaurant_id, active_from, active_to)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (product_id, product_name, product_price, restaurant_id,
                             update_ts, datetime(2099, 12, 31)),
                        )
                        log.info(f"Inserted new product {product_id} from order at {update_ts}")

            conn.commit()
            
            # Проверяем результат
            cur.execute("SELECT COUNT(*) FROM dds.dm_products")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM dds.dm_products WHERE active_to = '2099-12-31'")
            active = cur.fetchone()[0]
            log.info(f"Final: {total} total products, {active} active products")
            
            # Выводим пример продуктов для проверки
            cur.execute("""
                SELECT product_id, product_name, product_price, restaurant_id, active_from 
                FROM dds.dm_products 
                WHERE active_to = '2099-12-31'
                LIMIT 5
            """)
            samples = cur.fetchall()
            for sample in samples:
                log.info(f"Sample: product_id={sample[0]}, name={sample[1]}, price={sample[2]}, restaurant_id={sample[3]}, active_from={sample[4]}")

    products_loader = load_products()
    products_loader


dm_products_loader_dag = dm_products_loader()
