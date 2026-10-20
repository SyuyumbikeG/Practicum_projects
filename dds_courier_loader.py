import logging
from datetime import datetime

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from lib.dict_util import str2json, json2str

log = logging.getLogger(__name__)


@dag(
    schedule_interval="0 2 * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint6", "dds", "couriers", "deliveries"],
    is_paused_upon_creation=True,
)
def dds_courier_loader():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_couriers():
        """Загрузка курьеров из STG в DDS"""
        with dwh_pg_connect.connection() as conn:
            cur = conn.cursor()

            # Получаем курьеров из STG
            cur.execute("""
                SELECT object_value
                FROM stg.api_couriers
            """)
            rows = cur.fetchall()

            inserted_count = 0
            updated_count = 0

            for row in rows:
                # Данные уже в формате dict, не нужно парсить
                obj = row[0] if isinstance(row[0], dict) else str2json(row[0])
                courier_id = obj.get("_id")
                courier_name = obj.get("name")

                if not courier_id:
                    continue

                # Проверяем, существует ли уже курьер
                cur.execute(
                    """
                    SELECT id, courier_name FROM dds.dm_couriers
                    WHERE courier_id = %s AND active_to = '2099-12-31'
                    """,
                    (courier_id,),
                )
                existing = cur.fetchone()

                if existing:
                    # Если имя изменилось - закрываем старую версию и создаём новую
                    if existing[1] != courier_name:
                        # Закрываем старую версию
                        cur.execute(
                            """
                            UPDATE dds.dm_couriers
                            SET active_to = %s
                            WHERE courier_id = %s AND active_to = '2099-12-31'
                            """,
                            (datetime.now(), courier_id),
                        )
                        # Вставляем новую версию
                        cur.execute(
                            """
                            INSERT INTO dds.dm_couriers (courier_id, courier_name, active_from, active_to)
                            VALUES (%s, %s, %s, '2099-12-31')
                            """,
                            (courier_id, courier_name, datetime.now()),
                        )
                        updated_count += 1
                else:
                    # Вставляем нового курьера
                    cur.execute(
                        """
                        INSERT INTO dds.dm_couriers (courier_id, courier_name, active_from, active_to)
                        VALUES (%s, %s, %s, '2099-12-31')
                        """,
                        (courier_id, courier_name, datetime.now()),
                    )
                    inserted_count += 1

            conn.commit()
            log.info(
                f"Couriers loaded: inserted={inserted_count}, updated={updated_count}"
            )

    @task()
    def update_orders_courier():
        """Обновление заказов: добавляем courier_id из доставок"""
        with dwh_pg_connect.connection() as conn:
            cur = conn.cursor()

            # Обновляем courier_id в dm_orders на основе данных из доставок
            cur.execute("""
                UPDATE dds.dm_orders o
                SET courier_id = d.courier_id
                FROM (
                    SELECT DISTINCT
                        d.object_value->>'order_id' as order_key,
                        d.object_value->>'courier_id' as courier_id
                    FROM stg.api_deliveries d
                ) d
                WHERE o.order_key = d.order_key
                  AND o.courier_id IS NULL
            """)

            updated_count = cur.rowcount
            conn.commit()
            log.info(f"Updated {updated_count} orders with courier_id")

    @task()
    def load_deliveries_fact():
        """Загрузка фактов доставок в dds.fct_deliveries"""
        with dwh_pg_connect.connection() as conn:
            cur = conn.cursor()

            # Загружаем факты доставок
            cur.execute("""
                INSERT INTO dds.fct_deliveries 
                (order_id, courier_id, delivery_ts, rate, tip_sum)
                SELECT 
                    o.id as order_id,
                    c.id as courier_id,
                    (d.object_value->>'delivery_ts')::TIMESTAMP as delivery_ts,
                    (d.object_value->>'rate')::INT as rate,
                    (d.object_value->>'tip_sum')::NUMERIC(14,2) as tip_sum
                FROM stg.api_deliveries d
                JOIN dds.dm_orders o ON o.order_key = d.object_value->>'order_id'
                JOIN dds.dm_couriers c ON c.courier_id = d.object_value->>'courier_id'
                    AND c.active_to = '2099-12-31'
                ON CONFLICT (order_id) DO UPDATE 
                SET 
                    courier_id = EXCLUDED.courier_id,
                    delivery_ts = EXCLUDED.delivery_ts,
                    rate = EXCLUDED.rate,
                    tip_sum = EXCLUDED.tip_sum
            """)

            affected_count = cur.rowcount
            conn.commit()
            log.info(f"Loaded/updated {affected_count} delivery facts")

    @task()
    def check_data_quality():
        """Проверка качества данных"""
        with dwh_pg_connect.connection() as conn:
            cur = conn.cursor()

            # Проверяем, есть ли заказы без courier_id
            cur.execute("""
                SELECT COUNT(*) FROM dds.dm_orders 
                WHERE order_status = 'CLOSED' 
                  AND courier_id IS NULL
            """)
            missing_courier = cur.fetchone()[0]

            if missing_courier > 0:
                log.warning(f"Found {missing_courier} closed orders without courier_id")
            else:
                log.info("All closed orders have courier_id assigned")

            # Проверяем, все ли доставки привязаны к заказам
            cur.execute("""
                SELECT COUNT(*) 
                FROM stg.api_deliveries d
                LEFT JOIN dds.dm_orders o ON o.order_key = d.object_value->>'order_id'
                WHERE o.id IS NULL
            """)
            missing_orders = cur.fetchone()[0]

            if missing_orders > 0:
                log.warning(
                    f"Found {missing_orders} deliveries without matching orders"
                )
            else:
                log.info("All deliveries matched to existing orders")

            # Проверяем, все ли доставки привязаны к курьерам
            cur.execute("""
                SELECT COUNT(*) 
                FROM stg.api_deliveries d
                LEFT JOIN dds.dm_couriers c ON c.courier_id = d.object_value->>'courier_id'
                    AND c.active_to = '2099-12-31'
                WHERE c.id IS NULL
            """)
            missing_couriers = cur.fetchone()[0]

            if missing_couriers > 0:
                log.warning(
                    f"Found {missing_couriers} deliveries without matching couriers"
                )
            else:
                log.info("All deliveries matched to existing couriers")

    # Определяем порядок выполнения
    couriers = load_couriers()
    orders_update = update_orders_courier()
    deliveries = load_deliveries_fact()
    quality_check = check_data_quality()

    # Зависимости: сначала курьеры, потом обновление заказов, потом факты, потом проверка
    couriers >> orders_update >> deliveries >> quality_check


dds_courier_loader_dag = dds_courier_loader()
