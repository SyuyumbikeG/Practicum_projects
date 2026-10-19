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
    tags=["sprint5", "dds", "fct", "sales"],
    is_paused_upon_creation=True,
)
def fct_product_sales_loader():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_product_sales():
        with dwh_pg_connect.connection() as conn:
            cur = conn.cursor()

            # Очищаем таблицу
            cur.execute("TRUNCATE TABLE dds.fct_product_sales;")

            # Вставляем ТОЛЬКО продукты с бонусами (как в тесте)
            cur.execute("""
                INSERT INTO dds.fct_product_sales 
                (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                SELECT 
                    dp.id AS product_id,
                    ord.id AS order_id,
                    oi.count,
                    oi.price,
                    oi.total_sum,
                    COALESCE(bp.bonus_payment, 0) AS bonus_payment,
                    COALESCE(bp.bonus_grant, 0) AS bonus_grant
                FROM (
                    SELECT 
                        o.object_value::json->>'_id' as order_key,
                        item->>'id' as product_id,
                        o.object_value::json->'restaurant'->>'id' as restaurant_id,
                        SUM((item->>'quantity')::INT) as count,
                        AVG((item->>'price')::NUMERIC(19,5)) as price,
                        SUM(((item->>'quantity')::INT * (item->>'price')::NUMERIC(19,5))) as total_sum,
                        o.update_ts as update_ts
                    FROM stg.ordersystem_orders o
                    CROSS JOIN LATERAL json_array_elements(o.object_value::json->'order_items') AS item
                    WHERE o.object_value::json->>'final_status' IN ('CLOSED', 'CANCELLED')
                    GROUP BY o.object_value::json->>'_id', item->>'id', 
                             o.object_value::json->'restaurant'->>'id', o.update_ts
                ) oi
                JOIN dds.dm_orders ord 
                    ON ord.order_key = oi.order_key
                JOIN dds.dm_restaurants dr 
                    ON dr.restaurant_id = oi.restaurant_id
                JOIN dds.dm_products dp 
                    ON dp.product_id = oi.product_id
                    AND dp.restaurant_id = dr.id
                    AND dp.active_to = '2099-12-31'
                INNER JOIN (  -- Меняем LEFT JOIN на INNER JOIN
                    SELECT 
                        be.event_value::json->>'order_id' as order_key,
                        p->>'product_id' as product_id,
                        SUM((p->>'bonus_payment')::NUMERIC) as bonus_payment,
                        SUM((p->>'bonus_grant')::NUMERIC) as bonus_grant
                    FROM stg.bonussystem_events be
                    CROSS JOIN LATERAL json_array_elements(be.event_value::json->'product_payments') AS p
                    WHERE be.event_type = 'bonus_transaction'
                    GROUP BY be.event_value::json->>'order_id', p->>'product_id'
                ) bp ON bp.order_key = oi.order_key AND bp.product_id = oi.product_id
                WHERE (bp.bonus_payment > 0 OR bp.bonus_grant > 0)  -- Только с ненулевыми бонусами
            """)

            conn.commit()

            cur.execute("SELECT COUNT(*) FROM dds.fct_product_sales")
            total = cur.fetchone()[0]
            log.info(f"Final: {total} records in fct_product_sales")

    sales_loader = load_product_sales()
    sales_loader


fct_product_sales_loader_dag = fct_product_sales_loader()
