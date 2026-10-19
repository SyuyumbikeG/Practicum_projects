import logging
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval="0 0 * * *",  # Запускаем ежедневно
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=True,
    tags=["sprint5", "cdm", "dm", "settlement"],
    is_paused_upon_creation=True,
)
def dm_settlement_report_loader():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_settlement_report():
        with dwh_pg_connect.connection() as conn:
            cur = conn.cursor()

            # Инкрементальная загрузка (только за последние 7 дней)
            cur.execute("""
                INSERT INTO cdm.dm_settlement_report 
                (restaurant_id, restaurant_name, settlement_date, 
                 orders_count, orders_total_sum, orders_bonus_payment_sum, 
                 orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                SELECT 
                    r.id AS restaurant_id,
                    r.restaurant_name,
                    t.ts::DATE AS settlement_date,
                    COUNT(DISTINCT o.id) AS orders_count,
                    SUM(fs.total_sum) AS orders_total_sum,
                    SUM(fs.bonus_payment) AS orders_bonus_payment_sum,
                    SUM(fs.bonus_grant) AS orders_bonus_granted_sum,
                    SUM(fs.total_sum) * 0.25 AS order_processing_fee,
                    SUM(fs.total_sum) - SUM(fs.bonus_payment) - (SUM(fs.total_sum) * 0.25) AS restaurant_reward_sum
                FROM dds.fct_product_sales fs
                JOIN dds.dm_orders o ON fs.order_id = o.id
                JOIN dds.dm_restaurants r ON o.restaurant_id = r.id
                JOIN dds.dm_timestamps t ON o.timestamp_id = t.id
                WHERE o.order_status = 'CLOSED'
                  AND t.ts >= CURRENT_DATE - INTERVAL '7 days'  -- Только последние 7 дней
                GROUP BY r.id, r.restaurant_name, t.ts::DATE
                ON CONFLICT (restaurant_id, settlement_date) DO UPDATE 
                SET 
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum,
                    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                    order_processing_fee = EXCLUDED.order_processing_fee,
                    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum
            """)

            conn.commit()

            cur.execute("SELECT COUNT(*) FROM cdm.dm_settlement_report")
            total = cur.fetchone()[0]
            log.info(f"Total records in cdm.dm_settlement_report: {total}")

            # Показываем данные за последние 3 дня
            cur.execute("""
                SELECT 
                    settlement_date,
                    restaurant_name,
                    orders_count,
                    orders_total_sum,
                    restaurant_reward_sum
                FROM cdm.dm_settlement_report
                WHERE settlement_date >= CURRENT_DATE - INTERVAL '3 days'
                ORDER BY settlement_date DESC, restaurant_id
            """)
            samples = cur.fetchall()
            log.info("Last 3 days data:")
            for sample in samples:
                log.info(
                    f"  {sample[0]}: {sample[1]} - {sample[2]} orders, {sample[3]:.2f} revenue, {sample[4]:.2f} reward"
                )

    settlement_loader = load_settlement_report()
    settlement_loader


dm_settlement_report_loader_dag = dm_settlement_report_loader()
