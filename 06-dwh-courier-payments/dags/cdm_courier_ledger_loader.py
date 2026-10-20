import logging
from datetime import datetime

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval="0 3 * * *",  # Запуск ежедневно в 03:00
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=True,
    tags=["sprint6", "cdm", "courier", "ledger"],
    is_paused_upon_creation=True,
)
def cdm_courier_ledger_loader():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_courier_ledger():
        """Расчёт витрины для курьеров"""
        with dwh_pg_connect.connection() as conn:
            cur = conn.cursor()

            # Очищаем витрину перед загрузкой
            cur.execute("TRUNCATE TABLE cdm.dm_courier_ledger;")
            log.info("Truncated cdm.dm_courier_ledger")

            # Заполняем витрину
            cur.execute("""
                INSERT INTO cdm.dm_courier_ledger 
                (courier_id, courier_name, settlement_year, settlement_month,
                 orders_count, orders_total_sum, rate_avg,
                 order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
                WITH courier_metrics AS (
                    SELECT 
                        c.courier_id,
                        c.courier_name,
                        EXTRACT(YEAR FROM t.ts)::INT as settlement_year,
                        EXTRACT(MONTH FROM t.ts)::INT as settlement_month,
                        COUNT(DISTINCT o.id) as orders_count,
                        SUM(fs.total_sum) as orders_total_sum,
                        AVG(d.rate) as rate_avg,
                        SUM(d.tip_sum) as courier_tips_sum,
                        -- Процент выплаты курьеру в зависимости от рейтинга
                        CASE 
                            WHEN AVG(d.rate) < 4 THEN 0.05
                            WHEN AVG(d.rate) >= 4 AND AVG(d.rate) < 4.5 THEN 0.07
                            WHEN AVG(d.rate) >= 4.5 AND AVG(d.rate) < 4.9 THEN 0.08
                            WHEN AVG(d.rate) >= 4.9 THEN 0.10
                            ELSE 0.05
                        END as rate_percent,
                        -- Минимальная сумма выплаты
                        CASE 
                            WHEN AVG(d.rate) < 4 THEN 100
                            WHEN AVG(d.rate) >= 4 AND AVG(d.rate) < 4.5 THEN 150
                            WHEN AVG(d.rate) >= 4.5 AND AVG(d.rate) < 4.9 THEN 175
                            WHEN AVG(d.rate) >= 4.9 THEN 200
                            ELSE 100
                        END as min_payment
                    FROM dds.fct_product_sales fs
                    JOIN dds.dm_orders o ON fs.order_id = o.id
                    JOIN dds.dm_timestamps t ON o.timestamp_id = t.id
                    JOIN dds.dm_couriers c ON o.courier_id = c.courier_id
                    LEFT JOIN dds.fct_deliveries d ON d.order_id = o.id
                    WHERE o.order_status = 'CLOSED'
                    GROUP BY c.courier_id, c.courier_name, EXTRACT(YEAR FROM t.ts), EXTRACT(MONTH FROM t.ts)
                )
                SELECT 
                    courier_id,
                    courier_name,
                    settlement_year,
                    settlement_month,
                    orders_count,
                    orders_total_sum,
                    COALESCE(rate_avg, 0) as rate_avg,
                    orders_total_sum * 0.25 as order_processing_fee,
                    GREATEST(
                        orders_total_sum * rate_percent,
                        min_payment
                    ) as courier_order_sum,
                    COALESCE(courier_tips_sum, 0) as courier_tips_sum,
                    GREATEST(
                        orders_total_sum * rate_percent,
                        min_payment
                    ) + COALESCE(courier_tips_sum, 0) * 0.95 as courier_reward_sum
                FROM courier_metrics
                ORDER BY settlement_year DESC, settlement_month DESC, courier_id
            """)

            conn.commit()

            # Проверяем результат
            cur.execute("SELECT COUNT(*) FROM cdm.dm_courier_ledger")
            total = cur.fetchone()[0]
            log.info(f"Loaded {total} records into cdm.dm_courier_ledger")

            # Выводим статистику по месяцам
            cur.execute("""
                SELECT 
                    settlement_year,
                    settlement_month,
                    COUNT(DISTINCT courier_id) as couriers_count,
                    SUM(orders_count) as total_orders,
                    SUM(orders_total_sum) as total_revenue,
                    SUM(courier_reward_sum) as total_reward
                FROM cdm.dm_courier_ledger
                GROUP BY settlement_year, settlement_month
                ORDER BY settlement_year DESC, settlement_month DESC
                LIMIT 6
            """)
            stats = cur.fetchall()
            log.info("Monthly statistics:")
            for stat in stats:
                log.info(
                    f"  {stat[0]}-{stat[1]:02d}: {stat[2]} couriers, "
                    f"{stat[3]} orders, {stat[4]:.2f} revenue, {stat[5]:.2f} reward"
                )

    courier_ledger = load_courier_ledger()
    courier_ledger


cdm_courier_ledger_loader_dag = cdm_courier_ledger_loader()
