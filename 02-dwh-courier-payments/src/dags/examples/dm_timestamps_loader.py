import logging
from datetime import datetime

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "dds", "dm", "timestamps"],
    is_paused_upon_creation=True,
)
def dm_timestamps_loader():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_timestamps():
        with dwh_pg_connect.connection() as conn:
            cur = conn.cursor()

            # Очищаем таблицу
            cur.execute("DELETE FROM dds.dm_timestamps;")

            # Загружаем данные из заказов с финальным статусом (CLOSED или CANCELLED)
            cur.execute("""
                INSERT INTO dds.dm_timestamps (ts, year, month, day, date, time)
                SELECT DISTINCT
                    ts,
                    EXTRACT(YEAR FROM ts)::INT AS year,
                    EXTRACT(MONTH FROM ts)::INT AS month,
                    EXTRACT(DAY FROM ts)::INT AS day,
                    ts::DATE AS date,
                    ts::TIME AS time
                FROM (
                    SELECT 
                        (object_value::json->>'date')::TIMESTAMP AS ts
                    FROM stg.ordersystem_orders
                    WHERE object_value::json->>'final_status' IN ('CLOSED', 'CANCELLED')
                        AND object_value::json->>'date' IS NOT NULL
                ) AS t
                WHERE ts IS NOT NULL
            """)

            conn.commit()
            cur.execute("SELECT COUNT(*) FROM dds.dm_timestamps;")
            count = cur.fetchone()[0]
            log.info(f"Loaded {count} records into dds.dm_timestamps")

    timestamps_loader = load_timestamps()
    timestamps_loader


dm_timestamps_loader_dag = dm_timestamps_loader()
