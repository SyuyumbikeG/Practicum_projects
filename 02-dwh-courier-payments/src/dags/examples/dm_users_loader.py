import logging
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "dds", "dm", "users"],
    is_paused_upon_creation=True,
)
def dm_users_loader():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_users():
        with dwh_pg_connect.connection() as conn:
            cur = conn.cursor()

            cur.execute("DELETE FROM dds.dm_users;")

            cur.execute("""
                INSERT INTO dds.dm_users (user_id, user_name, user_login)
                SELECT 
                    object_value::json->>'_id' AS user_id,
                    object_value::json->>'name' AS user_name,
                    object_value::json->>'login' AS user_login
                FROM stg.ordersystem_users
                WHERE object_value::json->>'_id' IS NOT NULL
            """)

            conn.commit()
            cur.execute("SELECT COUNT(*) FROM dds.dm_users;")
            count = cur.fetchone()[0]
            log.info(f"Loaded {count} records into dds.dm_users")

    users_loader = load_users()
    users_loader


dm_users_loader_dag = dm_users_loader()
