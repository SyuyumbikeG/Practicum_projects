from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.operators.python import PythonOperator

# настройки 
PG_CONN_ID = "postgres_source"
VT_CONN_ID = "vertica_dwh"
STG_SCHEMA = "VT2606186DB5B4__STAGING"

default_args = {
    "owner": "vt2606186db5b4",
    "start_date": datetime(2022, 10, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def load_transactions(ds: str, **context):
    """Читает public.transactions за ds из Postgres и пишет в staging Vertica."""
    pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
    pg_conn = pg.get_conn()
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT operation_id,
                   account_number_from,
                   account_number_to,
                   currency_code,
                   country,
                   status,
                   transaction_type,
                   amount,
                   transaction_dt
            FROM public.transactions
            WHERE transaction_dt::date = %s
            """,
            (ds,),
        )
        rows = cur.fetchall()

    print(f"[{ds}] transactions fetched from Postgres: {len(rows)}")
    if not rows:
        return

    vt = VerticaHook(vertica_conn_id=VT_CONN_ID)
    vt_conn = vt.get_conn()
    with vt_conn.cursor() as cur:
        # идемпотентность
        cur.execute(
            f"DELETE FROM {STG_SCHEMA}.transactions WHERE transaction_dt::date = %s",
            (ds,),
        )
        cur.executemany(
            f"""
            INSERT INTO {STG_SCHEMA}.transactions
            (operation_id, account_number_from, account_number_to, currency_code,
             country, status, transaction_type, amount, transaction_dt)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
    vt_conn.commit()
    print(f"[{ds}] transactions loaded into Vertica: {len(rows)}")


def load_currencies(ds: str, **context):
    """Читает public.currencies за ds из Postgres и пишет в staging Vertica."""
    pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
    pg_conn = pg.get_conn()
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT date_update,
                   currency_code,
                   currency_code_with,
                   currency_with_div
            FROM public.currencies
            WHERE date_update::date = %s
            """,
            (ds,),
        )
        rows = cur.fetchall()

    print(f"[{ds}] currencies fetched from Postgres: {len(rows)}")
    if not rows:
        return

    vt = VerticaHook(vertica_conn_id=VT_CONN_ID)
    vt_conn = vt.get_conn()
    with vt_conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {STG_SCHEMA}.currencies WHERE date_update::date = %s",
            (ds,),
        )
        cur.executemany(
            f"""
            INSERT INTO {STG_SCHEMA}.currencies
            (date_update, currency_code, currency_code_with, currency_with_div)
            VALUES (%s, %s, %s, %s)
            """,
            rows,
        )
    vt_conn.commit()
    print(f"[{ds}] currencies loaded into Vertica: {len(rows)}")


with DAG(
    dag_id="1_data_import",
    default_args=default_args,
    description="Postgres → Vertica staging (transactions, currencies)",
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["final_project", "staging"],
) as dag:

    t_transactions = PythonOperator(
        task_id="load_transactions",
        python_callable=load_transactions,
    )

    t_currencies = PythonOperator(
        task_id="load_currencies",
        python_callable=load_currencies,
    )

    t_transactions >> t_currencies
