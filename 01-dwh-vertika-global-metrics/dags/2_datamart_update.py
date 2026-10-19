from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.operators.python import PythonOperator

VT_CONN_ID = "vertica_dwh"
STG_SCHEMA = "VT2606186DB5B4__STAGING"
DWH_SCHEMA = "VT2606186DB5B4__DWH"

GLOBAL_METRICS_SQL = f"""
INSERT INTO {DWH_SCHEMA}.global_metrics
WITH
tx AS (
    SELECT
        operation_id,
        account_number_from,
        currency_code AS currency_from,
        amount,
        transaction_dt::date AS date_update
    FROM {STG_SCHEMA}.transactions
    WHERE transaction_dt::date = :ds
      AND status = 'done'
      AND account_number_from > 0
      AND account_number_to   > 0
),
rate AS (
    SELECT
        currency_code,
        currency_with_div,
        date_update::date AS date_update
    FROM {STG_SCHEMA}.currencies
    WHERE currency_code_with = 420
      AND date_update::date = :ds
)
SELECT
    tx.date_update,
    tx.currency_from,
    ROUND(
        SUM(tx.amount / 100.0 * COALESCE(r.currency_with_div, 1))::NUMERIC,
        2
    ) AS amount_total,
    COUNT(*) AS cnt_transactions,
    ROUND(
        COUNT(*)::NUMERIC / NULLIF(COUNT(DISTINCT tx.account_number_from), 0),
        2
    ) AS avg_transactions_per_account,
    COUNT(DISTINCT tx.account_number_from) AS cnt_accounts_make_transactions
FROM tx
LEFT JOIN rate r
       ON r.currency_code = tx.currency_from
      AND r.date_update  = tx.date_update
GROUP BY tx.date_update, tx.currency_from;
"""

DELETE_SQL = f"DELETE FROM {DWH_SCHEMA}.global_metrics WHERE date_update = :ds;"


def fill_global_metrics(ds: str, **context):
    vt = VerticaHook(vertica_conn_id=VT_CONN_ID)
    vt_conn = vt.get_conn()
    with vt_conn.cursor() as cur:
        cur.execute(DELETE_SQL, {"ds": ds})
        cur.execute(GLOBAL_METRICS_SQL, {"ds": ds})
        inserted = cur.rowcount
    vt_conn.commit()
    print(f"[{ds}] global_metrics rows inserted: {inserted}")


default_args = {
    "owner": "vt2606186db5b4",
    "start_date": datetime(2022, 10, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


with DAG(
    dag_id="2_datamart_update",
    default_args=default_args,
    description="Заполнение витрины global_metrics в Vertica",
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["final_project", "dwh"],
) as dag:

    t_metrics = PythonOperator(
        task_id="fill_global_metrics",
        python_callable=fill_global_metrics,
    )
