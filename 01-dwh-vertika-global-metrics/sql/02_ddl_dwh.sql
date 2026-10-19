DROP TABLE IF EXISTS DWH.global_metrics CASCADE;

CREATE TABLE DWH.global_metrics (
    date_update                    DATE,
    currency_from                  INTEGER,
    amount_total                   NUMERIC(18,2),
    cnt_transactions               INTEGER,
    avg_transactions_per_account   NUMERIC(18,2),
    cnt_accounts_make_transactions INTEGER
);

CREATE PROJECTION DWH.global_metrics_proj AS
SELECT * FROM DWH.global_metrics
ORDER BY date_update, currency_from
SEGMENTED BY HASH(date_update, currency_from) ALL NODES;
