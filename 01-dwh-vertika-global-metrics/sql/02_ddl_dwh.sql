DROP TABLE IF EXISTS VT2606186DB5B4__DWH.global_metrics CASCADE;

CREATE TABLE VT2606186DB5B4__DWH.global_metrics (
    date_update                    DATE,
    currency_from                  INTEGER,
    amount_total                   NUMERIC(18,2),
    cnt_transactions               INTEGER,
    avg_transactions_per_account   NUMERIC(18,2),
    cnt_accounts_make_transactions INTEGER
);

CREATE PROJECTION VT2606186DB5B4__DWH.global_metrics_proj AS
SELECT * FROM VT2606186DB5B4__DWH.global_metrics
ORDER BY date_update, currency_from
SEGMENTED BY HASH(date_update, currency_from) ALL NODES;
