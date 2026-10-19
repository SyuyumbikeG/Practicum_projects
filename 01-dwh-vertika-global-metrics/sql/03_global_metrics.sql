INSERT INTO DWH.global_metrics
WITH
-- 1 Только успешные (done) транзакции за день, без тестовых аккаунтов
tx AS (
    SELECT
        operation_id,
        account_number_from,
        currency_code        AS currency_from,
        amount,
        transaction_dt::date AS date_update
    FROM STAGING.transactions
    WHERE transaction_dt::date = :ds
      AND status = 'done'
      AND account_number_from > 0
      AND account_number_to   > 0
),
-- 2 Курс валюты к USD (внутренний код 420) за этот же день
rate AS (
    SELECT
        currency_code,
        currency_with_div,
        date_update::date AS date_update
    FROM STAGING.currencies
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
