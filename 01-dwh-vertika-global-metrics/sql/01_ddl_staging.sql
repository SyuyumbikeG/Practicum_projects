--  STAGING.transactions 
DROP TABLE IF EXISTS VT2606186DB5B4__STAGING.transactions CASCADE;

CREATE TABLE VT2606186DB5B4__STAGING.transactions (
    operation_id         VARCHAR(50),
    account_number_from  INTEGER,
    account_number_to    INTEGER,
    currency_code        INTEGER,
    country              VARCHAR(50),
    status               VARCHAR(20),
    transaction_type     VARCHAR(30),
    amount               INTEGER,
    transaction_dt       TIMESTAMP
);

-- Проекция: сортировка по дате и id, сегментирование по хешу от (date, id)
CREATE PROJECTION VT2606186DB5B4__STAGING.transactions_proj AS
SELECT * FROM VT2606186DB5B4__STAGING.transactions
ORDER BY transaction_dt, operation_id
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES;


-- STAGING.currencies
DROP TABLE IF EXISTS VT2606186DB5B4__STAGING.currencies CASCADE;

CREATE TABLE VT2606186DB5B4__STAGING.currencies (
    date_update        TIMESTAMP,
    currency_code      INTEGER,
    currency_code_with INTEGER,
    currency_with_div  NUMERIC(10,4)
);

CREATE PROJECTION VT2606186DB5B4__STAGING.currencies_proj AS
SELECT * FROM VT2606186DB5B4__STAGING.currencies
ORDER BY date_update, currency_code, currency_code_with
SEGMENTED BY HASH(date_update, currency_code, currency_code_with) ALL NODES;
