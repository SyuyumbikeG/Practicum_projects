-- Таблица курьеров
DROP TABLE IF EXISTS dds.dm_couriers;
CREATE TABLE dds.dm_couriers (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR NOT NULL UNIQUE,
    courier_name VARCHAR NOT NULL
);

-- Добавляем courier_id в dm_orders
ALTER TABLE dds.dm_orders 
ADD COLUMN IF NOT EXISTS courier_id VARCHAR;

-- Таблица фактов доставок
DROP TABLE IF EXISTS dds.fct_deliveries;
CREATE TABLE dds.fct_deliveries (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES dds.dm_orders(id),
    courier_id INT NOT NULL REFERENCES dds.dm_couriers(id),
    delivery_ts TIMESTAMP NOT NULL,
    rate INT NOT NULL CHECK (rate >= 1 AND rate <= 5),
    tip_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
    CONSTRAINT fct_deliveries_order_id_unique UNIQUE (order_id)
);