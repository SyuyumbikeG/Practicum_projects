-- Таблица для курьеров
DROP TABLE IF EXISTS stg.api_couriers;
CREATE TABLE stg.api_couriers (
    id SERIAL PRIMARY KEY,
    object_value JSON NOT NULL,
    update_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Таблица для доставок
DROP TABLE IF EXISTS stg.api_deliveries;
CREATE TABLE stg.api_deliveries (
    id SERIAL PRIMARY KEY,
    object_value JSON NOT NULL,
    update_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Таблица для ресторанов
DROP TABLE IF EXISTS stg.api_restaurants;
CREATE TABLE stg.api_restaurants (
    id SERIAL PRIMARY KEY,
    object_value JSON NOT NULL,
    update_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
