-- Добавляем колонку courier_id в существующую таблицу dm_orders
ALTER TABLE dds.dm_orders 
ADD COLUMN IF NOT EXISTS courier_id VARCHAR;