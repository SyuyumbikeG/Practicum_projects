insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount)
select dc.date_id, item_id, customer_id, city_id, quantity, payment_amount from stage.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}';

INSERT INTO mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
SELECT 
    dc.date_id, 
    uol.item_id, 
    uol.customer_id, 
    uol.city_id,
    CASE 
        WHEN uol.status = 'refunded' THEN -uol.quantity
        ELSE uol.quantity
    END AS quantity,
    CASE 
        WHEN uol.status = 'refunded' THEN -uol.payment_amount
        ELSE uol.payment_amount
    END AS payment_amount,
    COALESCE(uol.status, 'shipped') AS status
FROM stage.user_order_log uol
LEFT JOIN mart.d_calendar dc ON uol.date_time::DATE = dc.fact_date
WHERE uol.date_time::DATE = '{{ds}}'::DATE;