CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
    id SERIAL PRIMARY KEY,
    period_name VARCHAR(10),
    period_id INTEGER,
    item_id INTEGER,
    new_customers_count INTEGER,
    returning_customers_count INTEGER,
    refunded_customer_count INTEGER,
    new_customers_revenue NUMERIC(15,2),
    returning_customers_revenue NUMERIC(15,2),
    customers_refunded INTEGER
);

TRUNCATE TABLE mart.f_customer_retention;

INSERT INTO mart.f_customer_retention (
    period_name, period_id, item_id,
    new_customers_count, returning_customers_count, refunded_customer_count,
    new_customers_revenue, returning_customers_revenue, customers_refunded
)
WITH customer_weekly AS (
    SELECT 
        EXTRACT(WEEK FROM uol.date_time) AS week_num,
        EXTRACT(YEAR FROM uol.date_time) AS year_num,
        uol.customer_id,
        uol.item_id,
        COUNT(DISTINCT uol.order_id) AS orders_count,
        SUM(uol.payment_amount) AS revenue,
        SUM(CASE WHEN uol.status = 'refunded' THEN uol.payment_amount ELSE 0 END) AS refunded_revenue
    FROM stage.user_order_log uol
    WHERE uol.date_time IS NOT NULL
    GROUP BY week_num, year_num, uol.customer_id, uol.item_id
)
SELECT 
    'weekly' AS period_name,
    (year_num * 100 + week_num) AS period_id,
    item_id,
    COUNT(DISTINCT CASE WHEN orders_count = 1 THEN customer_id END) AS new_customers_count,
    COUNT(DISTINCT CASE WHEN orders_count > 1 THEN customer_id END) AS returning_customers_count,
    COUNT(DISTINCT CASE WHEN refunded_revenue > 0 THEN customer_id END) AS refunded_customer_count,
    COALESCE(SUM(CASE WHEN orders_count = 1 THEN revenue ELSE 0 END), 0) AS new_customers_revenue,
    COALESCE(SUM(CASE WHEN orders_count > 1 THEN revenue ELSE 0 END), 0) AS returning_customers_revenue,
    COALESCE(SUM(refunded_revenue), 0) AS customers_refunded
FROM customer_weekly
GROUP BY year_num, week_num, item_id;