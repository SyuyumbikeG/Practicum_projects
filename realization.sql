-- Шаг 2. Создать таблицу group_log в Vertica
DROP TABLE IF EXISTS STAGING.group_log;
CREATE TABLE STAGING.group_log
(
    group_id BIGINT,
    user_id BIGINT,
    user_id_from BIGINT,
    event VARCHAR(50),
    datetime DATETIME
);

-- Проверям количество записей
SELECT COUNT(*) FROM STAGING.group_log;


-- Шаг 4. Создать таблицу связи 
DROP TABLE IF EXISTS DWH.l_user_group_activity;
CREATE TABLE DWH.l_user_group_activity
(
    hk_l_user_group_activity INT PRIMARY KEY,
    hk_user_id INT NOT NULL CONSTRAINT fk_l_user_group_activity_user REFERENCES DWH.h_users (hk_user_id),
    hk_group_id INT NOT NULL CONSTRAINT fk_l_user_group_activity_group REFERENCES DWH.h_groups (hk_group_id),
    load_dt DATETIME,
    load_src VARCHAR(20)
);

-- Шаг 5. Создать скрипты миграции в таблицу связи

INSERT INTO DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
select distinct
        HASH(hu.hk_user_id, hg.hk_group_id) AS hk_l_user_group_activity,
        hu.hk_user_id,
        hg.hk_group_id,
        NOW() AS load_dt,
        's3' AS load_src
from STAGING.group_log as gl
left join DWH.h_users as hu ON gl.user_id = hu.user_id
left join DWH.h_groups as hg ON gl.group_id = hg.group_id
WHERE hu.hk_user_id IS NOT NULL 
AND hg.hk_group_id IS NOT NULL;


-- Шаг 6. Создать и наполнить сателлит

DROP TABLE IF EXISTS DWH.s_auth_history;

CREATE TABLE DWH.s_auth_history
(
    hk_l_user_group_activity BIGINT NOT NULL CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from INT,
    event VARCHAR(50),
    event_dt DATETIME,
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);

INSERT INTO DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
SELECT 
    luga.hk_l_user_group_activity,
    gl.user_id_from,
    gl.event,
    gl.datetime AS event_dt,
    NOW() AS load_dt,
    's3' AS load_src
FROM STAGING.group_log AS gl
LEFT JOIN DWH.h_users AS hu ON gl.user_id = hu.user_id
LEFT JOIN DWH.h_groups AS hg ON gl.group_id = hg.group_id
LEFT JOIN DWH.l_user_group_activity AS luga 
    ON hg.hk_group_id = luga.hk_group_id 
    AND hu.hk_user_id = luga.hk_user_id
WHERE luga.hk_l_user_group_activity IS NOT NULL;

-- Шаги 7. Подготовить CTE для ответов бизнесу
-- Шаг 7.1. Подготовить CTE user_group_messages

WITH user_group_messages AS (
    SELECT 
        lgd.hk_group_id,
        COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM DWH.l_groups_dialogs AS lgd
    LEFT JOIN DWH.l_user_message AS lum 
        ON lgd.hk_message_id = lum.hk_message_id
    GROUP BY lgd.hk_group_id
)
SELECT 
    hk_group_id,
    cnt_users_in_group_with_messages
FROM user_group_messages
ORDER BY cnt_users_in_group_with_messages DESC
LIMIT 10;

-- Шаг 7.2. Подготовить CTE user_group_log
WITH user_group_log AS (
    SELECT 
        luga.hk_group_id,
        COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users
    FROM DWH.s_auth_history AS ah
    LEFT JOIN DWH.l_user_group_activity AS luga
        ON ah.hk_l_user_group_activity = luga.hk_l_user_group_activity
    WHERE ah.event = 'add'
      AND luga.hk_group_id IN (
          SELECT hk_group_id
          FROM DWH.h_groups
          ORDER BY registration_dt
          LIMIT 10
      )
    GROUP BY luga.hk_group_id
)
SELECT 
    hk_group_id,
    cnt_added_users
FROM user_group_log
ORDER BY cnt_added_users DESC
LIMIT 10;

-- Шаг 7.3. Написать запрос и ответить на вопрос бизнеса
WITH user_group_log AS (
    -- Количество пользователей, вступивших в 10 самых старых групп
    SELECT 
        luga.hk_group_id,
        COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users
    FROM DWH.s_auth_history AS ah
    LEFT JOIN DWH.l_user_group_activity AS luga
        ON ah.hk_l_user_group_activity = luga.hk_l_user_group_activity
    WHERE ah.event = 'add'
      AND luga.hk_group_id IN (
          SELECT hk_group_id
          FROM DWH.h_groups
          ORDER BY registration_dt
          LIMIT 10
      )
    GROUP BY luga.hk_group_id
),
user_group_messages AS (
    -- Количество пользователей, написавших сообщения в этих группах
    SELECT 
        lgd.hk_group_id,
        COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM DWH.l_groups_dialogs AS lgd
    LEFT JOIN DWH.l_user_message AS lum 
        ON lgd.hk_message_id = lum.hk_message_id
    WHERE lgd.hk_group_id IN (
        SELECT hk_group_id
        FROM DWH.h_groups
        ORDER BY registration_dt
        LIMIT 10
    )
    GROUP BY lgd.hk_group_id
)
-- Финальный SELECT с расчётом конверсии
SELECT 
    ugl.hk_group_id,
    ugl.cnt_added_users,
    COALESCE(ugm.cnt_users_in_group_with_messages, 0) AS cnt_users_in_group_with_messages,
    ROUND(
        COALESCE(ugm.cnt_users_in_group_with_messages, 0)::NUMERIC / 
        NULLIF(ugl.cnt_added_users, 0) * 100, 
        2
    ) AS group_conversion
FROM user_group_log AS ugl
LEFT JOIN user_group_messages AS ugm 
    ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY group_conversion DESC;