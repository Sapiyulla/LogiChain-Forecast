-- gold/fact_warehouse_daily.sql
{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key=['date', 'warehouse_id'],
        incremental_strategy='delete+insert',
        tags=['forecast', 'daily']
    )
}}

WITH daily_operations AS (
    -- Агрегируем операции из silver в дневные суммы
    SELECT
        date,
        warehouse_id,
        SUM(receiving_volume_24h) AS receiving_24h,
        SUM(shipping_volume_24h) AS shipping_24h,
        -- occupied_pallets: берём последнее значение за день (ближе к концу дня)
        -- Используем ARRAY_AGG с ORDER BY timestamp DESC
        (ARRAY_AGG(occupied_pallets ORDER BY timestamp DESC NULLS LAST))[1] AS occupied_pallets_end_of_day,
        COUNT(*) AS records_count
    FROM {{ ref('fact_operations') }}
    WHERE occupied_pallets IS NOT NULL
    GROUP BY date, warehouse_id
),

-- Подтягиваем ёмкость склада
with_capacity AS (
    SELECT
        do.*,
        w.total_capacity,
        -- load_factor = занятость / ёмкость (обрезаем до 1)
        LEAST(occupied_pallets_end_of_day / NULLIF(w.total_capacity, 0), 1.0) AS load_factor
    FROM daily_operations do
    LEFT JOIN {{ ref('dim_warehouses') }} w ON do.warehouse_id = w.warehouse_id
),

-- Рассчитываем кумулятивную занятость (если пропуски)
-- Используем оконную функцию, чтобы заполнить NULL значения
cumulative_occupancy AS (
    SELECT
        *,
        -- Если occupied_pallets_end_of_day NULL, берём последнее известное значение
        COALESCE(
            occupied_pallets_end_of_day,
            LAST_VALUE(occupied_pallets_end_of_day IGNORE NULLS) OVER (
                PARTITION BY warehouse_id ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            )
        ) AS occupied_pallets_filled
    FROM with_capacity
),

-- Добавляем лаговые признаки (для ML)
with_lags AS (
    SELECT
        co.*,
        -- Лаги load_factor (для прогноза)
        LAG(load_factor, 1) OVER w AS load_factor_d1,
        LAG(load_factor, 2) OVER w AS load_factor_d2,
        LAG(load_factor, 3) OVER w AS load_factor_d3,
        LAG(load_factor, 7) OVER w AS load_factor_d7,
        LAG(load_factor, 14) OVER w AS load_factor_d14,
        
        -- Скользящие средние потоков (7 дней)
        AVG(receiving_24h) OVER w7 AS receiving_avg_7d,
        AVG(shipping_24h) OVER w7 AS shipping_avg_7d,
        
        -- Скользящие суммы (тренд)
        SUM(receiving_24h) OVER w7 AS receiving_sum_7d,
        SUM(shipping_24h) OVER w7 AS shipping_sum_7d,
        
        -- Дневной нетто-поток
        receiving_24h - shipping_24h AS net_flow
        
    FROM cumulative_occupancy co
    WINDOW 
        w AS (PARTITION BY warehouse_id ORDER BY date),
        w7 AS (PARTITION BY warehouse_id ORDER BY date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING)
)

-- Инкрементальная логика
{% if is_incremental() %}
    -- При инкрементальной загрузке: 
    -- берём все дни за последние 30 дней + все даты, которые отсутствуют в целевой таблице
    SELECT 
        wl.*,
        CURRENT_TIMESTAMP AS updated_at
    FROM with_lags wl
    WHERE date >= (SELECT COALESCE(MAX(date), '1900-01-01') FROM {{ this }}) - INTERVAL '30 days'
       OR NOT EXISTS (SELECT 1 FROM {{ this }} t WHERE t.date = wl.date AND t.warehouse_id = wl.warehouse_id)
{% else %}
    -- Первый запуск: вся история
    SELECT 
        wl.*,
        CURRENT_TIMESTAMP AS updated_at
    FROM with_lags wl
{% endif %}