-- gold/dim_calendar.sql
{{
    config(
        materialized='table',
        schema='gold',
        tags=['calendar']
    )
}}

WITH date_spine AS (
    -- Генерируем непрерывный ряд дат от минимальной до максимальной
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="SELECT MIN(timestamp)::DATE FROM " ~ ref('raw_warehouse_occupancy'),
            end_date="SELECT MAX(timestamp)::DATE + INTERVAL '60 days' FROM " ~ ref('raw_warehouse_occupancy')
        )
    }}
),

calendar_base AS (
    SELECT
        date_day AS date,
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(DAY FROM date_day) AS day_of_month,
        EXTRACT(DOW FROM date_day) AS day_of_week,  -- 0=воскресенье, 6=суббота
        EXTRACT(DOY FROM date_day) AS day_of_year,
        EXTRACT(WEEK FROM date_day) AS week_of_year
        
    FROM date_spine
),

-- Добавляем признаки выходных и праздников (на основе вашего dim_calendar из генератора)
-- Если у вас уже есть таблица silver.dim_calendar, лучше джойнить её сюда
calendar_with_flags AS (
    SELECT
        cb.*,
        
        -- Выходные (суббота=6, воскресенье=0)
        CASE WHEN cb.day_of_week IN (0, 6) THEN 1 ELSE 0 END AS is_weekend,
        
        -- Праздники: берём из существующего календаря или задаём явно
        COALESCE(sc.is_holiday, 0) AS is_holiday,
        COALESCE(sc.holiday_name, '') AS holiday_name,
        
        -- День недели как категория для one-hot (или можно оставить числовым)
        CASE cb.day_of_week
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_of_week_name,
        
        -- Квартал
        EXTRACT(QUARTER FROM cb.date) AS quarter,
        
        -- Признак "начало/конец месяца"
        CASE 
            WHEN cb.day_of_month <= 7 THEN 'early_month'
            WHEN cb.day_of_month >= (EXTRACT(DAY FROM (DATE_TRUNC('month', cb.date) + INTERVAL '1 month - 1 day'))) - 6 
                 THEN 'late_month'
            ELSE 'mid_month'
        END AS month_period,
        
        -- Дней до ближайшего праздника (если есть таблица праздников)
        -- Это сложный расчёт, пока упростим
        
        -- Сезонность
        CASE 
            WHEN EXTRACT(MONTH FROM cb.date) IN (12, 1, 2) THEN 'winter'
            WHEN EXTRACT(MONTH FROM cb.date) IN (3, 4, 5) THEN 'spring'
            WHEN EXTRACT(MONTH FROM cb.date) IN (6, 7, 8) THEN 'summer'
            ELSE 'autumn'
        END AS season
        
    FROM calendar_base cb
    LEFT JOIN {{ ref('dim_calendar') }} sc ON cb.date = sc.date_id  -- если есть silver.dim_calendar
)

SELECT * FROM calendar_with_flags