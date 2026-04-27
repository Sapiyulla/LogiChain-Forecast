-- gold/state_warehouse_last_known.sql
{{
    config(
        materialized='table',
        schema='gold',
        tags=['control']
    )
}}

SELECT
    warehouse_id,
    MAX(date) AS last_known_date,
    -- Последнее известное occupied_pallets
    (ARRAY_AGG(occupied_pallets_filled ORDER BY date DESC))[1] AS last_occupied_pallets,
    (ARRAY_AGG(load_factor ORDER BY date DESC))[1] AS last_load_factor,
    CURRENT_TIMESTAMP AS state_updated_at
FROM {{ ref('fact_warehouse_daily') }}
GROUP BY warehouse_id