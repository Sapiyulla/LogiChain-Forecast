-- silver/fact_operations.sql
{{
    config(
        materialized='table',
        schema='silver',
        unique_key='operation_id',
        tags=['cleansing']
    )
}}

WITH raw_data AS (
    SELECT
        timestamp,
        warehouse_id,
        occupied_pallets,
        receiving_volume_24h,
        shipping_volume_24h,
        wms_batch_id,
        -- Генерируем уникальный ID для каждой операции (для отслеживания)
        {{ dbt_utils.generate_surrogate_key(['timestamp', 'warehouse_id', 'wms_batch_id']) }} AS operation_id
    FROM {{ ref('raw_warehouse_occupancy') }}
    WHERE timestamp IS NOT NULL
      AND warehouse_id IS NOT NULL
),

-- Очистка от выбросов и некорректных значений
cleansed AS (
    SELECT
        operation_id,
        timestamp::DATE AS date,  -- извлекаем дату для агрегации
        timestamp,
        warehouse_id,
        
        -- occupied_pallets: отрицательные → 0, NULL → интерполируем позже
        CASE 
            WHEN occupied_pallets < 0 THEN 0
            WHEN occupied_pallets IS NULL THEN NULL  -- оставляем NULL для интерполяции
            ELSE occupied_pallets
        END AS occupied_pallets,
        
        -- Приёмка: отрицательные → 0
        GREATEST(COALESCE(receiving_volume_24h, 0), 0) AS receiving_volume_24h,
        
        -- Отгрузка: отрицательные → 0
        GREATEST(COALESCE(shipping_volume_24h, 0), 0) AS shipping_volume_24h,
        
        wms_batch_id
        
    FROM raw_data
),

-- Добавляем флаги типов операций (хотя у вас уже есть готовые объёмы)
enriched AS (
    SELECT
        *,
        -- Признак: была ли активность в этот момент
        CASE WHEN receiving_volume_24h > 0 OR shipping_volume_24h > 0 THEN 1 ELSE 0 END AS has_activity
    FROM cleansed
)

SELECT * FROM enriched