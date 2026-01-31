{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='event_id',
        partition_by=['event_date'],
        file_format='iceberg'
    )
}}

/*
=============================================================================
STAGING: Events (Bronze → Silver)
=============================================================================
Làm sạch và chuẩn hóa dữ liệu events từ Bronze layer.

Input: iceberg.bronze.events_raw
Output: iceberg.silver.stg_events
=============================================================================
*/

WITH source AS (
    SELECT *
    FROM iceberg.bronze.events_raw
    {% if is_incremental() %}
    WHERE _ingestion_time > (SELECT MAX(_ingestion_time) FROM {{ this }})
    {% endif %}
),

-- Loại bỏ duplicates
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY event_time, event_type, product_id, user_id, user_session
            ORDER BY _ingestion_time DESC
        ) AS row_num
    FROM source
),

-- Làm sạch dữ liệu
cleaned AS (
    SELECT
        -- Generate unique event ID
        {{ dbt_utils.generate_surrogate_key(['event_time', 'event_type', 'product_id', 'user_id', 'user_session']) }} AS event_id,
        
        -- Timestamps
        event_time,
        CAST(event_time AS DATE) AS event_date,
        YEAR(event_time) AS event_year,
        MONTH(event_time) AS event_month,
        DAY(event_time) AS event_day,
        HOUR(event_time) AS event_hour,
        DAYOFWEEK(event_time) AS day_of_week,
        
        -- Event info
        event_type,
        
        -- Product info (cleaned)
        product_id,
        category_id,
        COALESCE(NULLIF(TRIM(category_code), ''), 'uncategorized') AS category_code,
        SPLIT(COALESCE(NULLIF(TRIM(category_code), ''), 'uncategorized'), '\\.')[0] AS category_level1,
        COALESCE(SPLIT(COALESCE(NULLIF(TRIM(category_code), ''), 'uncategorized'), '\\.')[1], 'general') AS category_level2,
        SPLIT(COALESCE(NULLIF(TRIM(category_code), ''), 'uncategorized'), '\\.')[2] AS category_level3,
        COALESCE(NULLIF(TRIM(brand), ''), 'Unknown') AS brand,
        LOWER(TRIM(COALESCE(NULLIF(TRIM(brand), ''), 'unknown'))) AS brand_normalized,
        
        -- Price (validated)
        CASE 
            WHEN price IS NULL OR price < 0 THEN 0.0
            ELSE ROUND(price, 2)
        END AS price,
        
        -- Price tier
        CASE
            WHEN price IS NULL OR price = 0 THEN 'free'
            WHEN price < 10 THEN 'budget'
            WHEN price < 50 THEN 'mid_range'
            WHEN price < 200 THEN 'premium'
            ELSE 'luxury'
        END AS price_tier,
        
        -- User info
        user_id,
        user_session,
        
        -- Payment method (Schema Evolution - may be NULL for old data)
        payment_method,
        
        -- Weekend flag
        CASE 
            WHEN DAYOFWEEK(event_time) IN (1, 7) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        
        -- Metadata
        _ingestion_time,
        _source_file,
        _batch_id,
        CURRENT_TIMESTAMP() AS _transform_time
        
    FROM deduplicated
    WHERE row_num = 1
      AND user_id IS NOT NULL
      AND product_id IS NOT NULL
)

SELECT * FROM cleaned
