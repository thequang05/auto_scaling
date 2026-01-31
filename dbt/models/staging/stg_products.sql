{{
    config(
        materialized='table',
        file_format='iceberg'
    )
}}

/*
=============================================================================
STAGING: Products Dimension
=============================================================================
Tạo dimension table cho Products từ events data.
=============================================================================
*/

WITH product_events AS (
    SELECT
        product_id,
        category_id,
        category_code,
        category_level1,
        category_level2,
        category_level3,
        brand,
        brand_normalized,
        price,
        event_type
    FROM {{ ref('stg_events') }}
),

product_stats AS (
    SELECT
        product_id,
        
        -- Get most recent category info
        FIRST_VALUE(category_id) OVER (
            PARTITION BY product_id 
            ORDER BY product_id
        ) AS category_id,
        
        FIRST_VALUE(category_code) OVER (
            PARTITION BY product_id 
            ORDER BY product_id
        ) AS category_code,
        
        FIRST_VALUE(category_level1) OVER (
            PARTITION BY product_id 
            ORDER BY product_id
        ) AS category_level1,
        
        FIRST_VALUE(category_level2) OVER (
            PARTITION BY product_id 
            ORDER BY product_id
        ) AS category_level2,
        
        FIRST_VALUE(category_level3) OVER (
            PARTITION BY product_id 
            ORDER BY product_id
        ) AS category_level3,
        
        FIRST_VALUE(brand) OVER (
            PARTITION BY product_id 
            ORDER BY product_id
        ) AS brand,
        
        FIRST_VALUE(brand_normalized) OVER (
            PARTITION BY product_id 
            ORDER BY product_id
        ) AS brand_normalized,
        
        price,
        event_type
    FROM product_events
),

aggregated AS (
    SELECT
        product_id,
        MAX(category_id) AS category_id,
        MAX(category_code) AS category_code,
        MAX(category_level1) AS category_level1,
        MAX(category_level2) AS category_level2,
        MAX(category_level3) AS category_level3,
        MAX(brand) AS brand,
        MAX(brand_normalized) AS brand_normalized,
        
        -- Price statistics
        ROUND(AVG(price), 2) AS avg_price,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        
        -- Event counts
        COUNT(*) AS total_events,
        SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS view_count,
        SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS cart_count,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
        
        -- Conversion metrics
        ROUND(
            SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) * 100.0 / 
            NULLIF(SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END), 0),
            2
        ) AS view_to_cart_rate,
        
        ROUND(
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) * 100.0 / 
            NULLIF(SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END), 0),
            2
        ) AS cart_to_purchase_rate
        
    FROM product_stats
    GROUP BY product_id
)

SELECT
    *,
    CURRENT_TIMESTAMP() AS _created_at
FROM aggregated
