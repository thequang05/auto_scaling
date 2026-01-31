{{
    config(
        materialized='table',
        file_format='iceberg'
    )
}}

/*
=============================================================================
STAGING: Users Dimension
=============================================================================
Tạo dimension table cho Users từ events data.
=============================================================================
*/

WITH user_events AS (
    SELECT
        user_id,
        event_time,
        event_type,
        price,
        product_id
    FROM {{ ref('stg_events') }}
),

aggregated AS (
    SELECT
        user_id,
        
        -- Temporal metrics
        MIN(event_time) AS first_seen,
        MAX(event_time) AS last_seen,
        DATEDIFF(MAX(event_time), MIN(event_time)) AS days_active,
        
        -- Activity counts
        COUNT(*) AS total_events,
        SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS total_views,
        SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS total_carts,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases,
        
        -- Monetary metrics
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS total_spent,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN product_id END) AS unique_products_purchased,
        
        -- Session metrics
        COUNT(DISTINCT CAST(event_time AS DATE)) AS active_days
        
    FROM user_events
    GROUP BY user_id
)

SELECT
    user_id,
    first_seen,
    last_seen,
    days_active,
    total_events,
    total_views,
    total_carts,
    total_purchases,
    ROUND(total_spent, 2) AS total_spent,
    unique_products_purchased,
    active_days,
    
    -- Derived metrics
    CASE WHEN total_purchases > 0 THEN TRUE ELSE FALSE END AS is_buyer,
    
    ROUND(
        CASE WHEN total_purchases > 0 
        THEN total_spent / total_purchases 
        ELSE 0 END,
        2
    ) AS avg_order_value,
    
    ROUND(total_events * 1.0 / NULLIF(active_days, 0), 2) AS avg_events_per_day,
    
    -- Conversion metrics
    ROUND(
        total_carts * 100.0 / NULLIF(total_views, 0),
        2
    ) AS view_to_cart_rate,
    
    ROUND(
        total_purchases * 100.0 / NULLIF(total_carts, 0),
        2
    ) AS cart_to_purchase_rate,
    
    CURRENT_TIMESTAMP() AS _created_at
    
FROM aggregated
