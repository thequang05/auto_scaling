{{
    config(
        materialized='table',
        file_format='iceberg',
        partition_by=['analysis_year', 'analysis_month']
    )
}}

/*
=============================================================================
MARTS: Funnel Analysis Fact Table
=============================================================================
Phân tích phễu chuyển đổi: View → Cart → Purchase

KPIs:
- View to Cart conversion rate
- Cart to Purchase conversion rate
- Overall conversion rate
- Revenue per purchaser
=============================================================================
*/

WITH events AS (
    SELECT
        event_date,
        event_type,
        user_id,
        price
    FROM {{ ref('stg_events') }}
),

daily_funnel AS (
    SELECT
        event_date,
        
        -- Event counts
        SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views,
        SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carts,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
        
        -- Unique user counts by event type
        COUNT(DISTINCT CASE WHEN event_type = 'view' THEN user_id END) AS unique_viewers,
        COUNT(DISTINCT CASE WHEN event_type = 'cart' THEN user_id END) AS unique_carters,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS unique_purchasers,
        
        -- Revenue
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS total_revenue
        
    FROM events
    GROUP BY event_date
)

SELECT
    event_date AS analysis_date,
    
    -- Raw counts
    views,
    carts,
    purchases,
    unique_viewers,
    unique_carters,
    unique_purchasers,
    ROUND(total_revenue, 2) AS total_revenue,
    
    -- Event-based conversion rates
    ROUND(carts * 100.0 / NULLIF(views, 0), 2) AS view_to_cart_rate,
    ROUND(purchases * 100.0 / NULLIF(carts, 0), 2) AS cart_to_purchase_rate,
    ROUND(purchases * 100.0 / NULLIF(views, 0), 4) AS overall_conversion_rate,
    
    -- User-based conversion rates
    ROUND(unique_carters * 100.0 / NULLIF(unique_viewers, 0), 2) AS user_view_to_cart_rate,
    ROUND(unique_purchasers * 100.0 / NULLIF(unique_carters, 0), 2) AS user_cart_to_purchase_rate,
    ROUND(unique_purchasers * 100.0 / NULLIF(unique_viewers, 0), 4) AS user_overall_conversion_rate,
    
    -- Revenue metrics
    ROUND(total_revenue / NULLIF(unique_purchasers, 0), 2) AS avg_revenue_per_purchaser,
    ROUND(total_revenue / NULLIF(purchases, 0), 2) AS avg_order_value,
    
    -- Date dimensions
    YEAR(event_date) AS analysis_year,
    MONTH(event_date) AS analysis_month,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _aggregated_at
    
FROM daily_funnel
ORDER BY event_date
