{{
    config(
        materialized='view'
    )
}}

/*
=============================================================================
STAGING: Funnel Analysis (ClickHouse over Iceberg)
=============================================================================
Conversion funnel from Iceberg tables.

Input: lakehouse.funnel_analysis (Iceberg on MinIO)
Output: Funnel analysis view with health indicators
=============================================================================
*/

SELECT
    event_date,
    views,
    carts,
    purchases,
    unique_viewers,
    unique_carters,
    unique_purchasers,
    total_revenue,
    view_to_cart_rate,
    cart_to_purchase_rate,
    overall_conversion_rate,
    user_view_to_cart_rate,
    user_cart_to_purchase_rate,
    avg_revenue_per_purchaser,
    analysis_year,
    analysis_month,
    -- Funnel health indicator
    CASE
        WHEN overall_conversion_rate >= 5 THEN 'Excellent'
        WHEN overall_conversion_rate >= 2 THEN 'Good'
        WHEN overall_conversion_rate >= 1 THEN 'Average'
        ELSE 'Needs Improvement'
    END as funnel_health
FROM lakehouse.funnel_analysis
