{{
    config(
        materialized='view'
    )
}}

/*
=============================================================================
STAGING: Events Analysis (ClickHouse over Iceberg)
=============================================================================
Analyze events data from Iceberg tables on MinIO using ClickHouse engine.

Input: lakehouse.daily_sales (Iceberg on MinIO)
Output: Analysis view
=============================================================================
*/

SELECT
    event_date,
    category_level1,
    category_level2,
    order_count,
    unique_customers,
    unique_products,
    total_revenue,
    avg_order_value,
    sale_year,
    sale_month,
    -- Calculated fields
    ROUND(total_revenue / NULLIF(order_count, 0), 2) as revenue_per_order,
    ROUND(order_count / NULLIF(unique_customers, 0), 2) as orders_per_customer
FROM lakehouse.daily_sales
