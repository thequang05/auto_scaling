{{
    config(
        materialized='table',
        file_format='iceberg',
        partition_by=['sale_year', 'sale_month']
    )
}}

/*
=============================================================================
MARTS: Daily Sales Fact Table
=============================================================================
Bảng doanh thu theo ngày và danh mục.
Partition by year/month để tối ưu truy vấn time-series.

KPIs:
- Daily revenue by category
- Order count and unique customers
- Average order value
=============================================================================
*/

WITH purchases AS (
    SELECT
        event_date,
        category_level1,
        category_level2,
        brand,
        user_id,
        product_id,
        price
    FROM {{ ref('stg_events') }}
    WHERE event_type = 'purchase'
),

daily_aggregation AS (
    SELECT
        event_date AS sale_date,
        category_level1,
        category_level2,
        
        -- Counts
        COUNT(*) AS order_count,
        COUNT(DISTINCT user_id) AS unique_customers,
        COUNT(DISTINCT product_id) AS unique_products,
        
        -- Revenue metrics
        ROUND(SUM(price), 2) AS total_revenue,
        ROUND(AVG(price), 2) AS avg_order_value,
        MIN(price) AS min_order_value,
        MAX(price) AS max_order_value,
        
        -- Date dimensions
        YEAR(event_date) AS sale_year,
        MONTH(event_date) AS sale_month,
        QUARTER(event_date) AS sale_quarter,
        WEEKOFYEAR(event_date) AS sale_week,
        DAYOFWEEK(event_date) AS sale_day_of_week,
        
        -- Weekend flag
        CASE 
            WHEN DAYOFWEEK(event_date) IN (1, 7) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend
        
    FROM purchases
    GROUP BY 
        event_date,
        category_level1,
        category_level2
)

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['sale_date', 'category_level1', 'category_level2']) }} AS sale_id,
    
    sale_date,
    category_level1,
    category_level2,
    order_count,
    unique_customers,
    unique_products,
    total_revenue,
    avg_order_value,
    min_order_value,
    max_order_value,
    
    -- Derived metrics
    ROUND(total_revenue / NULLIF(unique_customers, 0), 2) AS revenue_per_customer,
    ROUND(order_count * 1.0 / NULLIF(unique_customers, 0), 2) AS orders_per_customer,
    
    -- Date dimensions
    sale_year,
    sale_month,
    sale_quarter,
    sale_week,
    sale_day_of_week,
    is_weekend,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _aggregated_at
    
FROM daily_aggregation
ORDER BY sale_date, category_level1
