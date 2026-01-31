-- =============================================================================
-- ClickHouse Sample Queries for Data Lakehouse
-- =============================================================================
-- Các truy vấn mẫu cho Superset Dashboard
-- =============================================================================

-- =============================================================================
-- 1. REVENUE ANALYSIS
-- =============================================================================

-- Total Revenue by Month
SELECT
    sale_year,
    sale_month,
    formatReadableQuantity(sum(total_revenue)) AS total_revenue,
    formatReadableQuantity(sum(order_count)) AS total_orders,
    round(sum(total_revenue) / sum(order_count), 2) AS avg_order_value
FROM lakehouse.daily_sales
GROUP BY sale_year, sale_month
ORDER BY sale_year, sale_month;

-- Top 10 Categories by Revenue
SELECT
    category_level1,
    category_level2,
    sum(total_revenue) AS revenue,
    sum(order_count) AS orders,
    round(sum(total_revenue) / sum(order_count), 2) AS aov
FROM lakehouse.daily_sales
GROUP BY category_level1, category_level2
ORDER BY revenue DESC
LIMIT 10;

-- Daily Revenue Trend (Last 30 days)
SELECT
    sale_date,
    sum(total_revenue) AS daily_revenue,
    sum(order_count) AS daily_orders
FROM lakehouse.daily_sales
WHERE sale_date >= today() - 30
GROUP BY sale_date
ORDER BY sale_date;

-- Revenue by Day of Week
SELECT
    toDayOfWeek(sale_date) AS day_of_week,
    CASE toDayOfWeek(sale_date)
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_name,
    sum(total_revenue) AS revenue,
    round(avg(total_revenue), 2) AS avg_daily_revenue
FROM lakehouse.daily_sales
GROUP BY day_of_week
ORDER BY day_of_week;

-- =============================================================================
-- 2. FUNNEL ANALYSIS
-- =============================================================================

-- Daily Funnel Metrics
SELECT
    analysis_date,
    views,
    carts,
    purchases,
    view_to_cart_rate,
    cart_to_purchase_rate,
    overall_conversion_rate
FROM lakehouse.funnel_analysis
ORDER BY analysis_date DESC
LIMIT 30;

-- Average Conversion Rates
SELECT
    round(avg(view_to_cart_rate), 2) AS avg_view_to_cart,
    round(avg(cart_to_purchase_rate), 2) AS avg_cart_to_purchase,
    round(avg(overall_conversion_rate), 4) AS avg_overall_conversion,
    round(avg(avg_revenue_per_purchaser), 2) AS avg_revenue_per_purchaser
FROM lakehouse.funnel_analysis;

-- Funnel by Month
SELECT
    analysis_year,
    analysis_month,
    sum(views) AS total_views,
    sum(carts) AS total_carts,
    sum(purchases) AS total_purchases,
    round(sum(carts) * 100.0 / sum(views), 2) AS view_to_cart_rate,
    round(sum(purchases) * 100.0 / sum(carts), 2) AS cart_to_purchase_rate
FROM lakehouse.funnel_analysis
GROUP BY analysis_year, analysis_month
ORDER BY analysis_year, analysis_month;

-- =============================================================================
-- 3. CUSTOMER SEGMENTATION (RFM)
-- =============================================================================

-- Customer Segment Distribution
SELECT
    customer_segment,
    count() AS customer_count,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS percentage,
    round(avg(monetary), 2) AS avg_monetary,
    round(avg(frequency), 1) AS avg_frequency,
    round(avg(recency), 1) AS avg_recency
FROM lakehouse.customer_rfm
GROUP BY customer_segment
ORDER BY customer_count DESC;

-- Top Customers (Champions)
SELECT
    user_id,
    customer_segment,
    rfm_score,
    monetary,
    frequency,
    recency,
    avg_order_value
FROM lakehouse.customer_rfm
WHERE customer_segment = 'Champions'
ORDER BY monetary DESC
LIMIT 20;

-- At-Risk Customers (Need retention campaigns)
SELECT
    user_id,
    customer_segment,
    monetary,
    frequency,
    recency,
    last_purchase_date
FROM lakehouse.customer_rfm
WHERE customer_segment IN ('At Risk', 'Cant Lose Them')
ORDER BY monetary DESC
LIMIT 50;

-- RFM Score Distribution
SELECT
    rfm_score,
    count() AS customer_count,
    round(sum(monetary), 2) AS total_revenue,
    round(avg(monetary), 2) AS avg_monetary
FROM lakehouse.customer_rfm
GROUP BY rfm_score
ORDER BY rfm_score DESC;

-- =============================================================================
-- 4. PRODUCT PERFORMANCE
-- =============================================================================

-- Top 20 Products by Revenue
SELECT
    product_id,
    brand,
    category_level1,
    purchases,
    total_revenue,
    purchase_rate,
    revenue_rank
FROM lakehouse.product_performance
ORDER BY revenue_rank
LIMIT 20;

-- Products with High View but Low Purchase (Optimization opportunities)
SELECT
    product_id,
    brand,
    category_level1,
    views,
    purchases,
    purchase_rate,
    cart_to_purchase_rate
FROM lakehouse.product_performance
WHERE views > 1000 AND purchase_rate < 0.5
ORDER BY views DESC
LIMIT 20;

-- Category Performance Summary
SELECT
    category_level1,
    count() AS product_count,
    sum(views) AS total_views,
    sum(purchases) AS total_purchases,
    sum(total_revenue) AS total_revenue,
    round(sum(purchases) * 100.0 / sum(views), 4) AS avg_purchase_rate
FROM lakehouse.product_performance
GROUP BY category_level1
ORDER BY total_revenue DESC;

-- Brand Performance
SELECT
    brand,
    count() AS product_count,
    sum(purchases) AS total_purchases,
    sum(total_revenue) AS total_revenue,
    round(avg(purchase_rate), 4) AS avg_purchase_rate
FROM lakehouse.product_performance
WHERE brand != 'Unknown'
GROUP BY brand
ORDER BY total_revenue DESC
LIMIT 20;

-- =============================================================================
-- 5. TIME-BASED ANALYSIS
-- =============================================================================

-- Hourly Traffic Pattern (if hourly_traffic table exists)
-- SELECT
--     event_hour,
--     sum(event_count) AS total_events,
--     sum(unique_users) AS unique_users,
--     sum(revenue) AS revenue
-- FROM lakehouse.hourly_traffic
-- GROUP BY event_hour
-- ORDER BY event_hour;

-- Week-over-Week Growth
WITH weekly_sales AS (
    SELECT
        toStartOfWeek(sale_date) AS week_start,
        sum(total_revenue) AS weekly_revenue
    FROM lakehouse.daily_sales
    GROUP BY week_start
)
SELECT
    week_start,
    weekly_revenue,
    lagInFrame(weekly_revenue) OVER (ORDER BY week_start) AS prev_week_revenue,
    round(
        (weekly_revenue - lagInFrame(weekly_revenue) OVER (ORDER BY week_start)) * 100.0 /
        lagInFrame(weekly_revenue) OVER (ORDER BY week_start),
        2
    ) AS wow_growth_pct
FROM weekly_sales
ORDER BY week_start;

-- =============================================================================
-- 6. EXECUTIVE SUMMARY QUERIES
-- =============================================================================

-- KPI Summary
SELECT
    -- Revenue KPIs
    (SELECT sum(total_revenue) FROM lakehouse.daily_sales) AS total_revenue,
    (SELECT sum(order_count) FROM lakehouse.daily_sales) AS total_orders,
    (SELECT round(sum(total_revenue) / sum(order_count), 2) FROM lakehouse.daily_sales) AS avg_order_value,
    
    -- Customer KPIs
    (SELECT count() FROM lakehouse.customer_rfm) AS total_customers,
    (SELECT count() FROM lakehouse.customer_rfm WHERE customer_segment = 'Champions') AS champion_customers,
    (SELECT round(avg(monetary), 2) FROM lakehouse.customer_rfm) AS avg_customer_ltv,
    
    -- Conversion KPIs
    (SELECT round(avg(overall_conversion_rate), 4) FROM lakehouse.funnel_analysis) AS avg_conversion_rate,
    (SELECT round(avg(cart_to_purchase_rate), 2) FROM lakehouse.funnel_analysis) AS avg_cart_conversion;
