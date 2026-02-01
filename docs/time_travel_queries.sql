-- ============================================================================
-- TIME TRAVEL DEMO QUERIES
-- ============================================================================
-- Demonstrates Iceberg Time Travel features using ClickHouse
-- ============================================================================

-- PART 1: View Table Snapshots History
-- ============================================================================
-- Note: This requires Iceberg REST catalog with history tracking
-- For direct Iceberg engine in ClickHouse, snapshot metadata might not be accessible

-- Query current data
SELECT 
    'Current Data' as dataset,
    COUNT(*) as total_records,
    SUM(total_revenue) as total_revenue,
    MIN(event_date) as min_date,
    MAX(event_date) as max_date
FROM lakehouse.daily_sales;


-- PART 2: Query Historical Data (if supported by ClickHouse + Iceberg)
-- ============================================================================
-- Note: Time Travel syntax varies by query engine
-- ClickHouse with Iceberg might use different syntax than Spark

-- Standard approach for ClickHouse: 
-- Use Iceberg snapshots if available via REST catalog


-- PART 3: Compare Current vs Historical Data
-- ============================================================================

-- Current revenue by category
WITH current_revenue AS (
    SELECT 
        category_level1,
        SUM(total_revenue) as revenue,
        COUNT(*) as order_count
    FROM lakehouse.daily_sales
    WHERE event_date >= '2020-01-01'
    GROUP BY category_level1
)
SELECT 
    category_level1,
    revenue,
    order_count,
    ROUND(revenue / order_count, 2) as avg_order_value
FROM current_revenue
ORDER BY revenue DESC;


-- PART 4: Audit Trail Example
-- ============================================================================

-- Find sales pattern changes over time periods
SELECT 
    CASE 
        WHEN event_date < '2019-11-01' THEN '2019-10 (Oct)'
        WHEN event_date < '2019-12-01' THEN '2019-11 (Nov)' 
        WHEN event_date < '2020-01-01' THEN '2019-12 (Dec)'
        WHEN event_date < '2020-02-01' THEN '2020-01 (Jan)'
        WHEN event_date < '2020-03-01' THEN '2020-02 (Feb)'
        WHEN event_date < '2020-04-01' THEN '2020-03 (Mar)'
        ELSE '2020-04 (Apr)'
    END as month_period,
    COUNT(DISTINCT event_date) as days_with_sales,
    SUM(order_count) as total_orders,
    SUM(total_revenue) as total_revenue,
    SUM(unique_customers) as unique_customers
FROM lakehouse.daily_sales
GROUP BY month_period
ORDER BY month_period;


-- PART 5: Data Quality Check
-- ============================================================================

-- Identify anomalies or data quality issues
SELECT 
    event_date,
    category_level1,
    order_count,
    total_revenue,
    ROUND(total_revenue / NULLIF(order_count, 0), 2) as avg_order_value,
    -- Flag potential issues
    CASE 
        WHEN total_revenue = 0 AND order_count > 0 THEN 'Zero revenue with orders'
        WHEN order_count = 0 AND total_revenue > 0 THEN 'Revenue without orders'
        WHEN total_revenue / NULLIF(order_count, 0) > 10000 THEN 'Unusually high AOV'
        WHEN total_revenue / NULLIF(order_count, 0) < 1 THEN 'Unusually low AOV'
        ELSE 'Normal'
    END as data_quality_flag
FROM lakehouse.daily_sales
WHERE category_level1 != 'uncategorized'
HAVING data_quality_flag != 'Normal'
ORDER BY event_date DESC
LIMIT 20;


-- ============================================================================
-- TIME TRAVEL WITH SPARK SQL (Reference)
-- ============================================================================
-- The following queries work in Spark, shown here for reference

/*
-- Query data as of specific timestamp
SELECT * FROM iceberg.gold.daily_sales
FOR SYSTEM_TIME AS OF '2026-01-15 12:00:00'
WHERE event_date = '2026-01-15';

-- Query data at specific snapshot
SELECT * FROM iceberg.gold.daily_sales
VERSION AS OF 123456789012345678;

-- View snapshot history  
SELECT 
    snapshot_id,
    committed_at,
    operation,
    summary
FROM iceberg.gold.daily_sales.snapshots
ORDER BY committed_at DESC;

-- Rollback to previous snapshot
CALL iceberg.system.rollback_to_timestamp(
    'iceberg.gold.daily_sales',
    TIMESTAMP '2026-02-01 09:00:00'
);

-- Expire old snapshots (cleanup)
CALL iceberg.system.expire_snapshots(
    table => 'iceberg.gold.daily_sales',
    older_than => TIMESTAMP '2026-01-25 00:00:00',
    retain_last => 10
);
*/


-- ============================================================================
-- PRACTICAL SCENARIO: Debug Revenue Drop
-- ============================================================================

-- Step 1: Find sudden changes in daily revenue
WITH daily_revenue AS (
    SELECT 
        event_date,
        SUM(total_revenue) as revenue
    FROM lakehouse.daily_sales
    GROUP BY event_date
    ORDER BY event_date
),
revenue_with_prev AS (
    SELECT 
        event_date,
        revenue,
        LAG(revenue) OVER (ORDER BY event_date) as prev_revenue
    FROM daily_revenue
)
SELECT 
    event_date,
    revenue,
    prev_revenue,
    revenue - prev_revenue as change,
    ROUND((revenue - prev_revenue) / NULLIF(prev_revenue, 0) * 100, 2) as pct_change
FROM revenue_with_prev
WHERE ABS((revenue - prev_revenue) / NULLIF(prev_revenue, 0)) > 0.2  -- 20% change
ORDER BY ABS(pct_change) DESC
LIMIT 10;


-- Step 2: Investigate specific day's breakdown
SELECT 
    category_level1,
    SUM(order_count) as orders,
    SUM(total_revenue) as revenue,
    SUM(unique_customers) as customers,
    ROUND(SUM(total_revenue) / SUM(order_count), 2) as avg_order_value
FROM lakehouse.daily_sales
WHERE event_date = '2019-10-01'  -- Replace with date of interest
GROUP BY category_level1
ORDER BY revenue DESC;


-- ============================================================================
-- NOTES FOR TIME TRAVEL IN PRODUCTION
-- ============================================================================

/*
TIME TRAVEL CAPABILITIES BY ENGINE:

1. SPARK SQL (Full Support):
   - FOR SYSTEM_TIME AS OF timestamp
   - VERSION AS OF snapshot_id
   - Access to .snapshots, .files, .history metadata tables
   - Rollback procedures available

2. CLICKHOUSE + ICEBERG (Limited Support):
   - Direct queries via Iceberg engine read current snapshot
   - Time Travel may require Iceberg REST catalog integration
   - Alternate approach: Maintain separate tables per time period

3. DBT + CLICKHOUSE (Current Setup):
   - dbt models create views on current Iceberg data
   - Time Travel requires Spark or Iceberg-native tools
   - Alternative: Create dated materialized views

RECOMMENDATIONS:
- Use Spark for Time Travel operations
- ClickHouse for fast analytical queries on current data
- Document important snapshot IDs for rollback scenarios
- Schedule regular snapshot expiration to manage storage
*/
