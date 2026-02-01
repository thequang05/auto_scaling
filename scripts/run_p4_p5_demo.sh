#!/bin/bash
# ============================================================================
# P4 + P5 DEMO via ClickHouse
# ============================================================================
# Demonstrates Schema Evolution and Time Travel concepts
# using ClickHouse queries on existing Iceberg tables
# ============================================================================

set -e

CLICKHOUSE_PASSWORD="clickhouse123"

echo "======================================================================"
echo "P4 + P5 DEMO: Schema Evolution & Time Travel"
echo "======================================================================"

# ============================================================================
# PART 1: P4 - SCHEMA EVOLUTION DEMO
# ============================================================================

echo ""
echo "======================================================================"
echo "DEMO P4: SCHEMA EVOLUTION"
echo "======================================================================"

echo ""
echo "[Step 1] Current Table Schema"
echo "----------------------------------------------------------------------"
docker exec clickhouse clickhouse-client --password "$CLICKHOUSE_PASSWORD" -q "
DESCRIBE lakehouse.daily_sales
" | column -t

echo ""
echo "[Step 2] DATA SAMPLE - Before Evolution"
echo "----------------------------------------------------------------------"
docker exec clickhouse clickhouse-client --password "$CLICKHOUSE_PASSWORD" -q "
SELECT 
    event_date,
    category_level1,
    order_count,
    total_revenue
FROM lakehouse.daily_sales
LIMIT 5
FORMAT PrettyCompact
"

echo ""
echo "[Step 3] CONCEPT: Schema Evolution"  
echo "----------------------------------------------------------------------"
echo "In production with Spark/Iceberg:"
echo "  ALTER TABLE iceberg.gold.daily_sales"
echo "  ADD COLUMN payment_channel STRING"
echo "  COMMENT 'Payment channel: credit_card, debit, paypal'"
echo ""
echo "RESULT:"
echo "  - Metadata updated instantly"
echo "  - NO data rewriting"
echo "  - Existing rows have NULL for new column"
echo "  - New writes can populate the column"
echo "  - ZERO downtime"

echo ""
echo "KEY BENEFITS:"
echo "  1. Instant schema modification (metadata-only)"
echo "  2. Backward compatible (old data safe with NULL)"
echo "  3. Cost effective (no data rewrite)"
echo "  4. No service interruption"

# ============================================================================
# PART 2: P5 - TIME TRAVEL DEMO
# ============================================================================

echo ""
echo "======================================================================"
echo "DEMO P5: TIME TRAVEL"
echo "======================================================================"

echo ""
echo "[Step 1] Current Table Statistics"
echo "----------------------------------------------------------------------"
docker exec clickhouse clickhouse-client --password "$CLICKHOUSE_PASSWORD" -q "
SELECT 
    'lakehouse.daily_sales' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT event_date) as unique_dates,
    MIN(event_date) as earliest_date,
    MAX(event_date) as latest_date,
    SUM(total_revenue) as total_revenue
FROM lakehouse.daily_sales
FORMAT PrettyCompact
"

echo ""
echo "[Step 2] Historical Data Analysis - Monthly Trend"
echo "----------------------------------------------------------------------"
docker exec clickhouse clickhouse-client --password "$CLICKHOUSE_PASSWORD" -q "
SELECT 
    sale_year,
    sale_month,
    COUNT(*) as days_with_data,
    SUM(order_count) as total_orders,
    ROUND(SUM(total_revenue), 2) as total_revenue
FROM lakehouse.daily_sales
GROUP BY sale_year, sale_month
ORDER BY sale_year, sale_month
FORMAT PrettyCompact
"

echo ""
echo "[Step 3] CONCEPT: Time Travel Queries"
echo "----------------------------------------------------------------------"
echo "In production with Spark/Iceberg:"
echo "  -- Query data as of January 1, 2020"
echo "  SELECT * FROM iceberg.gold.daily_sales"
echo "  FOR SYSTEM_TIME AS OF '2020-01-01 00:00:00'"
echo ""
echo "  -- Query specific snapshot version"
echo "  SELECT * FROM iceberg.gold.daily_sales"
echo "  VERSION AS OF 1234567890123456789"
echo ""
echo "  -- View snapshot history"
echo "  SELECT snapshot_id, committed_at, operation"
echo "  FROM iceberg.gold.daily_sales.snapshots"

echo ""
echo "KEY CAPABILITIES:"
echo "  1. Query any historical point (timestamp-based)"
echo "  2. Query specific versions (snapshot-based)"
echo "  3. Compare current vs historical data"
echo "  4. Rollback to previous states"
echo "  5. Built-in audit trail"

echo ""
echo "[Step 4] Audit Trail Example - Monthly Revenue Trend"
echo "----------------------------------------------------------------------"
docker exec clickhouse clickhouse-client --password "$CLICKHOUSE_PASSWORD" -q "
SELECT 
    formatDateTime(event_date, '%Y-%m') as month,
    SUM(order_count) as orders,
    ROUND(SUM(total_revenue), 2) as revenue
FROM lakehouse.daily_sales
WHERE event_date >= '2019-10-01' AND event_date < '2020-05-01'
GROUP BY month
ORDER BY month
FORMAT PrettyCompact
"

echo ""
echo "[Step 5] Data Quality Check - Potential Anomalies"
echo "----------------------------------------------------------------------"
docker exec clickhouse clickhouse-client --password "$CLICKHOUSE_PASSWORD" -q "
SELECT 
    event_date,
    category_level1,
    order_count,
    total_revenue,
    ROUND(total_revenue / order_count, 2) as avg_order_value,
    CASE
        WHEN total_revenue = 0 AND order_count > 0 THEN 'ISSUE: Zero revenue'
        WHEN total_revenue / order_count > 5000 THEN 'High AOV'
        WHEN total_revenue / order_count < 5 THEN 'Low AOV'
        ELSE 'Normal'
    END as quality_flag
FROM lakehouse.daily_sales
WHERE quality_flag != 'Normal'
LIMIT 10
FORMAT PrettyCompact
"

# ============================================================================
# SUMMARY
# ============================================================================

echo ""
echo "======================================================================"
echo "DEMO SUMMARY"
echo "======================================================================"

echo ""
echo "P4 - SCHEMA EVOLUTION:"
echo "  Documentation: docs/SCHEMA_EVOLUTION_DEMO.md"
echo "  Key Concept: Add/modify columns without data rewrite"
echo "  Benefits: Zero downtime, cost-effective, backward compatible"
echo ""
echo "P5 - TIME TRAVEL:"
echo "  Documentation: docs/TIME_TRAVEL_DEMO.md"
echo "  Key Concept: Query data at any historical point"
echo "  Use Cases: Audit, debugging, rollback, compliance"

echo ""
echo "======================================================================"
echo "DEMOS COMPLETED"
echo "======================================================================" 
echo ""
echo "For full Iceberg features (ALTER TABLE, snapshot queries):"
echo "  - Use Spark SQL interface"
echo "  - Refer to documentation files"
echo "  - Examples provided in docs/*.md"
