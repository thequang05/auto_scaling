# Time Travel Demo - Apache Iceberg

## Overview

**Time Travel** is a powerful feature of Apache Iceberg that allows you to query data as it existed at any point in time or at any specific snapshot version. This enables auditing, debugging, and recovery from data errors without maintaining separate backup copies.

## What is Time Travel?

Time Travel allows you to:
- Query historical data at any timestamp
- Query data at specific snapshot versions
- Compare current vs historical data
- Rollback to previous states
- Debug data pipeline issues
- Satisfy audit and compliance requirements

All without maintaining separate backup tables or complex versioning systems.

## Key Concepts

### 1. Snapshots
- Every write operation creates a new snapshot
- Snapshots are immutable
- Each snapshot ID represents a version of the table
- Metadata tracks all snapshots

### 2. Metadata Retention
- Old snapshots retained based on retention policy
- Default: 5 days for snapshot metadata
- Configurable via table properties

### 3. Query Methods
- **Timestamp-based**: Query as of specific time
- **Snapshot-based**: Query specific version by ID
- **Version-based**: Query by sequential version number

## Use Cases

### Use Case 1: Audit Trail
**Scenario**: Compliance team needs to verify data state from last month.

```sql
-- Query data as it existed on January 1, 2026
SELECT COUNT(*) as total_sales, SUM(total_revenue) as revenue
FROM iceberg.gold.daily_sales
FOR SYSTEM_TIME AS OF '2026-01-01 00:00:00'
WHERE event_date = '2025-12-31';
```

### Use Case 2: Debug Data Issues
**Scenario**: Dashboard shows incorrect revenue today, need to find when it changed.

```sql
-- Compare current vs yesterday's data
WITH current_data AS (
    SELECT category_level1, SUM(total_revenue) as revenue
    FROM iceberg.gold.daily_sales
    WHERE event_date = '2026-02-01'
    GROUP BY category_level1
),
yesterday_data AS (
    SELECT category_level1, SUM(total_revenue) as revenue
    FROM iceberg.gold.daily_sales
    FOR SYSTEM_TIME AS OF '2026-01-31 23:59:59'
    WHERE event_date = '2026-02-01'
    GROUP BY category_level1
)
SELECT 
    c.category_level1,
    c.revenue as current_revenue,
    y.revenue as yesterday_revenue,
    c.revenue - y.revenue as difference
FROM current_data c
LEFT JOIN yesterday_data y ON c.category_level1 = y.category_level1
WHERE c.revenue != COALESCE(y.revenue, 0);
```

### Use Case 3: Rollback Data Error
**Scenario**: Bad data ingested at 10 AM, need to restore to 9 AM state.

```sql
-- Method 1: Create new table from historical snapshot
CREATE TABLE iceberg.gold.daily_sales_backup AS
SELECT * FROM iceberg.gold.daily_sales
FOR SYSTEM_TIME AS OF '2026-02-01 09:00:00';

-- Method 2: Rollback using Iceberg API (in Spark)
-- spark.sql("CALL iceberg.system.rollback_to_timestamp('iceberg.gold.daily_sales', TIMESTAMP '2026-02-01 09:00:00')")
```

### Use Case 4: Data Recovery
**Scenario**: Accidentally deleted records, need to recover them.

```sql
-- Find deleted records by comparing current vs 1 hour ago
SELECT h.*
FROM iceberg.gold.daily_sales FOR SYSTEM_TIME AS OF '2026-02-01 14:00:00' h
LEFT JOIN iceberg.gold.daily_sales c 
    ON h.event_date = c.event_date 
    AND h.category_level1 = c.category_level1
WHERE c.event_date IS NULL;

-- Restore deleted records
INSERT INTO iceberg.gold.daily_sales
SELECT * FROM iceberg.gold.daily_sales
FOR SYSTEM_TIME AS OF '2026-02-01 14:00:00'
WHERE event_date = '2026-01-31';
```

## SQL Syntax Examples

### 1. Query by Timestamp

```sql
-- Standard SQL syntax (Spark 3.3+)
SELECT * FROM iceberg.gold.daily_sales
FOR SYSTEM_TIME AS OF '2026-01-15 12:30:00'
WHERE event_date >= '2026-01-01';

-- Alternative syntax
SELECT * FROM iceberg.gold.daily_sales
TIMESTAMP AS OF '2026-01-15 12:30:00'
WHERE event_date >= '2026-01-01';
```

### 2. Query by Snapshot ID

```sql
-- First, find snapshot ID
SELECT snapshot_id, committed_at, summary
FROM iceberg.gold.daily_sales.snapshots
ORDER BY committed_at DESC
LIMIT 10;

-- Query specific snapshot
SELECT * FROM iceberg.gold.daily_sales
VERSION AS OF 1234567890123456789;
```

### 3. View Table History

```sql
-- View all snapshots (metadata)
SELECT 
    snapshot_id,
    parent_id,
    committed_at,
    operation,
    summary['added-records'] as added_records,
    summary['deleted-records'] as deleted_records,
    summary['total-records'] as total_records
FROM iceberg.gold.daily_sales.snapshots
ORDER BY committed_at DESC;

-- Example output:
-- snapshot_id           | committed_at        | operation | added_records | total_records
-- ----------------------|---------------------|-----------|---------------|---------------
-- 123456789012345678    | 2026-02-01 15:00:00 | append    | 100           | 1205
-- 123456789012345677    | 2026-02-01 14:00:00 | append    | 50            | 1105
-- 123456789012345676    | 2026-02-01 13:00:00 | append    | 55            | 1055
```

### 4. View Data Files

```sql
-- View physical files for specific snapshot
SELECT 
    file_path,
    file_size_in_bytes,
    record_count,
    partition
FROM iceberg.gold.daily_sales.files
WHERE snapshot_id = 123456789012345678;
```

## Spark DataFrame API Examples

### Example 1: Read Historical Data

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read data as of specific timestamp
df_historical = spark.read \
    .option("as-of-timestamp", "2026-01-01 00:00:00") \
    .table("iceberg.gold.daily_sales")

df_historical.show()

# Read data as of specific snapshot
df_snapshot = spark.read \
    .option("snapshot-id", "123456789012345678") \
    .table("iceberg.gold.daily_sales")

df_snapshot.count()
```

### Example 2: Compare Versions

```python
# Current data
df_current = spark.table("iceberg.gold.daily_sales")

# Data from 1 hour ago
df_old = spark.read \
    .option("as-of-timestamp", "2026-02-01 14:00:00") \
    .table("iceberg.gold.daily_sales")

# Find differences
from pyspark.sql.functions import col

df_deleted = df_old.subtract(df_current)
df_added = df_current.subtract(df_old)

print(f"Deleted records: {df_deleted.count()}")
print(f"Added records: {df_added.count()}")
```

### Example 3: Rollback to Previous State

```python
# Option 1: Using Spark SQL
spark.sql("""
    CALL iceberg.system.rollback_to_timestamp(
        'iceberg.gold.daily_sales', 
        TIMESTAMP '2026-02-01 09:00:00'
    )
""")

# Option 2: Using Spark SQL with snapshot ID
spark.sql("""
    CALL iceberg.system.rollback_to_snapshot(
        'iceberg.gold.daily_sales', 
        123456789012345676
    )
""")

# Option 3: Set current snapshot
spark.sql("""
    CALL iceberg.system.set_current_snapshot(
        'iceberg.gold.daily_sales', 
        123456789012345676
    )
""")
```

## Advanced Use Cases

### Incremental Processing with Time Travel

```python
# Process only new data since last run
last_processed_time = "2026-02-01 14:00:00"

df_new_data = spark.sql(f"""
    SELECT * FROM iceberg.gold.daily_sales
    WHERE _metadata.committed_at > TIMESTAMP '{last_processed_time}'
""")

# Process new data
df_new_data.write.mode("append").saveAsTable("processed_results")
```

### Data Quality Monitoring

```python
# Check data quality trend over time
timestamps = [
    "2026-02-01 10:00:00",
    "2026-02-01 11:00:00", 
    "2026-02-01 12:00:00",
    "2026-02-01 13:00:00"
]

for ts in timestamps:
    df = spark.read \
        .option("as-of-timestamp", ts) \
        .table("iceberg.gold.daily_sales")
    
    null_count = df.filter(col("total_revenue").isNull()).count()
    print(f"{ts}: NULL revenue count = {null_count}")
```

## ClickHouse Time Travel

ClickHouse with Iceberg engine also supports Time Travel:

```sql
-- Query historical data via ClickHouse
SELECT COUNT(*), SUM(total_revenue)
FROM lakehouse.daily_sales
SETTINGS iceberg_snapshot_id = 123456789012345678;

-- Note: Timestamp-based queries may require Iceberg REST catalog features
```

## Retention Configuration

### Set Snapshot Retention

```sql
-- Keep snapshots for 7 days (default is 5)
ALTER TABLE iceberg.gold.daily_sales 
SET TBLPROPERTIES (
    'history.expire.max-snapshot-age-ms' = '604800000'  -- 7 days in ms
);

-- Keep minimum 10 snapshots even if older than retention period
ALTER TABLE iceberg.gold.daily_sales 
SET TBLPROPERTIES (
    'history.expire.min-snapshots-to-keep' = '10'
);
```

### Expire Old Snapshots Manually

```python
# Spark SQL procedure to expire old snapshots
spark.sql("""
    CALL iceberg.system.expire_snapshots(
        table => 'iceberg.gold.daily_sales',
        older_than => TIMESTAMP '2026-01-25 00:00:00',
        retain_last => 5
    )
""")
```

## Best Practices

### 1. Retention Planning
- Set retention based on compliance requirements
- Balance storage costs vs audit needs
- Consider data volume when setting retention

### 2. Performance Optimization
- Time Travel queries read historical data files
- May be slower than current data queries
- Use partition pruning where possible

```sql
-- Good: Partition pruning works
SELECT * FROM iceberg.gold.daily_sales
FOR SYSTEM_TIME AS OF '2026-01-15 00:00:00'
WHERE event_date = '2026-01-14';  -- Partition filter

-- Less optimal: Full scan needed
SELECT * FROM iceberg.gold.daily_sales
FOR SYSTEM_TIME AS OF '2026-01-15 00:00:00'
WHERE total_revenue > 1000;
```

### 3. Snapshot Management
- Regularly expire old snapshots to save storage
- Monitor snapshot count and metadata size
- Test rollback procedures before incidents

### 4. Documentation
- Document snapshot IDs for important pipeline runs
- Log timestamps of significant data changes
- Maintain runbook for rollback procedures

## Comparison with Other Approaches

| Feature | Iceberg Time Travel | Traditional Backups | SCD Type 2 |
|---------|-------------------|---------------------|-----------|
| Storage Overhead | Low (shared files) | High (full copies) | Medium (history cols) |
| Query Speed | Fast | Slow (restore first) | Fast |
| Granularity | Per-write | Daily/hourly | Per-change |
| Complexity | Low | High | Medium |
| Point-in-time Recovery | Any time | Backup times only | Row-level |

## Troubleshooting

### Issue: "Snapshot not found"
**Cause**: Snapshot expired or never existed  
**Solution**: Check snapshot retention settings

```sql
-- Verify snapshots
SELECT snapshot_id, committed_at 
FROM iceberg.gold.daily_sales.snapshots;

-- Increase retention if needed
ALTER TABLE iceberg.gold.daily_sales 
SET TBLPROPERTIES ('history.expire.max-snapshot-age-ms' = '1209600000'); -- 14 days
```

### Issue: "No data found at timestamp"
**Cause**: Querying before table creation or data ingestion  
**Solution**: Verify table history

```sql
-- Find first snapshot
SELECT MIN(committed_at) as first_snapshot
FROM iceberg.gold.daily_sales.snapshots;
```

### Issue: Slow Time Travel queries
**Cause**: Reading many historical files  
**Solution**: Use partition filters and limit time range

## Practical Example: Data Pipeline Debugging

```python
# Scenario: Revenue dropped unexpectedly, investigate when it happened

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col
from datetime import datetime, timedelta

spark = SparkSession.builder.getOrCreate()

# Check revenue for last 24 hours, hour by hour
base_time = datetime(2026, 2, 1, 15, 0, 0)
results = []

for i in range(24):
    check_time = base_time - timedelta(hours=i)
    
    df = spark.read \
        .option("as-of-timestamp", check_time.strftime("%Y-%m-%d %H:%M:%S")) \
        .table("iceberg.gold.daily_sales")
    
    revenue = df.agg(sum("total_revenue")).collect()[0][0]
    results.append((check_time, revenue))
    
# Print results
for timestamp, revenue in results:
    print(f"{timestamp}: ${revenue:,.2f}")

# Find the hour when revenue dropped
for i in range(len(results)-1):
    diff = results[i][1] - results[i+1][1]
    if abs(diff) > 10000:  # Significant change
        print(f"\nRevenue changed significantly at {results[i][0]}")
        print(f"Change: ${diff:,.2f}")
```

## Conclusion

Apache Iceberg's Time Travel feature provides:
- Simple syntax for complex time-based queries
- Zero-cost data versioning (shared files)
- Powerful debugging and audit capabilities
- Production-ready rollback mechanisms

This makes Iceberg ideal for:
- Financial systems (audit trails)
- Data quality monitoring
- Compliance requirements
- Data pipeline debugging
- Disaster recovery

## Further Reading

- [Iceberg Time Travel Docs](https://iceberg.apache.org/docs/latest/spark-queries/#time-travel)
- [Snapshot Management](https://iceberg.apache.org/docs/latest/spark-procedures/#expire-snapshots)
- [Metadata Tables](https://iceberg.apache.org/docs/latest/spark-queries/#querying-with-sql)
