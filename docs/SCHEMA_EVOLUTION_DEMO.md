# Schema Evolution Demo - Apache Iceberg

## Overview

This document demonstrates how Apache Iceberg supports **Schema Evolution** - the ability to modify table schemas without rewriting data or causing downtime.

## What is Schema Evolution?

Schema Evolution allows you to:
- Add new columns
- Drop columns
- Rename columns
- Change column types (with restrictions)
- Reorder columns

All without rewriting existing data or breaking downstream consumers.

## Use Case: Adding payment_method Column

### Scenario
After the initial data pipeline was deployed, business requiresTracking payment methods (credit_card, debit_card, paypal, etc.) for better payment analytics.

### Traditional Approach (WITHOUT Iceberg)
1. Stop data ingestion
2. Create new table with additional column
3. Copy all existing data to new table
4. Update all queries/applications
5. Restart ingestion
6. **Downtime: Hours or days for large datasets**

### Iceberg Approach (WITH Schema Evolution)
1. Execute single ALTER TABLE command
2. Old data shows NULL for new column automatically
3. New data includes payment_method value
4. **Downtime: ZERO**

## Step-by-Step Demo

### Step 1: Check Current Schema

```sql
-- View current schema
DESCRIBE iceberg.silver.events_cleaned;

-- Current columns (before evolution):
-- - event_id
-- - event_time
-- - event_type
-- - product_id
-- - user_id
-- - price
-- - category_level1
-- - category_level2
-- - brand
-- - ... (other columns)
```

### Step 2: Add New Column

```sql
-- Add payment_method column to existing table
ALTER TABLE iceberg.silver.events_cleaned 
ADD COLUMN payment_method STRING 
COMMENT 'Payment method used for purchase';
```

**What happens:**
- Metadata updated immediately (< 1 second)
- NO data files rewritten
- Existing data automatically has NULL for new column
- New ingestions can populate payment_method

### Step 3: Verify Schema Update

```sql
-- Check updated schema
DESCRIBE iceberg.silver.events_cleaned;

-- New schema includes:
-- - ... (all previous columns)
-- - payment_method STRING  -- NEW!
```

### Step 4: Query Demonstrates Backward Compatibility

```sql
-- Query old data (NULL payment_method)
SELECT event_id, event_type, payment_method
FROM iceberg.silver.events_cleaned
WHERE event_date < '2020-01-01'
LIMIT 5;

-- Result:
-- event_id | event_type | payment_method
-- ---------|------------|---------------
-- abc123   | purchase   | NULL
-- abc124   | purchase   | NULL

-- Query new data (with payment_method)
SELECT event_id, event_type, payment_method
FROM iceberg.silver.events_cleaned
WHERE event_date >= '2020-04-01'
LIMIT 5;

-- Result (hypothetical after new data ingestion):
-- event_id | event_type | payment_method
-- ---------|------------|---------------
-- xyz789   | purchase   | credit_card
-- xyz790   | purchase   | paypal
```

## Key Benefits

### 1. Zero Downtime
- Table remains queryable during schema change
- No need to stop ingestion pipeline
- No need to recreate table

### 2. No Data Rewriting
- Metadata-only operation
- Instant completion even for petabyte-scale tables
- Cost effective (no compute/storage for rewrite)

### 3. Backward Compatibility
- Old queries still work (new column optional)
- Old data safe (automatically NULL for new columns)
- Gradual migration possible

### 4. Version Control
- Schema changes tracked in metadata
- Can view schema at any historical timestamp
- Enables Time Travel to previous schemas

## Implementation in Our Pipeline

### Option 1: Spark ALTER TABLE (Production)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SchemaEvolution").getOrCreate()

# Add column via Spark SQL
spark.sql("""
    ALTER TABLE iceberg.silver.events_cleaned 
    ADD COLUMN payment_method STRING 
    COMMENT 'Payment method: credit_card, debit_card, paypal, bank_transfer, cash'
""")

print("Schema evolution completed!")
```

### Option 2: Direct Iceberg API (Advanced)

```python
from org.apache.iceberg import Schema, Table
from org.apache.iceberg.types import Types

# Get table
table = catalog.loadTable(TableIdentifier.of("silver", "events_cleaned"))

# Update schema
table.updateSchema()
    .addColumn("payment_method", Types.StringType.get())
    .commit()
```

### Option 3: dbt Model (Recommended for Analytics)

```sql
-- dbt/models/silver/events_cleaned_v2.sql
{{ config(materialized='incremental') }}

SELECT 
    *,
    -- Add logic for payment_method based on business rules
    CASE 
        WHEN price > 1000 THEN 'bank_transfer'
        WHEN brand IN ('premium_brands') THEN 'credit_card'
        ELSE 'debit_card'
    END as payment_method_derived
FROM {{ ref('events_cleaned') }}
```

## Testing Schema Evolution

### Test 1: Add Column

```sql
-- Should succeed
ALTER TABLE iceberg.silver.events_cleaned 
ADD COLUMN shipping_address STRING;
```

### Test 2: Drop Column

```sql
-- Should succeed  
ALTER TABLE iceberg.silver.events_cleaned 
DROP COLUMN shipping_address;
```

### Test 3: Rename Column

```sql
-- Should succeed
ALTER TABLE iceberg.silver.events_cleaned 
RENAME COLUMN category_level1 TO category_primary;
```

### Test 4: Change Type (Supported)

```sql
-- Widening cast - should succeed
ALTER TABLE iceberg.silver.events_cleaned 
ALTER COLUMN price TYPE DECIMAL(20,4);  -- was DOUBLE
```

### Test 5: Change Type (Unsupported)

```sql
-- Narrowing cast - will FAIL
ALTER TABLE iceberg.silver.events_cleaned 
ALTER COLUMN price TYPE INT;  -- Cannot narrow DOUBLE to INT
```

## Comparison with Other Formats

| Feature | Iceberg | Hive/Parquet | Delta Lake |
|---------|---------|--------------|----------|
| Add Column | Metadata only | Full rewrite | Metadata only |
| Drop Column | Metadata only | Full rewrite | Metadata only |
| Rename Column | Supported | Not supported | Supported |
| Type Evolution | Controlled | Not supported | Controlled |
| Downtime | Zero | Hours/days | Zero |
| Cost | Low | High | Low |

## Best Practices

1. **Plan Schema Changes**
   - Document changes in ticket system
   - Communicate to downstream consumers
   - Test in dev environment first

2. **Use Comments**
   ```sql
   ALTER TABLE table_name 
   ADD COLUMN new_col TYPE COMMENT 'Business meaning and source';
   ```

3. **Version Control**
   - Keep ALTER TABLE commands in git
   - Use migration scripts (like Flyway/Liquibase)

4. **Monitor Impact**
   - Check query performance after schema change
   - Monitor storage metrics
   - Review downstream dashboard queries

5. **Gradual Rollout**
   - Add column first (default NULL)
   - Update ingestion to populate column
   - After data filled, add NOT NULL constraint if needed

## Conclusion

Apache Iceberg's Schema Evolution enables agile data modeling:
- Fast iteration on data model
- No downtime for schema changes
- Cost-effective (no data rewriting)
- Safe (backward compatible)

This makes Iceberg ideal for modern data lakehouses where business requirements change frequently.

## Further Reading

- [Iceberg Schema Evolution Docs](https://iceberg.apache.org/docs/latest/evolution/)
- [Migration from Hive to Iceberg](https://iceberg.apache.org/docs/latest/hive-migration/)
- [Best Practices for Schema Design](https://iceberg.apache.org/docs/latest/schemas/)
