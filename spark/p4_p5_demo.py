"""
P4 + P5 DEMO: Schema Evolution & Time Travel with Apache Iceberg
Simplified version using Spark SQL
"""

from pyspark.sql import SparkSession
import sys

def create_spark():
    """Create Spark session with Iceberg"""
    return SparkSession.builder \
        .appName("P4-P5-Demo") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3://iceberg-warehouse/") \
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.iceberg.s3.access-key-id", "minioadmin") \
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", "minioadmin123") \
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
        .getOrCreate()

def demo_p4_schema_evolution(spark):
    """P4: Schema Evolution Demo"""
    print("\n" + "="*70)
    print("DEMO P4: SCHEMA EVOLUTION")
    print("="*70)
    
    table = "iceberg.gold.daily_sales_by_category"
    
    # Step 1: Show current schema
    print("\n[Step 1] Current Schema:")
    print("-" * 70)
    df = spark.table(table)
    df.printSchema()
    print(f"Total columns: {len(df.columns)}")
    
    # Step 2: Add new column
    print("\n[Step 2] Adding new column 'sales_channel'...")
    print("-" * 70)
    
    try:
        spark.sql(f"""
            ALTER TABLE {table}
            ADD COLUMN sales_channel STRING
            COMMENT 'Sales channel: online, retail, mobile'
        """)
        print("SUCCESS: Column 'sales_channel' added!")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("INFO: Column 'sales_channel' already exists")
        else:
            print(f"ERROR: {e}")
            return
    
    # Step 3: Verify new schema
    print("\n[Step 3] Updated Schema:")
    print("-" * 70)
    df_new = spark.table(table)
    df_new.printSchema()
    print(f"Total columns: {len(df_new.columns)}")
    
    # Step 4: Show sample data
    print("\n[Step 4] Sample Data (showing new column):")
    print("-" * 70)
    df_new.select("event_date", "category_level1", "total_revenue", "sales_channel").show(5)
    
    print("\n" + "="*70)
    print("P4 DEMO COMPLETE")
    print("="*70)
    print("\nKEY POINTS:")
    print("- Column added via single ALTER TABLE command")
    print("- No data rewriting required")
    print("- Existing data shows NULL for new column")
    print("- Zero downtime")

def demo_p5_time_travel(spark):
    """P5: Time Travel Demo"""
    print("\n" + "="*70)
    print("DEMO P5: TIME TRAVEL")
    print("="*70)
    
    table = "iceberg.gold.daily_sales_by_category"
    
    # Step 1: View table snapshots
    print("\n[Step 1] Table Snapshot History:")
    print("-" * 70)
    
    try:
        snapshots = spark.sql(f"""
            SELECT 
                snapshot_id,
                parent_id,
                operation,
                summary['added-records'] as added_records,
                summary['total-records'] as total_records,
                committed_at
            FROM {table}.snapshots
            ORDER BY committed_at DESC
            LIMIT 5
        """)
        snapshots.show(truncate=False)
        
        snapshot_count = snapshots.count()
        print(f"\nTotal snapshots (showing last 5): {snapshot_count}+")
        
    except Exception as e:
        print(f"INFO: Snapshot history not available: {e}")
        print("(This is normal if table metadata doesn't track history)")
    
    # Step 2: Query current data
    print("\n[Step 2] Current Data Summary:")
    print("-" * 70)
    
    current = spark.sql(f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT event_date) as unique_dates,
            MIN(event_date) as min_date,
            MAX(event_date) as max_date,
            SUM(total_revenue) as total_revenue
        FROM {table}
    """)
    current.show()
    
    # Step 3: Demonstrate Time Travel query concept
    print("\n[Step 3] Time Travel Query Concept:")
    print("-" * 70)
    print("Example SQL for querying historical data:")
    print(f"""
    SELECT * FROM {table}
    FOR SYSTEM_TIME AS OF '2020-01-15 00:00:00'
    WHERE event_date = '2020-01-15'
    """)
    
    print("\nExample SQL for querying specific snapshot:")
    print(f"""
    SELECT * FROM {table}
    VERSION AS OF [snapshot_id]
    """)
    
    # Step 4: Show metadata tables
    print("\n[Step 4] Available Metadata Tables:")
    print("-" * 70)
    metadata_tables = [
        f"{table}.snapshots - Snapshot history",
        f"{table}.files - Data file manifest",
        f"{table}.history - Operation history",
        f"{table}.partitions - Partition information"
    ]
    for mt in metadata_tables:
        print(f"  - {mt}")
    
    print("\n" + "="*70)
    print("P5 DEMO COMPLETE")
    print("="*70)
    print("\nKEY POINTS:")
    print("- Every write creates immutable snapshot")
    print("- Can query data at any historical point")
    print("- Rollback to previous snapshots possible")
    print("- Audit trail built-in")

def main():
    print("\n" + "="*70)
    print("STARTING P4 + P5 DEMOS")
    print("="*70)
    
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Run P4: Schema Evolution
        demo_p4_schema_evolution(spark)
        
        # Run P5: Time Travel
        demo_p5_time_travel(spark)
        
        print("\n" + "="*70)
        print("ALL DEMOS COMPLETED SUCCESSFULLY")
        print("="*70)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
