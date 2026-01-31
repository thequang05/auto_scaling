"""
=============================================================================
TIME TRAVEL DEMO - APACHE ICEBERG
=============================================================================
Demo tính năng Time Travel của Apache Iceberg:
- Truy vấn dữ liệu tại snapshot cũ
- Truy vấn dữ liệu tại thời điểm cụ thể
- Rollback về version trước

Yêu cầu đề bài: "Yêu cầu tính năng Time Travel của Iceberg để truy vấn lại 
trạng thái dữ liệu tại thời điểm quá khứ nhằm kiểm tra lại các giao dịch nghi vấn."
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, count
from datetime import datetime, timedelta


def get_spark_session():
    """Khởi tạo SparkSession với cấu hình Iceberg."""
    spark = SparkSession.builder \
        .appName("TimeTravel-Demo") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3://iceberg-warehouse/") \
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.defaultCatalog", "iceberg") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def demo_time_travel(spark):
    """
    Demo Time Travel workflow.
    """
    table_name = "iceberg.bronze.events_raw"
    
    print("\n" + "="*70)
    print("⏰ TIME TRAVEL DEMO - APACHE ICEBERG")
    print("="*70)
    
    # ==========================================================================
    # STEP 1: Show current snapshots
    # ==========================================================================
    print("\n📸 STEP 1: Current Snapshots")
    print("-" * 50)
    
    snapshots_df = spark.sql(f"""
        SELECT 
            snapshot_id,
            committed_at,
            operation,
            summary
        FROM {table_name}.snapshots
        ORDER BY committed_at DESC
        LIMIT 10
    """)
    
    snapshots_df.show(truncate=False)
    
    # Get snapshot IDs for demo
    snapshots = snapshots_df.collect()
    if len(snapshots) < 2:
        print("⚠️ Need at least 2 snapshots for time travel demo.")
        print("Running some data modifications first...")
        
        # Create some snapshots by modifying data
        create_demo_snapshots(spark, table_name)
        
        # Refresh snapshots
        snapshots_df = spark.sql(f"""
            SELECT snapshot_id, committed_at
            FROM {table_name}.snapshots
            ORDER BY committed_at DESC
        """)
        snapshots = snapshots_df.collect()
    
    current_snapshot_id = snapshots[0]["snapshot_id"]
    older_snapshot_id = snapshots[-1]["snapshot_id"] if len(snapshots) > 1 else current_snapshot_id
    
    print(f"\nCurrent snapshot: {current_snapshot_id}")
    print(f"Older snapshot: {older_snapshot_id}")
    
    # ==========================================================================
    # STEP 2: Query current data
    # ==========================================================================
    print("\n\n📊 STEP 2: Current Data State")
    print("-" * 50)
    
    current_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]["cnt"]
    print(f"Current row count: {current_count:,}")
    
    spark.sql(f"""
        SELECT event_type, COUNT(*) as count
        FROM {table_name}
        GROUP BY event_type
    """).show()
    
    # ==========================================================================
    # STEP 3: Time Travel - Query by Snapshot ID
    # ==========================================================================
    print("\n\n🕰️ STEP 3: Time Travel - Query by Snapshot ID")
    print("-" * 50)
    print(f"Querying data at snapshot: {older_snapshot_id}")
    
    old_count = spark.sql(f"""
        SELECT COUNT(*) as cnt 
        FROM {table_name} 
        VERSION AS OF {older_snapshot_id}
    """).collect()[0]["cnt"]
    
    print(f"Row count at old snapshot: {old_count:,}")
    
    spark.sql(f"""
        SELECT event_type, COUNT(*) as count
        FROM {table_name}
        VERSION AS OF {older_snapshot_id}
        GROUP BY event_type
    """).show()
    
    # ==========================================================================
    # STEP 4: Time Travel - Query by Timestamp
    # ==========================================================================
    print("\n\n🕐 STEP 4: Time Travel - Query by Timestamp")
    print("-" * 50)
    
    # Get the timestamp of older snapshot
    older_timestamp = snapshots[-1]["committed_at"] if len(snapshots) > 1 else None
    
    if older_timestamp:
        print(f"Querying data at timestamp: {older_timestamp}")
        
        # Format timestamp for SQL
        timestamp_str = older_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        
        spark.sql(f"""
            SELECT event_type, COUNT(*) as count
            FROM {table_name}
            TIMESTAMP AS OF '{timestamp_str}'
            GROUP BY event_type
        """).show()
    
    # ==========================================================================
    # STEP 5: Compare snapshots (Diff)
    # ==========================================================================
    print("\n\n🔍 STEP 5: Compare Snapshots")
    print("-" * 50)
    
    if len(snapshots) >= 2:
        print(f"Comparing snapshot {older_snapshot_id} with {current_snapshot_id}")
        
        # Show what changed between snapshots
        spark.sql(f"""
            SELECT 
                'older' as version,
                COUNT(*) as row_count
            FROM {table_name}
            VERSION AS OF {older_snapshot_id}
            
            UNION ALL
            
            SELECT 
                'current' as version,
                COUNT(*) as row_count
            FROM {table_name}
        """).show()
    
    # ==========================================================================
    # STEP 6: Show History
    # ==========================================================================
    print("\n\n📜 STEP 6: Table History")
    print("-" * 50)
    
    spark.sql(f"""
        SELECT 
            made_current_at,
            snapshot_id,
            parent_id,
            is_current_ancestor
        FROM {table_name}.history
        ORDER BY made_current_at DESC
        LIMIT 10
    """).show(truncate=False)
    
    # ==========================================================================
    # Use Cases
    # ==========================================================================
    print("\n\n💡 TIME TRAVEL USE CASES")
    print("-" * 50)
    print("""
    1. 🔍 Audit & Compliance
       - Truy vấn dữ liệu tại thời điểm báo cáo được tạo
       - Xác minh dữ liệu historical cho auditor
    
    2. 🐛 Debug Data Issues
       - So sánh data trước và sau khi lỗi xảy ra
       - Tìm ra chính xác thời điểm data bị corrupt
    
    3. 🔄 Recovery & Rollback
       - Rollback về trạng thái tốt trước khi có lỗi
       - Recover data đã bị xóa nhầm
    
    4. 💳 Fraud Detection (Use Case B)
       - Kiểm tra lại các giao dịch nghi vấn
       - So sánh patterns tại các thời điểm khác nhau
    
    5. 📊 Historical Analysis
       - Phân tích xu hướng qua thời gian
       - A/B testing với data historical
    """)
    
    # ==========================================================================
    # Rollback Demo
    # ==========================================================================
    print("\n\n⚠️ ROLLBACK CAPABILITY (For Reference Only)")
    print("-" * 50)
    print("""
    Để rollback về snapshot cũ, sử dụng:
    
    -- Rollback to specific snapshot
    CALL iceberg.system.rollback_to_snapshot(
        'iceberg.bronze.events_raw',
        <snapshot_id>
    );
    
    -- Rollback to timestamp
    CALL iceberg.system.rollback_to_timestamp(
        'iceberg.bronze.events_raw',
        TIMESTAMP '2024-01-15 10:00:00'
    );
    
    ⚠️ CẢNH BÁO: Rollback sẽ thay đổi current state của table!
    """)


def create_demo_snapshots(spark, table_name):
    """
    Tạo thêm snapshots bằng cách thêm dữ liệu mới.
    """
    print("Creating demo snapshots...")
    
    # Add some sample data to create new snapshots
    from pyspark.sql.functions import to_timestamp, to_date, year, month
    
    sample_data = spark.createDataFrame([
        ("2024-01-20 12:00:00", "view", 99999, 100, "demo.category", "DemoBrand", 10.0, 99999, "demo_session"),
    ], ["event_time", "event_type", "product_id", "category_id", "category_code", "brand", "price", "user_id", "user_session"])
    
    # Add metadata columns
    sample_with_metadata = sample_data \
        .withColumn("event_time", to_timestamp("event_time")) \
        .withColumn("_ingestion_time", current_timestamp()) \
        .withColumn("_source_file", lit("time_travel_demo")) \
        .withColumn("_batch_id", lit("demo_batch")) \
        .withColumn("event_date", to_date(col("event_time"))) \
        .withColumn("event_year", year(col("event_time"))) \
        .withColumn("event_month", month(col("event_time")))
    
    # Append to create new snapshot
    sample_with_metadata.writeTo(table_name).append()
    print("✓ Demo snapshot created")


def main():
    spark = get_spark_session()
    
    try:
        demo_time_travel(spark)
        
        print("\n" + "="*70)
        print("✅ TIME TRAVEL DEMO COMPLETED!")
        print("="*70)
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
