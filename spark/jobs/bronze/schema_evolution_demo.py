"""
=============================================================================
SCHEMA EVOLUTION DEMO
=============================================================================
Demo tính năng Schema Evolution của Apache Iceberg:
- Thêm cột mới (payment_method) mà không cần rewrite data
- Backward/Forward compatibility
- Iceberg tự động xử lý các thay đổi schema

Yêu cầu đề bài: "Với giả lập thay đổi cấu trúc dữ liệu nguồn 
(ví dụ: thêm cột payment_method vào ngày thứ t), hệ thống Apache Iceberg 
phải tự động xử lý, thích ứng mà không cần viết lại toàn bộ dữ liệu."
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when
from pyspark.sql.types import StringType
from datetime import datetime


def get_spark_session():
    """Khởi tạo SparkSession với cấu hình Iceberg."""
    spark = SparkSession.builder \
        .appName("SchemaEvolution-Demo") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3://iceberg-warehouse/") \
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
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


def demo_schema_evolution(spark):
    """
    Demo Schema Evolution workflow.
    """
    table_name = "iceberg.bronze.events_raw"
    
    print("\n" + "="*70)
    print(" SCHEMA EVOLUTION DEMO - APACHE ICEBERG")
    print("="*70)
    
    # ==========================================================================
    # STEP 1: Show current schema (NGÀY T)
    # ==========================================================================
    print("\n STEP 1: Current Schema (Ngày T - Trước khi thêm cột)")
    print("-" * 50)
    
    current_df = spark.table(table_name)
    print("Current columns:")
    for field in current_df.schema.fields:
        print(f"  - {field.name}: {field.dataType}")
    
    print(f"\nTotal records: {current_df.count():,}")
    
    # ==========================================================================
    # STEP 2: Add new column (NGÀY T+1) - Schema Evolution
    # ==========================================================================
    print("\n\n🆕 STEP 2: Adding new column 'payment_method' (Ngày T+1)")
    print("-" * 50)
    print("Sử dụng ALTER TABLE để thêm cột mới...")
    print("Iceberg sẽ xử lý schema evolution mà KHÔNG rewrite data!")
    
    # Add new column using ALTER TABLE
    try:
        spark.sql(f"""
            ALTER TABLE {table_name} 
            ADD COLUMN payment_method STRING COMMENT 'Payment method (credit_card, paypal, bank_transfer)'
        """)
        print("✓ Column 'payment_method' added successfully!")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(" Column 'payment_method' already exists (idempotent operation)")
        else:
            raise
    
    # ==========================================================================
    # STEP 3: Verify new schema
    # ==========================================================================
    print("\n\n STEP 3: Verify New Schema")
    print("-" * 50)
    
    # Refresh table metadata
    spark.catalog.refreshTable(table_name)
    
    updated_df = spark.table(table_name)
    print("Updated columns:")
    for field in updated_df.schema.fields:
        print(f"  - {field.name}: {field.dataType}")
    
    # ==========================================================================
    # STEP 4: Read old data with new schema
    # ==========================================================================
    print("\n\n📖 STEP 4: Read Old Data with New Schema")
    print("-" * 50)
    print("Dữ liệu cũ vẫn đọc được, cột mới sẽ có giá trị NULL:")
    
    updated_df.select(
        "event_type", "user_id", "price", "payment_method"
    ).show(5, truncate=False)
    
    # ==========================================================================
    # STEP 5: Insert new data with new column
    # ==========================================================================
    print("\n\n STEP 5: Insert New Data with payment_method")
    print("-" * 50)
    
    # Create sample new data with payment_method
    new_data = spark.createDataFrame([
        ("2024-01-15 10:00:00", "purchase", 12345, 678, "electronics.smartphone", 
         "Apple", 999.99, 1001, "session_001", "credit_card"),
        ("2024-01-15 10:05:00", "purchase", 12346, 678, "electronics.smartphone", 
         "Samsung", 899.99, 1002, "session_002", "paypal"),
        ("2024-01-15 10:10:00", "purchase", 12347, 679, "electronics.laptop", 
         "Dell", 1299.99, 1003, "session_003", "bank_transfer"),
    ], ["event_time", "event_type", "product_id", "category_id", "category_code",
        "brand", "price", "user_id", "user_session", "payment_method"])
    
    # Add metadata columns
    from pyspark.sql.functions import to_date, year, month, input_file_name
    
    new_data_with_metadata = new_data \
        .withColumn("_ingestion_time", current_timestamp()) \
        .withColumn("_source_file", lit("schema_evolution_demo")) \
        .withColumn("_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
        .withColumn("event_date", to_date(col("event_time"))) \
        .withColumn("event_year", year(col("event_time"))) \
        .withColumn("event_month", month(col("event_time")))
    
    # Cast event_time to timestamp
    from pyspark.sql.functions import to_timestamp
    new_data_with_metadata = new_data_with_metadata \
        .withColumn("event_time", to_timestamp("event_time"))
    
    # Append new data
    new_data_with_metadata.writeTo(table_name).append()
    
    print("✓ New data with payment_method inserted!")
    
    # ==========================================================================
    # STEP 6: Query mixed data (old + new)
    # ==========================================================================
    print("\n\n🔍 STEP 6: Query Mixed Data (Old + New)")
    print("-" * 50)
    print("Dữ liệu cũ (payment_method = NULL) và mới (có payment_method) cùng tồn tại:")
    
    spark.sql(f"""
        SELECT 
            event_type,
            user_id,
            price,
            payment_method,
            CASE 
                WHEN payment_method IS NULL THEN 'Old Data (before schema change)'
                ELSE 'New Data (after schema change)'
            END as data_version
        FROM {table_name}
        ORDER BY CASE WHEN payment_method IS NULL THEN 0 ELSE 1 END, event_time DESC
        LIMIT 10
    """).show(truncate=False)
    
    # ==========================================================================
    # STEP 7: Show schema history
    # ==========================================================================
    print("\n\n STEP 7: Schema History")
    print("-" * 50)
    
    print("Snapshots (each snapshot captures schema at that point):")
    spark.sql(f"SELECT * FROM {table_name}.snapshots ORDER BY committed_at DESC").show(truncate=False)
    
    print("\n\n" + "="*70)
    print(" SCHEMA EVOLUTION DEMO COMPLETED!")
    print("="*70)
    print("""
    Key Points:
    -----------
    1. Iceberg cho phép thêm/xóa/đổi tên cột mà KHÔNG cần rewrite data
    2. Dữ liệu cũ vẫn đọc được với schema mới (backward compatible)
    3. Dữ liệu mới với schema mới có thể được insert bình thường
    4. Không có downtime, không cần migrate data
    5. Schema changes được track trong metadata (snapshots)
    """)


def demo_additional_schema_operations(spark):
    """
    Demo các thao tác schema khác của Iceberg.
    """
    table_name = "iceberg.bronze.events_raw"
    
    print("\n" + "="*70)
    print("🔧 ADDITIONAL SCHEMA OPERATIONS")
    print("="*70)
    
    # Rename column
    print("\n1️⃣ Rename Column:")
    print("   ALTER TABLE ... RENAME COLUMN old_name TO new_name")
    
    # Drop column
    print("\n2️⃣ Drop Column:")
    print("   ALTER TABLE ... DROP COLUMN column_name")
    
    # Change column type (if compatible)
    print("\n3️⃣ Change Column Type:")
    print("   ALTER TABLE ... ALTER COLUMN col_name TYPE new_type")
    
    # Reorder columns
    print("\n4️⃣ Reorder Columns:")
    print("   ALTER TABLE ... ALTER COLUMN col_name FIRST")
    print("   ALTER TABLE ... ALTER COLUMN col_name AFTER other_col")
    
    # Make column required/optional
    print("\n5️⃣ Change Nullability:")
    print("   ALTER TABLE ... ALTER COLUMN col_name DROP NOT NULL")
    
    print("""
     Note: All these operations are metadata-only operations.
    They do NOT rewrite the underlying data files!
    """)


def main():
    spark = get_spark_session()
    
    try:
        demo_schema_evolution(spark)
        demo_additional_schema_operations(spark)
        
    except Exception as e:
        print(f"\n Error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
