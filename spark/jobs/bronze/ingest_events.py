"""
=============================================================================
BRONZE LAYER: Ingest Raw E-commerce Events
=============================================================================
Đọc dữ liệu thô từ CSV và ghi vào Iceberg tables trên MinIO.
Giữ nguyên dữ liệu gốc, chỉ thêm metadata columns.

Usage:
    spark-submit --master spark://spark-master:7077 ingest_events.py
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lit,
    to_date, year, month, dayofmonth, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    DoubleType, TimestampType
)
from datetime import datetime
import os


def get_spark_session():
    """
    Khởi tạo SparkSession với cấu hình Iceberg + MinIO.
    """
    spark = SparkSession.builder \
        .appName("Bronze-IngestEvents") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://iceberg-warehouse/") \
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.defaultCatalog", "iceberg") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def define_raw_schema():
    """
    Schema cho dữ liệu e-commerce events (REES46/Cosmetics Shop format).
    """
    return StructType([
        StructField("event_time", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("category_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True),
    ])


def create_namespace_if_not_exists(spark, namespace: str):
    """
    Tạo namespace (database) nếu chưa tồn tại.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{namespace}")
    print(f"✓ Namespace '{namespace}' ready")


def ingest_csv_to_bronze(spark, source_path: str, table_name: str):
    """
    Đọc dữ liệu CSV và ghi vào Bronze Layer (Iceberg).
    
    Args:
        spark: SparkSession
        source_path: Đường dẫn đến file/folder CSV
        table_name: Tên bảng Iceberg (format: namespace.table)
    """
    print(f"\n{'='*60}")
    print(f"INGESTING DATA TO BRONZE LAYER")
    print(f"{'='*60}")
    print(f"Source: {source_path}")
    print(f"Target: {table_name}")
    print(f"{'='*60}\n")
    
    # Đọc dữ liệu thô
    raw_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(source_path)
    
    # Thêm metadata columns
    bronze_df = raw_df \
        .withColumn("_ingestion_time", current_timestamp()) \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    
    # Thêm partition columns
    if "event_time" in bronze_df.columns:
        bronze_df = bronze_df \
            .withColumn("event_date", to_date(col("event_time"))) \
            .withColumn("event_year", year(col("event_time"))) \
            .withColumn("event_month", month(col("event_time")))
    
    # Show sample
    print("Sample data:")
    bronze_df.show(5, truncate=False)
    print(f"\nTotal records: {bronze_df.count():,}")
    print(f"Schema:\n{bronze_df.printSchema()}")
    
    # Tạo hoặc append vào bảng Iceberg
    bronze_df.writeTo(table_name) \
        .tableProperty("format-version", "2") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .partitionedBy("event_date") \
        .createOrReplace()
    
    print(f"\n✓ Data ingested successfully to {table_name}")
    
    # Verify
    verify_df = spark.table(table_name)
    print(f"✓ Verification - Table row count: {verify_df.count():,}")
    
    return bronze_df


def show_table_metadata(spark, table_name: str):
    """
    Hiển thị metadata của bảng Iceberg.
    """
    print(f"\n{'='*60}")
    print(f"TABLE METADATA: {table_name}")
    print(f"{'='*60}")
    
    # Snapshots
    print("\n📸 Snapshots:")
    spark.sql(f"SELECT * FROM {table_name}.snapshots").show(truncate=False)
    
    # Files
    print("\n📁 Data Files:")
    spark.sql(f"SELECT * FROM {table_name}.files LIMIT 5").show(truncate=False)
    
    # Partitions
    print("\n📂 Partitions:")
    spark.sql(f"SELECT * FROM {table_name}.partitions").show(truncate=False)


def main():
    """
    Main ingestion pipeline.
    """
    spark = get_spark_session()
    
    try:
        # Tạo namespace cho Bronze layer
        create_namespace_if_not_exists(spark, "bronze")
        
        # Đường dẫn dữ liệu nguồn
        # Có thể là local path hoặc S3 path
        source_path = os.environ.get(
            "SOURCE_PATH", 
            "/opt/data/raw/*.csv"  # Default local path
        )
        
        # Alternative: Read from MinIO directly
        # source_path = "s3a://lakehouse-bronze/raw/*.csv"
        
        # Ingest events data
        ingest_csv_to_bronze(
            spark=spark,
            source_path=source_path,
            table_name="iceberg.bronze.events_raw"
        )
        
        # Show metadata
        show_table_metadata(spark, "iceberg.bronze.events_raw")
        
        print("\n" + "="*60)
        print("✅ BRONZE LAYER INGESTION COMPLETED SUCCESSFULLY")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
