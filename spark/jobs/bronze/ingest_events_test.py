"""
=============================================================================
BRONZE LAYER: Ingest Raw E-commerce Events (TEST VERSION)
=============================================================================
Test với 1 file CSV để chạy nhanh hơn
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lit,
    to_date, year, month
)
from datetime import datetime
import os


def get_spark_session():
    """Khởi tạo SparkSession với cấu hình Iceberg + MinIO."""
    spark = SparkSession.builder \
        .appName("Bronze-IngestEvents-Test") \
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


def main():
    spark = get_spark_session()
    
    try:
        # Tạo namespace
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")
        print("✓ Namespace 'bronze' ready")
        
        # Chỉ đọc 1 file để test nhanh
        source_path = "/opt/data/raw/2019-Oct.csv"  # File nhỏ nhất
        
        print(f"\n{'='*60}")
        print(f"INGESTING TEST DATA (1 file only)")
        print(f"{'='*60}")
        print(f"Source: {source_path}")
        
        # Đọc dữ liệu
        raw_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(source_path)
        
        # Thêm metadata
        bronze_df = raw_df \
            .withColumn("_ingestion_time", current_timestamp()) \
            .withColumn("_source_file", input_file_name()) \
            .withColumn("_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
        
        # Thêm partition columns
        if "event_time" in bronze_df.columns:
            bronze_df = bronze_df \
                .withColumn("event_time", col("event_time").cast("timestamp")) \
                .withColumn("event_date", to_date(col("event_time"))) \
                .withColumn("event_year", year(col("event_time"))) \
                .withColumn("event_month", month(col("event_time")))
        
        print(f"\nTotal records: {bronze_df.count():,}")
        bronze_df.show(5, truncate=False)
        
        # Ghi vào Iceberg
        print("\nWriting to Iceberg...")
        bronze_df.writeTo("iceberg.bronze.events_raw") \
            .tableProperty("format-version", "2") \
            .tableProperty("write.parquet.compression-codec", "snappy") \
            .partitionedBy("event_date") \
            .createOrReplace()
        
        print("✓ Data ingested successfully!")
        
        # Verify
        verify_df = spark.table("iceberg.bronze.events_raw")
        print(f"✓ Verification - Table row count: {verify_df.count():,}")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
