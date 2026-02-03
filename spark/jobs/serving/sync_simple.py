"""
=============================================================================
SERVING LAYER: Simple Sync from Iceberg to ClickHouse
=============================================================================
Export Gold data to CSV and use wget to insert into ClickHouse.
No external dependencies required.
=============================================================================
"""

from pyspark.sql import SparkSession
import subprocess
import os

def get_spark_session():
    """Khởi tạo SparkSession - đọc Parquet trực tiếp từ S3."""
    spark = SparkSession.builder \
        .appName("Serving-SyncSimple") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.environ.get("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse123")


def execute_clickhouse(query):
    """Execute ClickHouse query using wget."""
    import urllib.parse
    encoded_query = urllib.parse.quote(query)
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?password={CLICKHOUSE_PASSWORD}&query={encoded_query}"
    result = subprocess.run(['wget', '-q', '-O', '-', url], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
    return result.stdout


def insert_csv_to_clickhouse(table, csv_file):
    """Insert CSV file to ClickHouse using wget."""
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?password={CLICKHOUSE_PASSWORD}&query=INSERT%20INTO%20{table}%20FORMAT%20CSVWithNames"
    result = subprocess.run(['wget', '-q', '-O', '-', '--post-file=' + csv_file, url], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error inserting: {result.stderr}")
    return result.returncode == 0


def sync_table(spark, s3_path, clickhouse_table, temp_path):
    """Sync một bảng từ S3 Parquet sang ClickHouse."""
    print(f"\n{'='*60}")
    print(f"Syncing: {s3_path} -> lakehouse.{clickhouse_table}")
    print(f"{'='*60}")
    
    try:
        # Read Parquet from S3
        df = spark.read.parquet(s3_path)
        count = df.count()
        print(f"✓ Read {count:,} rows from {s3_path}")
        
        if count == 0:
            print(f"⚠ No data to sync for {iceberg_table}")
            return False
        
        # Write to temp CSV
        csv_path = f"{temp_path}/{clickhouse_table}"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
        print(f"✓ Exported to CSV")
        
        # Find the CSV file
        import glob
        csv_files = subprocess.run(['find', csv_path, '-name', '*.csv'], capture_output=True, text=True)
        csv_file = csv_files.stdout.strip().split('\n')[0]
        
        if not csv_file:
            print(f"⚠ No CSV file found")
            return False
        
        # Truncate ClickHouse table
        execute_clickhouse(f"TRUNCATE TABLE lakehouse.{clickhouse_table}")
        print(f"✓ Truncated lakehouse.{clickhouse_table}")
        
        # Insert to ClickHouse
        success = insert_csv_to_clickhouse(f"lakehouse.{clickhouse_table}", csv_file)
        if success:
            # Verify
            result = execute_clickhouse(f"SELECT count() FROM lakehouse.{clickhouse_table}")
            ch_count = result.strip()
            print(f"✓ Inserted {ch_count} rows to ClickHouse")
            return True
        else:
            print(f"✗ Failed to insert data")
            return False
            
    except Exception as e:
        print(f"✗ Error syncing {iceberg_table}: {str(e)}")
        return False


def main():
    print("""
╔═══════════════════════════════════════════════════════════════╗
║         SYNC ICEBERG GOLD LAYER TO CLICKHOUSE                  ║
╚═══════════════════════════════════════════════════════════════╝
    """)
    
    spark = get_spark_session()
    temp_path = "/tmp/clickhouse_sync"
    
    # Tables to sync - đọc Parquet trực tiếp từ S3
    tables = [
        ("s3a://iceberg-warehouse/gold/daily_sales_by_category/data", "daily_sales"),
        ("s3a://iceberg-warehouse/gold/funnel_analysis/data", "funnel_analysis"),
        ("s3a://iceberg-warehouse/gold/customer_rfm/data", "customer_rfm"),
        ("s3a://iceberg-warehouse/gold/product_performance/data", "product_performance"),
    ]
    
    results = []
    for iceberg_table, ch_table in tables:
        success = sync_table(spark, iceberg_table, ch_table, temp_path)
        results.append((ch_table, success))
    
    # Summary
    print(f"\n{'='*60}")
    print("SYNC SUMMARY")
    print(f"{'='*60}")
    for table, success in results:
        status = "✓" if success else "✗"
        print(f"  {status} {table}")
    
    spark.stop()


if __name__ == "__main__":
    main()
