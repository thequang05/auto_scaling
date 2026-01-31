"""
=============================================================================
SERVING LAYER: Sync Data from Iceberg to ClickHouse
=============================================================================
Alternative approach: Copy Gold layer data to native ClickHouse tables.

Pros:
- Faster queries với ClickHouse native optimizations
- Full ClickHouse feature support

Cons:
- Data duplication
- Cần cơ chế sync định kỳ

Usage:
    spark-submit --master spark://spark-master:7077 sync_to_clickhouse.py
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os


def get_spark_session():
    """Khởi tạo SparkSession với ClickHouse JDBC driver."""
    spark = SparkSession.builder \
        .appName("Serving-SyncToClickHouse") \
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
        .config("spark.jars.packages", "com.github.housepower:clickhouse-spark-runtime-3.4_2.12:0.7.3") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ClickHouse connection settings
CLICKHOUSE_CONFIG = {
    "host": os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
    "port": os.environ.get("CLICKHOUSE_PORT", "8123"),
    "user": os.environ.get("CLICKHOUSE_USER", "default"),
    "password": os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse123"),
    "database": os.environ.get("CLICKHOUSE_DATABASE", "lakehouse")
}


def get_clickhouse_url():
    """Build ClickHouse JDBC URL."""
    return f"jdbc:clickhouse://{CLICKHOUSE_CONFIG['host']}:{CLICKHOUSE_CONFIG['port']}/{CLICKHOUSE_CONFIG['database']}"


def get_clickhouse_properties():
    """Get ClickHouse JDBC properties."""
    return {
        "driver": "com.github.housepower.jdbc.ClickHouseDriver",
        "user": CLICKHOUSE_CONFIG["user"],
        "password": CLICKHOUSE_CONFIG["password"],
        "socket_timeout": "300000",
        "batchsize": "100000"
    }


def sync_table(spark, iceberg_table: str, clickhouse_table: str, mode: str = "overwrite"):
    """
    Sync một bảng từ Iceberg sang ClickHouse.
    
    Args:
        spark: SparkSession
        iceberg_table: Tên bảng Iceberg (e.g., "iceberg.gold.daily_sales_by_category")
        clickhouse_table: Tên bảng ClickHouse (e.g., "lakehouse.daily_sales")
        mode: Write mode (overwrite, append)
    """
    print(f"\n{'='*60}")
    print(f"SYNCING: {iceberg_table} → {clickhouse_table}")
    print(f"{'='*60}")
    
    # Read from Iceberg
    df = spark.table(iceberg_table)
    row_count = df.count()
    print(f"Source rows: {row_count:,}")
    
    # Write to ClickHouse
    df.write \
        .format("jdbc") \
        .option("url", get_clickhouse_url()) \
        .option("dbtable", clickhouse_table) \
        .option("user", CLICKHOUSE_CONFIG["user"]) \
        .option("password", CLICKHOUSE_CONFIG["password"]) \
        .option("driver", "com.github.housepower.jdbc.ClickHouseDriver") \
        .option("batchsize", "100000") \
        .option("isolationLevel", "NONE") \
        .mode(mode) \
        .save()
    
    print(f"✓ Synced {row_count:,} rows to {clickhouse_table}")


def sync_daily_sales(spark):
    """Sync Daily Sales table."""
    sync_table(
        spark,
        "iceberg.gold.daily_sales_by_category",
        "lakehouse.daily_sales"
    )


def sync_funnel_analysis(spark):
    """Sync Funnel Analysis table."""
    sync_table(
        spark,
        "iceberg.gold.funnel_analysis",
        "lakehouse.funnel_analysis"
    )


def sync_customer_rfm(spark):
    """Sync Customer RFM table."""
    sync_table(
        spark,
        "iceberg.gold.customer_rfm",
        "lakehouse.customer_rfm"
    )


def sync_product_performance(spark):
    """Sync Product Performance table."""
    sync_table(
        spark,
        "iceberg.gold.product_performance",
        "lakehouse.product_performance"
    )


def sync_all_tables(spark):
    """Sync all Gold layer tables to ClickHouse."""
    tables = [
        ("iceberg.gold.daily_sales_by_category", "lakehouse.daily_sales"),
        ("iceberg.gold.funnel_analysis", "lakehouse.funnel_analysis"),
        ("iceberg.gold.customer_rfm", "lakehouse.customer_rfm"),
        ("iceberg.gold.product_performance", "lakehouse.product_performance"),
    ]
    
    for iceberg_table, clickhouse_table in tables:
        try:
            sync_table(spark, iceberg_table, clickhouse_table)
        except Exception as e:
            print(f"❌ Error syncing {iceberg_table}: {str(e)}")
            continue


def main():
    """Main sync pipeline."""
    spark = get_spark_session()
    
    try:
        print("\n" + "="*60)
        print("SERVING LAYER: SYNC TO CLICKHOUSE")
        print("="*60)
        
        sync_all_tables(spark)
        
        print("\n" + "="*60)
        print("✅ SYNC TO CLICKHOUSE COMPLETED!")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
