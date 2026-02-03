"""
=============================================================================
SERVING LAYER: Sync Data from Iceberg to ClickHouse
=============================================================================
Alternative approach: Export Gold data to CSV, then use ClickHouse HTTP API
to import directly. Avoids JDBC driver issues.

Usage:
    spark-submit --master spark://spark-master:7077 sync_to_clickhouse_v2.py
=============================================================================
"""

from pyspark.sql import SparkSession
import requests
import os

def get_spark_session():
    """Khởi tạo SparkSession."""
    spark = SparkSession.builder \
        .appName("Serving-SyncToClickHouse-V2") \
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


# ClickHouse configuration
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.environ.get("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse123")
CLICKHOUSE_DATABASE = "lakehouse"


def execute_clickhouse_query(query):
    """Execute a query on ClickHouse via HTTP."""
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    params = {
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
        "query": query
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"ClickHouse error: {response.text}")
    return response.text


def insert_clickhouse_data(table, data_csv):
    """Insert CSV data into ClickHouse table."""
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    params = {
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
        "query": f"INSERT INTO {table} FORMAT CSVWithNames"
    }
    response = requests.post(url, params=params, data=data_csv.encode('utf-8'))
    if response.status_code != 200:
        raise Exception(f"ClickHouse insert error: {response.text}")
    return response.text


def create_clickhouse_tables():
    """Create ClickHouse tables for Gold layer data."""
    
    # Create database
    execute_clickhouse_query(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")
    print(f"✓ Database '{CLICKHOUSE_DATABASE}' ready")
    
    # Daily Sales table
    execute_clickhouse_query(f"""
        DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE}.daily_sales
    """)
    execute_clickhouse_query(f"""
        CREATE TABLE {CLICKHOUSE_DATABASE}.daily_sales (
            event_date Date,
            category_level1 String,
            category_level2 String,
            order_count UInt32,
            unique_customers UInt32,
            unique_products UInt32,
            total_revenue Float64,
            avg_order_value Float64,
            min_order_value Float64,
            max_order_value Float64,
            revenue_per_customer Float64,
            sale_year UInt16,
            sale_month UInt8,
            sale_quarter UInt8,
            sale_week UInt8
        ) ENGINE = MergeTree()
        PARTITION BY (sale_year, sale_month)
        ORDER BY (event_date, category_level1)
    """)
    print("✓ Created table: daily_sales")
    
    # Funnel Analysis table
    execute_clickhouse_query(f"""
        DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE}.funnel_analysis
    """)
    execute_clickhouse_query(f"""
        CREATE TABLE {CLICKHOUSE_DATABASE}.funnel_analysis (
            event_date Date,
            category_level1 String,
            views UInt32,
            carts UInt32,
            purchases UInt32,
            view_to_cart_rate Float64,
            cart_to_purchase_rate Float64,
            overall_conversion_rate Float64,
            analysis_year UInt16,
            analysis_month UInt8
        ) ENGINE = MergeTree()
        PARTITION BY (analysis_year, analysis_month)
        ORDER BY (event_date, category_level1)
    """)
    print("✓ Created table: funnel_analysis")
    
    # Customer RFM table
    execute_clickhouse_query(f"""
        DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE}.customer_rfm
    """)
    execute_clickhouse_query(f"""
        CREATE TABLE {CLICKHOUSE_DATABASE}.customer_rfm (
            user_id UInt64,
            recency_days UInt32,
            frequency UInt32,
            monetary Float64,
            r_score UInt8,
            f_score UInt8,
            m_score UInt8,
            rfm_score UInt8,
            customer_segment String
        ) ENGINE = MergeTree()
        ORDER BY (user_id)
    """)
    print("✓ Created table: customer_rfm")
    
    # Product Performance table
    execute_clickhouse_query(f"""
        DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE}.product_performance
    """)
    execute_clickhouse_query(f"""
        CREATE TABLE {CLICKHOUSE_DATABASE}.product_performance (
            product_id UInt32,
            brand String,
            category_level1 String,
            views UInt32,
            carts UInt32,
            purchases UInt32,
            revenue Float64,
            conversion_rate Float64,
            purchase_rate Float64,
            avg_price Float64,
            revenue_rank UInt32
        ) ENGINE = MergeTree()
        ORDER BY (product_id)
    """)
    print("✓ Created table: product_performance")


def sync_table(spark, iceberg_table, clickhouse_table, columns):
    """Sync a table from Iceberg to ClickHouse."""
    print(f"\n{'='*60}")
    print(f"SYNCING: {iceberg_table} → {clickhouse_table}")
    print(f"{'='*60}")
    
    try:
        # Read from Iceberg
        df = spark.table(iceberg_table).select(columns)
        count = df.count()
        print(f"Source rows: {count:,}")
        
        if count == 0:
            print("⚠️ No data to sync")
            return
        
        # Collect to driver and convert to CSV
        rows = df.limit(100000).collect()  # Limit for memory safety
        
        # Build CSV
        header = ",".join(columns)
        csv_lines = [header]
        for row in rows:
            values = []
            for col in columns:
                val = row[col]
                if val is None:
                    values.append("")
                elif isinstance(val, str):
                    values.append(f'"{val}"')
                else:
                    values.append(str(val))
            csv_lines.append(",".join(values))
        
        csv_data = "\n".join(csv_lines)
        
        # Insert into ClickHouse
        insert_clickhouse_data(clickhouse_table, csv_data)
        print(f"✓ Synced {min(count, 100000):,} rows to {clickhouse_table}")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")


def main():
    """Main sync pipeline."""
    print("\n" + "="*60)
    print("SERVING LAYER: SYNC TO CLICKHOUSE (V2 - HTTP API)")
    print("="*60)
    
    # Create tables first
    print("\n📋 Creating ClickHouse tables...")
    create_clickhouse_tables()
    
    # Init Spark
    spark = get_spark_session()
    
    try:
        # Sync daily sales
        sync_table(
            spark,
            "iceberg.gold.daily_sales_by_category",
            f"{CLICKHOUSE_DATABASE}.daily_sales",
            ["event_date", "category_level1", "category_level2", "order_count",
             "unique_customers", "unique_products", "total_revenue", "avg_order_value",
             "min_order_value", "max_order_value", "revenue_per_customer",
             "sale_year", "sale_month", "sale_quarter", "sale_week"]
        )
        
        # Sync funnel analysis
        sync_table(
            spark,
            "iceberg.gold.funnel_analysis",
            f"{CLICKHOUSE_DATABASE}.funnel_analysis",
            ["event_date", "category_level1", "views", "carts", "purchases",
             "view_to_cart_rate", "cart_to_purchase_rate", "overall_conversion_rate",
             "analysis_year", "analysis_month"]
        )
        
        # Sync customer RFM
        sync_table(
            spark,
            "iceberg.gold.customer_rfm",
            f"{CLICKHOUSE_DATABASE}.customer_rfm",
            ["user_id", "recency_days", "frequency", "monetary",
             "r_score", "f_score", "m_score", "rfm_score", "customer_segment"]
        )
        
        # Sync product performance
        sync_table(
            spark,
            "iceberg.gold.product_performance",
            f"{CLICKHOUSE_DATABASE}.product_performance",
            ["product_id", "brand", "category_level1", "views", "carts", 
             "purchases", "revenue", "conversion_rate", "purchase_rate", 
             "avg_price", "revenue_rank"]
        )
        
        print("\n" + "="*60)
        print("✅ SYNC TO CLICKHOUSE COMPLETED!")
        print("="*60)
        
        # Verify
        print("\n📊 Verification:")
        for table in ["daily_sales", "funnel_analysis", "customer_rfm", "product_performance"]:
            count = execute_clickhouse_query(f"SELECT count() FROM {CLICKHOUSE_DATABASE}.{table}")
            print(f"  - {table}: {count.strip()} rows")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
