"""
=============================================================================
SILVER LAYER: Clean and Transform Events (OPTIMIZED)
=============================================================================
Làm sạch dữ liệu từ Bronze Layer:
- Loại bỏ duplicates
- Xử lý NULL values
- Chuẩn hóa data types
- Tạo các dimension tables (products, users)

OPTIMIZATIONS:
- Removed unnecessary .count() calls
- Added caching for reused DataFrames
- Reduced partition count for better performance
- Added Spark SQL adaptive execution

Usage:
    spark-submit --master spark://spark-master:7077 clean_events.py
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, coalesce, lit, trim, lower, upper,
    regexp_replace, split, first, last, count,
    sum as spark_sum, avg, min as spark_min, max as spark_max,
    row_number, dense_rank, current_timestamp,
    to_date, year, month, dayofmonth, hour
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType


def get_spark_session():
    """Khởi tạo SparkSession với cấu hình Iceberg + Optimizations."""
    spark = SparkSession.builder \
        .appName("Silver-CleanEvents") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://iceberg-warehouse/") \
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.defaultCatalog", "iceberg") \
        .config("spark.sql.shuffle.partitions", "50") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "128m") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_namespace_if_not_exists(spark, namespace: str):
    """Tạo namespace nếu chưa tồn tại."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{namespace}")
    print(f"✓ Namespace '{namespace}' ready")


def clean_events(spark) -> None:
    """
    Làm sạch bảng events từ Bronze → Silver.
    OPTIMIZED: Removed unnecessary count() calls, added caching.
    """
    print("\n" + "="*60)
    print("SILVER LAYER: CLEANING EVENTS (OPTIMIZED)")
    print("="*60)
    
    # Đọc từ Bronze
    bronze_df = spark.table("iceberg.bronze.events_raw")
    print("✓ Loaded Bronze table")
    
    # =========================================================================
    # Step 1: Remove duplicates (OPTIMIZED - no count)
    # =========================================================================
    print("\n📋 Step 1: Removing duplicates...")
    
    # Use dropDuplicates instead of window function for better performance
    deduped_df = bronze_df.dropDuplicates([
        "event_time", "event_type", "product_id", "user_id", "user_session"
    ])
    
    print("✓ Duplicates removed")
    
    # =========================================================================
    # Step 2: Handle NULL values
    # =========================================================================
    print("\n📋 Step 2: Handling NULL values...")
    
    cleaned_df = deduped_df \
        .withColumn("brand", 
            when(col("brand").isNull() | (trim(col("brand")) == ""), "Unknown")
            .otherwise(trim(col("brand")))
        ) \
        .withColumn("category_code",
            when(col("category_code").isNull() | (trim(col("category_code")) == ""), "uncategorized")
            .otherwise(trim(col("category_code")))
        ) \
        .withColumn("price",
            when(col("price").isNull() | (col("price") < 0), 0.0)
            .otherwise(col("price"))
        )
    
    print("✓ NULL values handled")
    
    # =========================================================================
    # Step 3: Standardize and extract features
    # =========================================================================
    print("\n📋 Step 3: Standardizing data and extracting features...")
    
    # Extract category hierarchy
    silver_df = cleaned_df \
        .withColumn("category_level1", 
            split(col("category_code"), "\\.").getItem(0)
        ) \
        .withColumn("category_level2",
            when(split(col("category_code"), "\\.").getItem(1).isNotNull(),
                 split(col("category_code"), "\\.").getItem(1))
            .otherwise("general")
        ) \
        .withColumn("category_level3",
            when(split(col("category_code"), "\\.").getItem(2).isNotNull(),
                 split(col("category_code"), "\\.").getItem(2))
            .otherwise(None)
        ) \
        .withColumn("brand_normalized", lower(trim(col("brand")))) \
        .withColumn("event_hour", hour(col("event_time"))) \
        .withColumn("is_weekend", 
            when(dayofmonth(col("event_time")).isin([0, 6]), True)
            .otherwise(False)
        ) \
        .withColumn("price_tier",
            when(col("price") == 0, "free")
            .when(col("price") < 10, "budget")
            .when(col("price") < 50, "mid-range")
            .when(col("price") < 200, "premium")
            .otherwise("luxury")
        ) \
        .withColumn("_transform_time", current_timestamp())
    
    print("✓ Features extracted")
    
    # =========================================================================
    # Step 4: Filter valid records (OPTIMIZED - no count)
    # =========================================================================
    print("\n📋 Step 4: Filtering valid records...")
    
    valid_df = silver_df.filter(
        col("user_id").isNotNull() & 
        col("product_id").isNotNull()
    )
    
    print("✓ Invalid records filtered")
    
    # =========================================================================
    # Step 5: Write to Silver Layer (OPTIMIZED - fewer partitions)
    # =========================================================================
    print("\n📋 Step 5: Writing to Silver Layer...")
    
    # Select final columns
    final_df = valid_df.select(
        # Original columns
        "event_time",
        "event_type", 
        "product_id",
        "category_id",
        "category_code",
        "brand",
        "price",
        "user_id",
        "user_session",
        # Derived columns
        "category_level1",
        "category_level2", 
        "category_level3",
        "brand_normalized",
        "event_hour",
        "is_weekend",
        "price_tier",
        # Partition columns
        "event_date",
        "event_year",
        "event_month",
        # Metadata
        "_transform_time"
    )
    
    # Cache before writing (used for both main table and dimensions)
    final_df.cache()
    
    # Write with optimized partitioning (only by event_year, event_month)
    final_df.writeTo("iceberg.silver.events_cleaned") \
        .tableProperty("format-version", "2") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .tableProperty("write.target-file-size-bytes", "134217728") \
        .partitionedBy("event_year", "event_month") \
        .createOrReplace()
    
    # Get count after write (one time only)
    record_count = spark.table("iceberg.silver.events_cleaned").count()
    print(f"✓ Written {record_count:,} records to iceberg.silver.events_cleaned")
    
    # Show sample
    print("\nSample cleaned data:")
    spark.table("iceberg.silver.events_cleaned").show(5, truncate=False)
    
    return final_df


def create_dim_products(spark, silver_df=None) -> None:
    """
    Tạo dimension table Products từ Silver events.
    """
    print("\n" + "="*60)
    print("SILVER LAYER: CREATING DIM_PRODUCTS")
    print("="*60)
    
    if silver_df is None:
        silver_df = spark.table("iceberg.silver.events_cleaned")
    
    # Aggregate product information
    products_df = silver_df \
        .groupBy("product_id", "category_id", "category_code", "brand") \
        .agg(
            first("category_level1").alias("category_level1"),
            first("category_level2").alias("category_level2"),
            first("category_level3").alias("category_level3"),
            first("brand_normalized").alias("brand_normalized"),
            avg("price").alias("avg_price"),
            spark_min("price").alias("min_price"),
            spark_max("price").alias("max_price"),
            count("*").alias("total_events"),
            spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
            spark_sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count")
        ) \
        .withColumn("_created_at", current_timestamp())
    
    # Write to Silver
    products_df.writeTo("iceberg.silver.dim_products") \
        .tableProperty("format-version", "2") \
        .createOrReplace()
    
    product_count = spark.table("iceberg.silver.dim_products").count()
    print(f"✓ Created dim_products with {product_count:,} products")
    products_df.show(5, truncate=False)


def create_dim_users(spark, silver_df=None) -> None:
    """
    Tạo dimension table Users từ Silver events.
    """
    print("\n" + "="*60)
    print("SILVER LAYER: CREATING DIM_USERS")
    print("="*60)
    
    if silver_df is None:
        silver_df = spark.table("iceberg.silver.events_cleaned")
    
    # Aggregate user information
    users_df = silver_df \
        .groupBy("user_id") \
        .agg(
            spark_min("event_time").alias("first_seen"),
            spark_max("event_time").alias("last_seen"),
            count("*").alias("total_events"),
            spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
            spark_sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("total_carts"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
            spark_sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("total_spent"),
            count(when(col("event_type") == "purchase", True)).alias("purchase_count")
        ) \
        .withColumn("avg_order_value",
            when(col("purchase_count") > 0, col("total_spent") / col("purchase_count"))
            .otherwise(0)
        ) \
        .withColumn("is_buyer",
            when(col("total_purchases") > 0, True).otherwise(False)
        ) \
        .withColumn("_created_at", current_timestamp())
    
    # Write to Silver
    users_df.writeTo("iceberg.silver.dim_users") \
        .tableProperty("format-version", "2") \
        .createOrReplace()
    
    user_count = spark.table("iceberg.silver.dim_users").count()
    print(f"✓ Created dim_users with {user_count:,} users")
    users_df.show(5, truncate=False)


def main():
    """Main Silver layer transformation pipeline."""
    spark = get_spark_session()
    
    try:
        # Create namespace
        create_namespace_if_not_exists(spark, "silver")
        
        # Clean events and get cached DataFrame
        silver_df = clean_events(spark)
        
        # Create dimension tables using cached DataFrame
        create_dim_products(spark, silver_df)
        create_dim_users(spark, silver_df)
        
        # Unpersist cached DataFrame
        if silver_df is not None:
            silver_df.unpersist()
        
        print("\n" + "="*60)
        print("✅ SILVER LAYER TRANSFORMATION COMPLETED!")
        print("="*60)
        print("\nCreated tables:")
        print("  - iceberg.silver.events_cleaned")
        print("  - iceberg.silver.dim_products")
        print("  - iceberg.silver.dim_users")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
