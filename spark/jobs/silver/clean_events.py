"""
=============================================================================
SILVER LAYER: Clean and Transform Events
=============================================================================
Làm sạch dữ liệu từ Bronze Layer:
- Loại bỏ duplicates
- Xử lý NULL values
- Chuẩn hóa data types
- Tạo các dimension tables (products, users)

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
    """Khởi tạo SparkSession với cấu hình Iceberg."""
    spark = SparkSession.builder \
        .appName("Silver-CleanEvents") \
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


def create_namespace_if_not_exists(spark, namespace: str):
    """Tạo namespace nếu chưa tồn tại."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{namespace}")
    print(f"✓ Namespace '{namespace}' ready")


def clean_events(spark) -> None:
    """
    Làm sạch bảng events từ Bronze → Silver.
    """
    print("\n" + "="*60)
    print("SILVER LAYER: CLEANING EVENTS")
    print("="*60)
    
    # Đọc từ Bronze
    bronze_df = spark.table("iceberg.bronze.events_raw")
    print(f"Bronze records: {bronze_df.count():,}")
    
    # =========================================================================
    # Step 1: Remove duplicates
    # =========================================================================
    print("\n📋 Step 1: Removing duplicates...")
    
    # Define window for deduplication (keep latest record per event)
    dedup_window = Window.partitionBy(
        "event_time", "event_type", "product_id", "user_id", "user_session"
    ).orderBy(col("_ingestion_time").desc())
    
    deduped_df = bronze_df \
        .withColumn("_row_num", row_number().over(dedup_window)) \
        .filter(col("_row_num") == 1) \
        .drop("_row_num")
    
    print(f"After deduplication: {deduped_df.count():,}")
    
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
    
    # =========================================================================
    # Step 4: Data quality checks
    # =========================================================================
    print("\n📋 Step 4: Data quality checks...")
    
    total_records = silver_df.count()
    null_user_count = silver_df.filter(col("user_id").isNull()).count()
    null_product_count = silver_df.filter(col("product_id").isNull()).count()
    negative_price_count = silver_df.filter(col("price") < 0).count()
    
    print(f"  Total records: {total_records:,}")
    print(f"  Null user_id: {null_user_count:,} ({100*null_user_count/total_records:.2f}%)")
    print(f"  Null product_id: {null_product_count:,} ({100*null_product_count/total_records:.2f}%)")
    print(f"  Negative prices: {negative_price_count:,}")
    
    # Filter out invalid records
    valid_df = silver_df.filter(
        col("user_id").isNotNull() & 
        col("product_id").isNotNull()
    )
    
    print(f"  Valid records: {valid_df.count():,}")
    
    # =========================================================================
    # Step 5: Write to Silver Layer
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
        # Handle optional column (Schema Evolution)
        when(col("payment_method").isNotNull(), col("payment_method"))
            .otherwise(None).alias("payment_method"),
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
    
    # Write with partitioning
    final_df.writeTo("iceberg.silver.events_cleaned") \
        .tableProperty("format-version", "2") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .partitionedBy("event_date", "event_type") \
        .createOrReplace()
    
    print("✓ Events cleaned and written to iceberg.silver.events_cleaned")
    
    # Show sample
    print("\nSample cleaned data:")
    spark.table("iceberg.silver.events_cleaned").show(5, truncate=False)


def create_dim_products(spark) -> None:
    """
    Tạo dimension table Products từ Silver events.
    """
    print("\n" + "="*60)
    print("SILVER LAYER: CREATING DIM_PRODUCTS")
    print("="*60)
    
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
    
    print(f"✓ Created dim_products with {products_df.count():,} products")
    products_df.show(5, truncate=False)


def create_dim_users(spark) -> None:
    """
    Tạo dimension table Users từ Silver events.
    """
    print("\n" + "="*60)
    print("SILVER LAYER: CREATING DIM_USERS")
    print("="*60)
    
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
    
    print(f"✓ Created dim_users with {users_df.count():,} users")
    users_df.show(5, truncate=False)


def main():
    """Main Silver layer transformation pipeline."""
    spark = get_spark_session()
    
    try:
        # Create namespace
        create_namespace_if_not_exists(spark, "silver")
        
        # Clean events
        clean_events(spark)
        
        # Create dimension tables
        create_dim_products(spark)
        create_dim_users(spark)
        
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
