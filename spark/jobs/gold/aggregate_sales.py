"""
=============================================================================
GOLD LAYER: Aggregated Sales & Business Metrics
=============================================================================
Tạo các bảng tổng hợp phục vụ báo cáo:
- daily_sales_by_category: Doanh thu theo ngày và danh mục
- funnel_analysis: Phân tích phễu chuyển đổi
- customer_rfm: Phân khúc khách hàng RFM

Sử dụng Iceberg Partitioning và Z-Ordering để tối ưu truy vấn.

Usage:
    spark-submit --master spark://spark-master:7077 aggregate_sales.py
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, coalesce, lit, 
    count, countDistinct, sum as spark_sum, avg, 
    min as spark_min, max as spark_max,
    round as spark_round, datediff, current_date, current_timestamp,
    to_date, year, month, dayofmonth, weekofyear, quarter,
    ntile, concat_ws, expr
)
from pyspark.sql.window import Window
from datetime import datetime, timedelta


def get_spark_session():
    """Khởi tạo SparkSession với cấu hình Iceberg."""
    spark = SparkSession.builder \
        .appName("Gold-AggregateSales") \
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


def create_daily_sales_by_category(spark) -> None:
    """
    Tạo bảng doanh thu theo ngày và danh mục.
    Sử dụng partitioning theo ngày để tối ưu truy vấn time-series.
    """
    print("\n" + "="*60)
    print("GOLD LAYER: DAILY SALES BY CATEGORY")
    print("="*60)
    
    silver_df = spark.table("iceberg.silver.events_cleaned")
    
    # Filter only purchase events
    purchases_df = silver_df.filter(col("event_type") == "purchase")
    
    # Aggregate by date and category
    daily_sales_df = purchases_df \
        .groupBy(
            "event_date",
            "category_level1",
            "category_level2"
        ) \
        .agg(
            count("*").alias("order_count"),
            countDistinct("user_id").alias("unique_customers"),
            countDistinct("product_id").alias("unique_products"),
            spark_sum("price").alias("total_revenue"),
            avg("price").alias("avg_order_value"),
            spark_min("price").alias("min_order_value"),
            spark_max("price").alias("max_order_value")
        ) \
        .withColumn("revenue_per_customer", 
            spark_round(col("total_revenue") / col("unique_customers"), 2)
        ) \
        .withColumn("sale_year", year(col("event_date"))) \
        .withColumn("sale_month", month(col("event_date"))) \
        .withColumn("sale_quarter", quarter(col("event_date"))) \
        .withColumn("sale_week", weekofyear(col("event_date"))) \
        .withColumn("_aggregated_at", current_timestamp()) \
        .orderBy("event_date", "category_level1")
    
    # Write with partitioning
    daily_sales_df.writeTo("iceberg.gold.daily_sales_by_category") \
        .tableProperty("format-version", "2") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .partitionedBy("sale_year", "sale_month") \
        .createOrReplace()
    
    # Apply Z-Ordering for better query performance
    spark.sql("""
        CALL iceberg.system.rewrite_data_files(
            table => 'iceberg.gold.daily_sales_by_category',
            strategy => 'sort',
            sort_order => 'category_level1, event_date'
        )
    """)
    
    print(f"✓ Created daily_sales_by_category with {daily_sales_df.count():,} rows")
    daily_sales_df.show(10, truncate=False)


def create_funnel_analysis(spark) -> None:
    """
    Tạo bảng phân tích phễu chuyển đổi.
    View → Cart → Purchase
    """
    print("\n" + "="*60)
    print("GOLD LAYER: FUNNEL ANALYSIS")
    print("="*60)
    
    silver_df = spark.table("iceberg.silver.events_cleaned")
    
    # Aggregate funnel metrics by date
    funnel_df = silver_df \
        .groupBy("event_date") \
        .agg(
            # Total events by type
            spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("views"),
            spark_sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("carts"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            
            # Unique users by type
            countDistinct(when(col("event_type") == "view", col("user_id"))).alias("unique_viewers"),
            countDistinct(when(col("event_type") == "cart", col("user_id"))).alias("unique_carters"),
            countDistinct(when(col("event_type") == "purchase", col("user_id"))).alias("unique_purchasers"),
            
            # Revenue
            spark_sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("total_revenue")
        )
    
    # Calculate conversion rates
    funnel_with_rates_df = funnel_df \
        .withColumn("view_to_cart_rate",
            spark_round(col("carts") * 100.0 / col("views"), 2)
        ) \
        .withColumn("cart_to_purchase_rate",
            when(col("carts") > 0, 
                 spark_round(col("purchases") * 100.0 / col("carts"), 2))
            .otherwise(0)
        ) \
        .withColumn("overall_conversion_rate",
            spark_round(col("purchases") * 100.0 / col("views"), 4)
        ) \
        .withColumn("user_view_to_cart_rate",
            spark_round(col("unique_carters") * 100.0 / col("unique_viewers"), 2)
        ) \
        .withColumn("user_cart_to_purchase_rate",
            when(col("unique_carters") > 0,
                 spark_round(col("unique_purchasers") * 100.0 / col("unique_carters"), 2))
            .otherwise(0)
        ) \
        .withColumn("avg_revenue_per_purchaser",
            when(col("unique_purchasers") > 0,
                 spark_round(col("total_revenue") / col("unique_purchasers"), 2))
            .otherwise(0)
        ) \
        .withColumn("analysis_year", year(col("event_date"))) \
        .withColumn("analysis_month", month(col("event_date"))) \
        .withColumn("_aggregated_at", current_timestamp()) \
        .orderBy("event_date")
    
    # Write to Gold
    funnel_with_rates_df.writeTo("iceberg.gold.funnel_analysis") \
        .tableProperty("format-version", "2") \
        .partitionedBy("analysis_year", "analysis_month") \
        .createOrReplace()
    
    print(f"✓ Created funnel_analysis with {funnel_with_rates_df.count():,} rows")
    funnel_with_rates_df.show(10, truncate=False)
    
    # Summary stats
    print("\n📊 Funnel Summary:")
    spark.sql("""
        SELECT 
            SUM(views) as total_views,
            SUM(carts) as total_carts,
            SUM(purchases) as total_purchases,
            ROUND(SUM(carts) * 100.0 / SUM(views), 2) as overall_view_to_cart_pct,
            ROUND(SUM(purchases) * 100.0 / SUM(carts), 2) as overall_cart_to_purchase_pct,
            ROUND(SUM(purchases) * 100.0 / SUM(views), 4) as overall_conversion_pct
        FROM iceberg.gold.funnel_analysis
    """).show(truncate=False)


def create_customer_rfm(spark) -> None:
    """
    Tạo bảng RFM (Recency, Frequency, Monetary) Analysis.
    Phân khúc khách hàng dựa trên hành vi mua hàng.
    """
    print("\n" + "="*60)
    print("GOLD LAYER: CUSTOMER RFM SEGMENTATION")
    print("="*60)
    
    silver_df = spark.table("iceberg.silver.events_cleaned")
    
    # Filter purchase events
    purchases_df = silver_df.filter(col("event_type") == "purchase")
    
    # Get the reference date (latest date in data)
    max_date = purchases_df.agg(spark_max("event_date")).collect()[0][0]
    print(f"Reference date for RFM: {max_date}")
    
    # Calculate RFM metrics per customer
    rfm_df = purchases_df \
        .groupBy("user_id") \
        .agg(
            # Recency: days since last purchase
            datediff(lit(max_date), spark_max("event_date")).alias("recency"),
            # Frequency: number of purchases
            count("*").alias("frequency"),
            # Monetary: total spending
            spark_sum("price").alias("monetary"),
            # Additional metrics
            spark_min("event_date").alias("first_purchase_date"),
            spark_max("event_date").alias("last_purchase_date"),
            avg("price").alias("avg_order_value"),
            countDistinct("product_id").alias("unique_products_bought")
        )
    
    # Calculate RFM scores using ntile (1-5 scale)
    # Lower recency = better (score 5)
    # Higher frequency = better (score 5)
    # Higher monetary = better (score 5)
    
    rfm_scored_df = rfm_df \
        .withColumn("r_score", 
            ntile(5).over(Window.orderBy(col("recency").desc()))
        ) \
        .withColumn("f_score",
            ntile(5).over(Window.orderBy(col("frequency")))
        ) \
        .withColumn("m_score",
            ntile(5).over(Window.orderBy(col("monetary")))
        ) \
        .withColumn("rfm_score",
            col("r_score") + col("f_score") + col("m_score")
        ) \
        .withColumn("rfm_string",
            concat_ws("", col("r_score"), col("f_score"), col("m_score"))
        )
    
    # Assign customer segments based on RFM
    rfm_segmented_df = rfm_scored_df \
        .withColumn("customer_segment",
            when((col("r_score") >= 4) & (col("f_score") >= 4) & (col("m_score") >= 4), "Champions")
            .when((col("r_score") >= 3) & (col("f_score") >= 3) & (col("m_score") >= 3), "Loyal Customers")
            .when((col("r_score") >= 4) & (col("f_score") <= 2), "New Customers")
            .when((col("r_score") >= 3) & (col("f_score") >= 3) & (col("m_score") <= 2), "Potential Loyalists")
            .when((col("r_score") <= 2) & (col("f_score") >= 4), "At Risk")
            .when((col("r_score") <= 2) & (col("f_score") <= 2) & (col("m_score") >= 3), "Can't Lose Them")
            .when((col("r_score") <= 2) & (col("f_score") <= 2), "Lost Customers")
            .otherwise("Others")
        ) \
        .withColumn("segment_date", lit(max_date)) \
        .withColumn("_aggregated_at", current_timestamp())
    
    # Write to Gold
    rfm_segmented_df.writeTo("iceberg.gold.customer_rfm") \
        .tableProperty("format-version", "2") \
        .createOrReplace()
    
    print(f"✓ Created customer_rfm with {rfm_segmented_df.count():,} customers")
    
    # Show segment distribution
    print("\n📊 Customer Segment Distribution:")
    spark.sql("""
        SELECT 
            customer_segment,
            COUNT(*) as customer_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
            ROUND(AVG(monetary), 2) as avg_monetary,
            ROUND(AVG(frequency), 1) as avg_frequency,
            ROUND(AVG(recency), 1) as avg_recency
        FROM iceberg.gold.customer_rfm
        GROUP BY customer_segment
        ORDER BY customer_count DESC
    """).show(truncate=False)


def create_hourly_traffic(spark) -> None:
    """
    Tạo bảng phân tích traffic theo giờ.
    Hữu ích cho việc lên lịch marketing campaigns.
    """
    print("\n" + "="*60)
    print("GOLD LAYER: HOURLY TRAFFIC PATTERNS")
    print("="*60)
    
    silver_df = spark.table("iceberg.silver.events_cleaned")
    
    hourly_df = silver_df \
        .groupBy("event_date", "event_hour", "event_type") \
        .agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("user_session").alias("unique_sessions"),
            spark_sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("revenue")
        ) \
        .withColumn("day_of_week", dayofmonth(col("event_date")) % 7) \
        .withColumn("_aggregated_at", current_timestamp()) \
        .orderBy("event_date", "event_hour")
    
    hourly_df.writeTo("iceberg.gold.hourly_traffic") \
        .tableProperty("format-version", "2") \
        .partitionedBy("event_date") \
        .createOrReplace()
    
    print(f"✓ Created hourly_traffic with {hourly_df.count():,} rows")


def create_product_performance(spark) -> None:
    """
    Tạo bảng hiệu suất sản phẩm.
    Top sellers, conversion rates per product.
    """
    print("\n" + "="*60)
    print("GOLD LAYER: PRODUCT PERFORMANCE")
    print("="*60)
    
    silver_df = spark.table("iceberg.silver.events_cleaned")
    
    product_perf_df = silver_df \
        .groupBy("product_id", "category_level1", "category_level2", "brand") \
        .agg(
            spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias("views"),
            spark_sum(when(col("event_type") == "cart", 1).otherwise(0)).alias("carts"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            spark_sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("total_revenue"),
            avg("price").alias("avg_price"),
            countDistinct("user_id").alias("unique_users")
        ) \
        .withColumn("cart_rate",
            when(col("views") > 0, spark_round(col("carts") * 100.0 / col("views"), 2))
            .otherwise(0)
        ) \
        .withColumn("purchase_rate",
            when(col("views") > 0, spark_round(col("purchases") * 100.0 / col("views"), 4))
            .otherwise(0)
        ) \
        .withColumn("cart_to_purchase_rate",
            when(col("carts") > 0, spark_round(col("purchases") * 100.0 / col("carts"), 2))
            .otherwise(0)
        ) \
        .withColumn("_aggregated_at", current_timestamp())
    
    # Rank products by revenue
    product_ranked_df = product_perf_df \
        .withColumn("revenue_rank",
            dense_rank().over(Window.orderBy(col("total_revenue").desc()))
        ) \
        .withColumn("purchase_rank",
            dense_rank().over(Window.orderBy(col("purchases").desc()))
        )
    
    product_ranked_df.writeTo("iceberg.gold.product_performance") \
        .tableProperty("format-version", "2") \
        .createOrReplace()
    
    print(f"✓ Created product_performance with {product_ranked_df.count():,} products")
    
    # Show top 10 products
    print("\n📊 Top 10 Products by Revenue:")
    spark.sql("""
        SELECT 
            product_id,
            brand,
            category_level1,
            purchases,
            ROUND(total_revenue, 2) as revenue,
            purchase_rate,
            revenue_rank
        FROM iceberg.gold.product_performance
        ORDER BY revenue_rank
        LIMIT 10
    """).show(truncate=False)


from pyspark.sql.functions import dense_rank

def main():
    """Main Gold layer aggregation pipeline."""
    spark = get_spark_session()
    
    try:
        # Create namespace
        create_namespace_if_not_exists(spark, "gold")
        
        # Create aggregated tables
        create_daily_sales_by_category(spark)
        create_funnel_analysis(spark)
        create_customer_rfm(spark)
        create_hourly_traffic(spark)
        create_product_performance(spark)
        
        print("\n" + "="*60)
        print("✅ GOLD LAYER AGGREGATION COMPLETED!")
        print("="*60)
        print("\nCreated tables:")
        print("  - iceberg.gold.daily_sales_by_category")
        print("  - iceberg.gold.funnel_analysis")
        print("  - iceberg.gold.customer_rfm")
        print("  - iceberg.gold.hourly_traffic")
        print("  - iceberg.gold.product_performance")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
