-- =============================================================================
-- ClickHouse - Iceberg Integration
-- =============================================================================
-- Sử dụng IcebergS3 Engine để truy vấn trực tiếp Iceberg tables trên MinIO
-- Zero-Copy Architecture: Không cần copy dữ liệu, đọc trực tiếp từ S3
-- =============================================================================

-- Create database for lakehouse
CREATE DATABASE IF NOT EXISTS lakehouse;

-- =============================================================================
-- OPTION 1: IcebergS3 Engine (Zero-Copy - Recommended)
-- =============================================================================
-- ClickHouse đọc trực tiếp từ Iceberg tables trên MinIO

-- Daily Sales by Category (Gold Layer)
CREATE TABLE IF NOT EXISTS lakehouse.daily_sales_iceberg
ENGINE = IcebergS3(
    'http://minio:9000/iceberg-warehouse/gold/daily_sales_by_category',
    'minioadmin',
    'minioadmin123'
);

-- Funnel Analysis (Gold Layer)
CREATE TABLE IF NOT EXISTS lakehouse.funnel_analysis_iceberg
ENGINE = IcebergS3(
    'http://minio:9000/iceberg-warehouse/gold/funnel_analysis',
    'minioadmin',
    'minioadmin123'
);

-- Customer RFM (Gold Layer)
CREATE TABLE IF NOT EXISTS lakehouse.customer_rfm_iceberg
ENGINE = IcebergS3(
    'http://minio:9000/iceberg-warehouse/gold/customer_rfm',
    'minioadmin',
    'minioadmin123'
);

-- Product Performance (Gold Layer)
CREATE TABLE IF NOT EXISTS lakehouse.product_performance_iceberg
ENGINE = IcebergS3(
    'http://minio:9000/iceberg-warehouse/gold/product_performance',
    'minioadmin',
    'minioadmin123'
);

-- =============================================================================
-- OPTION 2: Native ClickHouse Tables (Data Copy)
-- =============================================================================
-- Alternative approach: Copy data from Iceberg to native ClickHouse tables
-- Pros: Faster queries with ClickHouse optimizations
-- Cons: Data duplication, need sync mechanism

-- Daily Sales - Native Table
CREATE TABLE IF NOT EXISTS lakehouse.daily_sales
(
    sale_date Date,
    category_level1 String,
    category_level2 String,
    order_count UInt32,
    unique_customers UInt32,
    unique_products UInt32,
    total_revenue Decimal(18, 2),
    avg_order_value Decimal(18, 2),
    min_order_value Decimal(18, 2),
    max_order_value Decimal(18, 2),
    revenue_per_customer Decimal(18, 2),
    sale_year UInt16,
    sale_month UInt8,
    sale_quarter UInt8,
    sale_week UInt8,
    _aggregated_at DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(sale_date)
ORDER BY (sale_date, category_level1, category_level2)
SETTINGS index_granularity = 8192;

-- Funnel Analysis - Native Table
CREATE TABLE IF NOT EXISTS lakehouse.funnel_analysis
(
    analysis_date Date,
    views UInt64,
    carts UInt64,
    purchases UInt64,
    unique_viewers UInt32,
    unique_carters UInt32,
    unique_purchasers UInt32,
    total_revenue Decimal(18, 2),
    view_to_cart_rate Decimal(5, 2),
    cart_to_purchase_rate Decimal(5, 2),
    overall_conversion_rate Decimal(8, 4),
    user_view_to_cart_rate Decimal(5, 2),
    user_cart_to_purchase_rate Decimal(5, 2),
    avg_revenue_per_purchaser Decimal(18, 2),
    analysis_year UInt16,
    analysis_month UInt8,
    _aggregated_at DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(analysis_date)
ORDER BY analysis_date
SETTINGS index_granularity = 8192;

-- Customer RFM - Native Table
CREATE TABLE IF NOT EXISTS lakehouse.customer_rfm
(
    user_id UInt64,
    recency UInt32,
    frequency UInt32,
    monetary Decimal(18, 2),
    r_score UInt8,
    f_score UInt8,
    m_score UInt8,
    rfm_score UInt8,
    rfm_string String,
    customer_segment String,
    first_purchase_date Date,
    last_purchase_date Date,
    avg_order_value Decimal(18, 2),
    unique_products_bought UInt32,
    segment_date Date,
    _aggregated_at DateTime
)
ENGINE = MergeTree()
ORDER BY (customer_segment, rfm_score, user_id)
SETTINGS index_granularity = 8192;

-- Product Performance - Native Table
CREATE TABLE IF NOT EXISTS lakehouse.product_performance
(
    product_id UInt64,
    category_level1 String,
    category_level2 String,
    brand String,
    views UInt64,
    carts UInt64,
    purchases UInt64,
    total_revenue Decimal(18, 2),
    avg_price Decimal(18, 2),
    unique_users UInt32,
    cart_rate Decimal(5, 2),
    purchase_rate Decimal(8, 4),
    cart_to_purchase_rate Decimal(5, 2),
    revenue_rank UInt32,
    purchase_rank UInt32,
    _aggregated_at DateTime
)
ENGINE = MergeTree()
ORDER BY (category_level1, revenue_rank, product_id)
SETTINGS index_granularity = 8192;

-- =============================================================================
-- Skip Indices for faster queries
-- =============================================================================

-- Add skip indices for filtering optimization
ALTER TABLE lakehouse.daily_sales
    ADD INDEX idx_category_bloom category_level1 TYPE bloom_filter GRANULARITY 1;

ALTER TABLE lakehouse.customer_rfm
    ADD INDEX idx_segment_bloom customer_segment TYPE bloom_filter GRANULARITY 1;

ALTER TABLE lakehouse.product_performance
    ADD INDEX idx_brand_bloom brand TYPE bloom_filter GRANULARITY 1;

-- =============================================================================
-- Materialized Views for pre-aggregation
-- =============================================================================

-- Monthly Sales Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS lakehouse.mv_monthly_sales
ENGINE = SummingMergeTree()
PARTITION BY sale_year
ORDER BY (sale_year, sale_month, category_level1)
AS SELECT
    sale_year,
    sale_month,
    category_level1,
    sum(order_count) AS total_orders,
    sum(unique_customers) AS total_customers,
    sum(total_revenue) AS total_revenue
FROM lakehouse.daily_sales
GROUP BY sale_year, sale_month, category_level1;

-- Segment Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS lakehouse.mv_segment_summary
ENGINE = SummingMergeTree()
ORDER BY customer_segment
AS SELECT
    customer_segment,
    count() AS customer_count,
    sum(monetary) AS total_revenue,
    avg(recency) AS avg_recency,
    avg(frequency) AS avg_frequency
FROM lakehouse.customer_rfm
GROUP BY customer_segment;
