# 🏗️ Data Lakehouse - Open Source Platform

> **DATAFLOW 2026: THE ALCHEMY OF MINDS**
> 
> Full-stack Open-Source Data Lakehouse Platform sử dụng kiến trúc Medallion

[![Docker](https://img.shields.io/badge/Docker-Ready-blue?logo=docker)](https://docker.com)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange?logo=apachespark)](https://spark.apache.org)
[![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1.4-blue)](https://iceberg.apache.org)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-24.1-yellow?logo=clickhouse)](https://clickhouse.com)
[![Apache Superset](https://img.shields.io/badge/Apache%20Superset-3.1-cyan)](https://superset.apache.org)

---

## 📋 Mục Lục

- [Giới Thiệu](#-giới-thiệu)
- [Kiến Trúc Hệ Thống](#-kiến-trúc-hệ-thống)
- [Công Nghệ Sử Dụng](#-công-nghệ-sử-dụng)
- [Yêu Cầu Hệ Thống](#-yêu-cầu-hệ-thống)
- [Cài Đặt](#-cài-đặt)
- [Hướng Dẫn Sử Dụng](#-hướng-dẫn-sử-dụng)
- [Data Pipeline](#-data-pipeline)
- [Schema Evolution Demo](#-schema-evolution-demo)
- [Dashboards](#-dashboards)
- [Cấu Trúc Dự Án](#-cấu-trúc-dự-án)
- [Troubleshooting](#-troubleshooting)

---

## 📖 Tài Liệu Hướng Dẫn

> **🎯 Bắt đầu nhanh?** Xem [QUICK_START.md](./QUICK_START.md) - Hướng dẫn chạy project trong 5 phút

> **📘 Cần hướng dẫn chi tiết?** Xem [HUONG_DAN_CHAY_PROJECT.md](./HUONG_DAN_CHAY_PROJECT.md) - Tài liệu đầy đủ từ setup đến troubleshooting

> **📁 Muốn hiểu cấu trúc project?** Xem [CAU_TRUC_PROJECT.md](./CAU_TRUC_PROJECT.md) - Giải thích chi tiết từng thư mục và file

---

## 🎯 Giới Thiệu

### Bài Toán

Dự án xây dựng hệ thống **Data Lakehouse** hoàn chỉnh từ con số 0, sử dụng các công nghệ mã nguồn mở để thay thế các dịch vụ cloud managed:

| Cloud Service | Open-Source Alternative |
|--------------|------------------------|
| AWS S3 | **MinIO** |
| Databricks | **Apache Spark + Iceberg** |
| Snowflake | **ClickHouse** |

### Use Case: E-commerce Event History

**Dataset**: [eCommerce Events History in Cosmetics Shop](https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop)

- **Quy mô**: ~20 triệu events
- **Loại dữ liệu**: User behavior (view, cart, purchase)
- **Thời gian**: Oct 2019 - Apr 2020

### Bài Toán Nghiệp Vụ

1. 📊 **Phân tích phễu chuyển đổi** (Funnel Analysis)
2. 💰 **Phân tích doanh thu theo thời gian** (Revenue Analysis)
3. 👥 **Phân khúc khách hàng RFM** (Customer Segmentation)

---

## 🏛️ Kiến Trúc Hệ Thống

### Medallion Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        VISUALIZATION LAYER                          │
│                         Apache Superset                             │
│                    (Dashboard & BI Reports)                         │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         SERVING LAYER                                │
│                          ClickHouse                                  │
│               (OLAP Database - Sub-second Queries)                   │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      TRANSFORMATION LAYER                            │
│              dbt (data build tool) + Apache Spark                    │
│                                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐           │
│  │    BRONZE    │───▶│    SILVER    │───▶│     GOLD     │           │
│  │  (Raw Data)  │    │(Cleaned Data)│    │(Aggregated)  │           │
│  └──────────────┘    └──────────────┘    └──────────────┘           │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        TABLE FORMAT                                  │
│                       Apache Iceberg                                 │
│    [Schema Evolution, Time Travel, Partitioning, Z-Ordering]        │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        STORAGE LAYER                                 │
│                           MinIO                                      │
│                (S3-Compatible Object Storage)                        │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
CSV Files → Spark → Bronze (Iceberg) → Silver (Iceberg) → Gold (Iceberg) → ClickHouse → Superset
```

---

## 🔧 Công Nghệ Sử Dụng

| Tầng | Công Nghệ | Phiên Bản | Mục Đích |
|------|-----------|-----------|----------|
| Storage | MinIO | 2024.01 | Object Storage (S3-compatible) |
| Table Format | Apache Iceberg | 1.4.3 | ACID transactions, Schema Evolution |
| Compute | Apache Spark | 3.5.0 | Distributed Processing |
| Transformation | dbt | 1.7+ | Data Modeling |
| Serving | ClickHouse | 24.1 | OLAP Queries |
| Visualization | Apache Superset | 3.1.0 | Dashboards & BI |
| Orchestration | Docker Compose | 3.8 | Container Management |

---

## 💻 Yêu Cầu Hệ Thống

### Hardware

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| RAM | 8 GB (đã tối ưu với cấu hình lightweight) | **16 GB** (thoải mái hơn, nhất là khi chạy full pipeline + ClickHouse) |
| CPU | 4 cores | 8 cores |
| Disk | 20 GB | 50 GB |

### Software

- Docker Desktop 4.0+
- Docker Compose 2.0+
- Make (optional, cho automation)
- Python 3.9+ (cho Superset setup)

---

## 🚀 Cài Đặt & Cách Chạy Project

> **⚠️ LƯU Ý QUAN TRỌNG:** Project này **KHÔNG sử dụng AWS services**. Tất cả đều dùng open-source self-hosted:
> - **MinIO** thay S3
> - **Apache Spark + Iceberg** thay Databricks  
> - **ClickHouse** thay Snowflake
> - Sử dụng **HadoopFileIO** thay vì AWS S3FileIO

### Bước 1: Clone Repository

```bash
git clone https://github.com/thequang05/auto_scaling.git
cd auto_scaling
```

### Bước 2: Tải Dataset

Download dataset từ Kaggle và đặt vào thư mục `data/raw/`:

```bash
# Tạo thư mục
mkdir -p data/raw

# Download từ Kaggle:
# https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop

# Giải nén và đặt các file CSV vào data/raw/
# Ví dụ: 2019-Oct.csv, 2019-Nov.csv, ...
```

### Bước 3: Build và Khởi động Services

```powershell
# Di chuyển vào thư mục docker
cd docker

# Build images
docker compose build

# Khởi động TẤT CẢ services
docker compose up -d

# Kiểm tra trạng thái
docker ps
```

**Đợi khoảng 1-2 phút để tất cả services healthy.**

**Services sau khi khởi động:**

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Iceberg REST | http://localhost:8181 | - |
| Spark Master UI | http://localhost:8080 | - |
| ClickHouse | http://localhost:8123 | default / clickhouse123 |
| Superset | http://localhost:8088 | admin / admin |

---

## 📖 Hướng Dẫn Chạy Data Pipeline

### Bước 1: Bronze Layer - Ingestion

Ingest dữ liệu CSV vào Bronze layer (Iceberg tables trên MinIO):

```powershell
docker exec spark-master spark-submit `
    --master spark://spark-master:7077 `
    --conf spark.driver.memory=2g `
    --conf spark.executor.memory=2g `
    /opt/spark-apps/jobs/bronze/ingest_events.py
```

**Kết quả:** ~20 triệu records được ghi vào `iceberg.bronze.events_raw`

### Bước 2: Silver Layer - Transformation

Làm sạch và transform dữ liệu:

```powershell
docker exec spark-master spark-submit `
    --master spark://spark-master:7077 `
    --conf spark.driver.memory=2g `
    --conf spark.executor.memory=2g `
    /opt/spark-apps/jobs/silver/clean_events.py
```

**Kết quả:** Dữ liệu được deduplicate, xử lý NULL, và chuẩn hóa

### Bước 3: Gold Layer - Aggregation

Tạo các bảng aggregate cho business analytics:

```powershell
docker exec spark-master spark-submit `
    --master spark://spark-master:7077 `
    --conf spark.driver.memory=2g `
    --conf spark.executor.memory=2g `
    /opt/spark-apps/jobs/gold/aggregate_sales.py
```

**Kết quả:** 4 bảng Gold được tạo:
- `iceberg.gold.daily_sales` - Doanh thu theo ngày/category
- `iceberg.gold.funnel_analysis` - Phân tích funnel chuyển đổi
- `iceberg.gold.customer_rfm` - Phân khúc khách hàng RFM
- `iceberg.gold.product_performance` - Hiệu suất sản phẩm

### Bước 4: Sync Gold Layer → ClickHouse

Export dữ liệu từ Iceberg sang ClickHouse cho serving layer:

**4.1. Tạo bảng trong ClickHouse:**

```powershell
docker exec clickhouse clickhouse-client --password clickhouse123 --multiquery --query "
CREATE DATABASE IF NOT EXISTS lakehouse;

CREATE TABLE IF NOT EXISTS lakehouse.daily_sales (
    event_date Date,
    category_level1 String,
    category_level2 String,
    order_count UInt64,
    unique_customers UInt64,
    unique_products UInt64,
    total_revenue Float64,
    avg_order_value Float64,
    min_order_value Float64,
    max_order_value Float64,
    revenue_per_customer Float64,
    sale_year UInt16,
    sale_month UInt8,
    sale_quarter UInt8,
    sale_week UInt8,
    _aggregated_at DateTime64(6)
) ENGINE = MergeTree() ORDER BY (event_date, category_level1);

CREATE TABLE IF NOT EXISTS lakehouse.funnel_analysis (
    event_date Date,
    category_level1 String,
    views UInt64,
    carts UInt64,
    purchases UInt64,
    unique_viewers UInt64,
    unique_carters UInt64,
    unique_purchasers UInt64,
    total_revenue Float64,
    view_to_cart_rate Float64,
    cart_to_purchase_rate Float64,
    overall_conversion_rate Float64,
    user_view_to_cart_rate Float64,
    user_cart_to_purchase_rate Float64,
    avg_revenue_per_purchaser Float64,
    analysis_year UInt16,
    analysis_month UInt8,
    _aggregated_at DateTime64(6)
) ENGINE = MergeTree() ORDER BY (event_date, category_level1);

CREATE TABLE IF NOT EXISTS lakehouse.customer_rfm (
    user_id UInt64,
    recency Int32,
    frequency Int64,
    monetary Float64,
    first_purchase_date Date,
    last_purchase_date Date,
    avg_order_value Float64,
    unique_products_bought Int64,
    r_score UInt8,
    f_score UInt8,
    m_score UInt8,
    rfm_score UInt16,
    rfm_string String,
    customer_segment String,
    segment_date Date,
    _aggregated_at DateTime64(6)
) ENGINE = MergeTree() ORDER BY (user_id);

CREATE TABLE IF NOT EXISTS lakehouse.product_performance (
    product_id UInt64,
    category_level1 String,
    category_level2 String,
    brand String,
    view_count UInt64,
    cart_count UInt64,
    purchase_count UInt64,
    unique_viewers UInt64,
    unique_carters UInt64,
    unique_purchasers UInt64,
    total_revenue Float64,
    avg_price Float64,
    min_price Float64,
    max_price Float64,
    view_to_cart_rate Float64,
    cart_to_purchase_rate Float64,
    overall_conversion_rate Float64,
    revenue_per_view Float64,
    _aggregated_at DateTime64(6)
) ENGINE = MergeTree() ORDER BY (product_id);
"
```

**4.2. Export và Import dữ liệu:**

```powershell
# Export CSV từ Iceberg
docker exec spark-master spark-submit `
    --master spark://spark-master:7077 `
    /opt/spark-apps/jobs/serving/export_csv.py

# Import vào ClickHouse
docker exec -i clickhouse bash -c "cat /tmp/daily_sales.csv | clickhouse-client --password clickhouse123 --query 'INSERT INTO lakehouse.daily_sales FORMAT CSVWithNames'"
docker exec -i clickhouse bash -c "cat /tmp/funnel_analysis.csv | clickhouse-client --password clickhouse123 --query 'INSERT INTO lakehouse.funnel_analysis FORMAT CSVWithNames'"
docker exec -i clickhouse bash -c "cat /tmp/customer_rfm.csv | clickhouse-client --password clickhouse123 --query 'INSERT INTO lakehouse.customer_rfm FORMAT CSVWithNames'"
docker exec -i clickhouse bash -c "cat /tmp/product_performance.csv | clickhouse-client --password clickhouse123 --query 'INSERT INTO lakehouse.product_performance FORMAT CSVWithNames'"
```

**4.3. Kiểm tra kết quả:**

```powershell
docker exec clickhouse clickhouse-client --password clickhouse123 --multiquery --query "
SELECT 'daily_sales' as tbl, count() as rows FROM lakehouse.daily_sales;
SELECT 'funnel_analysis' as tbl, count() as rows FROM lakehouse.funnel_analysis;
SELECT 'customer_rfm' as tbl, count() as rows FROM lakehouse.customer_rfm;
SELECT 'product_performance' as tbl, count() as rows FROM lakehouse.product_performance;
"
```

---

## 📊 Bước 5: Tạo Dashboard trong Superset

### 5.1. Truy cập Superset

Mở trình duyệt: **http://localhost:8088**
- Username: `admin`
- Password: `admin`

### 5.2. Kết nối ClickHouse Database

1. Vào **Settings** → **Database Connections** → **+ Database**
2. Chọn **ClickHouse Connect**
3. SQLAlchemy URI:
   ```
   clickhousedb://default:clickhouse123@clickhouse:8123/lakehouse
   ```
4. Click **Test Connection** → **Connect**

### 5.3. Tạo Datasets

1. Vào **Data** → **Datasets** → **+ Dataset**
2. Tạo 4 datasets cho 4 bảng:
   - `lakehouse.daily_sales`
   - `lakehouse.funnel_analysis`
   - `lakehouse.customer_rfm`
   - `lakehouse.product_performance`

### 5.4. Tạo Charts (4 KPI Charts)

| Chart | Dataset | Type | Cấu hình |
|-------|---------|------|----------|
| Revenue Trend | daily_sales | Line Chart | X: event_date, Y: SUM(total_revenue) |
| Conversion Funnel | funnel_analysis | Bar Chart | Metrics: SUM(views), SUM(carts), SUM(purchases) |
| Customer Segments | customer_rfm | Pie Chart | Dimension: customer_segment, Metric: COUNT(*) |
| Top Categories | product_performance | Bar Chart | X: category_level1, Y: SUM(total_revenue) |

### 5.5. Tạo Dashboard

1. **Dashboards** → **+ Dashboard**
2. Đặt tên: `E-commerce Analytics`
3. Kéo thả 4 charts vào dashboard
4. **Save**

---

## 🛠️ Các Lệnh Thường Dùng

### Docker Commands (Windows PowerShell)

```powershell
# Khởi động tất cả services
cd docker
docker compose up -d

# Dừng tất cả services
docker compose down

# Xem logs
docker compose logs -f

# Kiểm tra trạng thái
docker ps

# Restart một service cụ thể
docker compose restart clickhouse
```

### Spark Commands

```powershell
# Chạy Bronze layer
docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/jobs/bronze/ingest_events.py

# Chạy Silver layer
docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/jobs/silver/clean_events.py

# Chạy Gold layer
docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark-apps/jobs/gold/aggregate_sales.py

# Mở PySpark Shell
docker exec -it spark-master pyspark --master spark://spark-master:7077
```

### ClickHouse Commands

```powershell
# Mở ClickHouse client
docker exec -it clickhouse clickhouse-client --password clickhouse123

# Query trực tiếp
docker exec clickhouse clickhouse-client --password clickhouse123 --query "SELECT count() FROM lakehouse.daily_sales"

# Xem tất cả tables
docker exec clickhouse clickhouse-client --password clickhouse123 --query "SHOW TABLES FROM lakehouse"
```

---

## 🔄 Data Pipeline

### Bronze Layer (Raw Data)

```python
# spark/jobs/bronze/ingest_events.py

# Đọc CSV → Ghi Iceberg với metadata
bronze_df = spark.read.csv("data/raw/*.csv")
bronze_df.withColumn("_ingestion_time", current_timestamp())
         .withColumn("_source_file", input_file_name())
         .writeTo("iceberg.bronze.events_raw")
         .partitionedBy("event_date")
         .create()
```

**Bảng**: `iceberg.bronze.events_raw`
- Partition by: `event_date`
- Giữ nguyên dữ liệu gốc + metadata columns

### Silver Layer (Cleaned Data)

```python
# spark/jobs/silver/clean_events.py

# Làm sạch: Deduplication, NULL handling, Type casting
silver_df = bronze_df
    .dropDuplicates(["event_time", "user_id", "product_id"])
    .withColumn("brand", coalesce(col("brand"), lit("Unknown")))
    .withColumn("category_level1", split(col("category_code"), "\\.")[0])
```

**Bảng**:
- `iceberg.silver.events_cleaned` - Events đã làm sạch
- `iceberg.silver.dim_products` - Product dimension
- `iceberg.silver.dim_users` - User dimension

### Gold Layer (Business Aggregations)

```python
# spark/jobs/gold/aggregate_sales.py

# Tạo aggregated tables cho báo cáo
daily_sales = silver_df
    .filter(col("event_type") == "purchase")
    .groupBy("event_date", "category_level1")
    .agg(sum("price").alias("revenue"))
```

**Bảng**:
- `iceberg.gold.daily_sales_by_category` - Doanh thu theo ngày/category
- `iceberg.gold.funnel_analysis` - Phân tích funnel
- `iceberg.gold.customer_rfm` - RFM segmentation
- `iceberg.gold.product_performance` - Product metrics

---

## 🔄 Schema Evolution Demo

Một trong những tính năng mạnh mẽ của Iceberg là **Schema Evolution** - khả năng thay đổi schema mà không cần rewrite data.

### Demo: Thêm cột `payment_method`

```bash
# Chạy demo
make demo-schema-evolution
```

```python
# Ngày T: Schema ban đầu (không có payment_method)
# Ngày T+1: Thêm cột mới
spark.sql("""
    ALTER TABLE iceberg.bronze.events_raw 
    ADD COLUMN payment_method STRING
""")

# Iceberg tự động xử lý:
# - Dữ liệu cũ: payment_method = NULL
# - Dữ liệu mới: có giá trị payment_method
# - KHÔNG rewrite data files!
```

### Time Travel

```sql
-- Query data tại snapshot cũ
SELECT * FROM iceberg.bronze.events_raw
VERSION AS OF 123456789;

-- Query data tại thời điểm cụ thể
SELECT * FROM iceberg.bronze.events_raw
TIMESTAMP AS OF '2024-01-15 10:00:00';
```

---

## 📊 Dashboards

### Superset Setup

```bash
# Tự động setup
make setup-superset

# Hoặc chạy script
python superset/setup_superset.py
```

### Charts Được Tạo

1. **📈 Daily Revenue Trend** (Line Chart)
   - Dataset: `daily_sales`
   - Metric: SUM(total_revenue)
   - Time grain: Day

2. **🥧 Revenue by Category** (Pie Chart)
   - Dataset: `daily_sales`
   - Dimension: category_level1

3. **📊 Conversion Funnel** (Funnel Chart)
   - View → Cart → Purchase

4. **👥 Customer Segments** (Bar Chart)
   - Dataset: `customer_rfm`
   - RFM segment distribution

5. **📋 Top Products** (Table)
   - Dataset: `product_performance`
   - Top 10 by revenue

6. **🔢 KPI Cards** (Big Number)
   - Total Revenue
   - Total Orders
   - Conversion Rate

### ClickHouse Queries

```sql
-- Top categories by revenue
SELECT category_level1, sum(total_revenue) as revenue
FROM lakehouse.daily_sales
GROUP BY category_level1
ORDER BY revenue DESC;

-- Customer segment distribution
SELECT customer_segment, count() as customers
FROM lakehouse.customer_rfm
GROUP BY customer_segment;

-- Conversion funnel
SELECT 
    sum(views) as views,
    sum(carts) as carts,
    sum(purchases) as purchases,
    round(sum(carts) * 100.0 / sum(views), 2) as view_to_cart_pct,
    round(sum(purchases) * 100.0 / sum(carts), 2) as cart_to_purchase_pct
FROM lakehouse.funnel_analysis;
```

---

## 📁 Cấu Trúc Dự Án

```
data-lakehouse/
├── docker/
│   ├── docker-compose.yml      # Main orchestration
│   ├── spark/
│   │   ├── Dockerfile          # Spark + Iceberg image
│   │   └── spark-defaults.conf
│   ├── clickhouse/
│   │   ├── config.xml
│   │   └── users.xml
│   └── superset/
│       ├── Dockerfile
│       └── superset_config.py
├── spark/
│   └── jobs/
│       ├── bronze/
│       │   ├── ingest_events.py
│       │   └── schema_evolution_demo.py
│       ├── silver/
│       │   └── clean_events.py
│       ├── gold/
│       │   └── aggregate_sales.py
│       └── serving/
│           └── sync_to_clickhouse.py
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   ├── stg_events.sql
│       │   ├── stg_products.sql
│       │   └── stg_users.sql
│       └── marts/
│           └── core/
│               ├── fct_daily_sales.sql
│               ├── fct_funnel.sql
│               └── dim_customer_rfm.sql
├── clickhouse/
│   ├── migrations/
│   │   └── 001_create_iceberg_tables.sql
│   └── queries/
│       └── sample_queries.sql
├── superset/
│   ├── dashboards/
│   │   └── dashboard_config.json
│   └── setup_superset.py
├── scripts/
│   ├── setup.sh
│   └── run_pipeline.sh
├── data/
│   └── raw/                    # Place CSV files here
├── docs/
│   └── ARCHITECTURE.md
├── Makefile
└── README.md
```

---

## 🔧 Troubleshooting

### Service không khởi động

```bash
# Kiểm tra logs
make logs

# Kiểm tra trạng thái
make status

# Khởi động lại
make restart
```

### Spark job thất bại

```bash
# Kiểm tra Spark UI
# http://localhost:8080

# Xem logs Spark
make logs-spark

# Tăng memory nếu cần
# Chỉnh sửa docker-compose.yml: SPARK_EXECUTOR_MEMORY
```

### ClickHouse connection error

```bash
# Kiểm tra ClickHouse status
docker exec clickhouse clickhouse-client --password clickhouse123 -q "SELECT 1"

# Xem logs
make logs-clickhouse
```

### Superset không load dashboard

```bash
# Khởi động lại Superset
docker compose restart superset

# Kiểm tra database connection trong Superset UI
# Settings → Database Connections
```

### Out of memory

```bash
# Giảm số lượng partitions
# Trong spark-defaults.conf:
spark.sql.shuffle.partitions=10

# Hoặc xử lý data theo batch nhỏ hơn
```

---

## 📚 Tài Liệu Tham Khảo

1. [MinIO - Building a Data Lakehouse using Apache Iceberg](https://blog.min.io/building-a-data-lakehouse-using-apache-iceberg-and-minio/)
2. [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
3. [Apache Spark with Iceberg](https://iceberg.apache.org/docs/latest/spark-getting-started/)
4. [ClickHouse Documentation](https://clickhouse.com/docs/en/)
5. [Apache Superset Documentation](https://superset.apache.org/docs/intro)
6. [dbt Documentation](https://docs.getdbt.com/)

---


---

## 📄 License

MIT License - Xem file [LICENSE](LICENSE) để biết thêm chi tiết.

---

<p align="center">
  <b>🚀 Happy Data Engineering! 🚀</b>
</p>
