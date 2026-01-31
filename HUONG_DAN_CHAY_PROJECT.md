# 📘 HƯỚNG DẪN CHẠY PROJECT DATA LAKEHOUSE

> **Tài liệu này hướng dẫn chi tiết cách setup và chạy hệ thống Data Lakehouse từ đầu đến cuối**

---

## 📋 Mục Lục

1. [Tổng Quan Dự Án](#1-tổng-quan-dự-án)
2. [Yêu Cầu Hệ Thống](#2-yêu-cầu-hệ-thống)
3. [Cài Đặt Ban Đầu](#3-cài-đặt-ban-đầu)
4. [Chuẩn Bị Dữ Liệu](#4-chuẩn-bị-dữ-liệu)
5. [Khởi Động Hệ Thống](#5-khởi-động-hệ-thống)
6. [Chạy Data Pipeline](#6-chạy-data-pipeline)
7. [Thiết Lập Superset Dashboard](#7-thiết-lập-superset-dashboard)
8. [Kiểm Tra Kết Quả](#8-kiểm-tra-kết-quả)
9. [Các Lệnh Thường Dùng](#9-các-lệnh-thường-dùng)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Tổng Quan Dự Án

### 1.1. Mục Đích

Dự án xây dựng một **Data Lakehouse Platform** hoàn chỉnh sử dụng kiến trúc **Medallion** (Bronze → Silver → Gold) với các công nghệ mã nguồn mở:

- **Storage**: MinIO (thay thế AWS S3)
- **Table Format**: Apache Iceberg (ACID transactions, Schema Evolution)
- **Compute**: Apache Spark (xử lý dữ liệu phân tán)
- **Serving**: ClickHouse (OLAP database)
- **Visualization**: Apache Superset (BI dashboards)

### 1.2. Use Case

Xử lý và phân tích dữ liệu **E-commerce Events** từ Kaggle:
- Dataset: [eCommerce Events History in Cosmetics Shop](https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop)
- Quy mô: ~20 triệu events
- Thời gian: Oct 2019 - Apr 2020

### 1.3. Kiến Trúc Hệ Thống

```
CSV Files → Spark → Bronze (Iceberg) → Silver (Iceberg) → Gold (Iceberg) → ClickHouse → Superset
```

**Các tầng dữ liệu:**
- **Bronze**: Dữ liệu thô (raw data) - giữ nguyên format gốc
- **Silver**: Dữ liệu đã làm sạch (cleaned data) - deduplication, type casting
- **Gold**: Dữ liệu nghiệp vụ (business metrics) - aggregations, KPIs

---

## 2. Yêu Cầu Hệ Thống

### 2.1. Phần Cứng

| Resource | Tối Thiểu | Khuyến Nghị |
|----------|-----------|-------------|
| RAM | 8 GB | **16 GB** |
| CPU | 4 cores | 8 cores |
| Disk | 20 GB | 50 GB |

### 2.2. Phần Mềm

- **Docker Desktop** 4.0+ (hoặc Docker Engine + Docker Compose)
- **Docker Compose** 2.0+
- **Make** (optional, nhưng khuyến nghị)
- **Python** 3.9+ (cho Superset setup script)

### 2.3. Kiểm Tra Yêu Cầu

```bash
# Kiểm tra Docker
docker --version
docker-compose --version

# Kiểm tra Make (optional)
make --version

# Kiểm tra Python
python3 --version
```

---

## 3. Cài Đặt Ban Đầu

### 3.1. Clone Repository

```bash
# Nếu chưa có, clone repository
git clone <repository-url>
cd auto_scaling
```

### 3.2. Chạy Setup Script (Khuyến Nghị)

Script tự động kiểm tra môi trường và tạo thư mục cần thiết:

```bash
chmod +x scripts/setup.sh
./scripts/setup.sh
```

Script sẽ:
- ✅ Kiểm tra Docker và Docker Compose
- ✅ Kiểm tra tài nguyên hệ thống (RAM, disk)
- ✅ Tạo các thư mục cần thiết (`data/raw`, `logs`, etc.)
- ✅ Build Docker images

### 3.3. Build Docker Images Thủ Công

Nếu không dùng setup script:

```bash
cd docker
docker-compose build
```

**Lưu ý:** Lần đầu build có thể mất 10-15 phút để download các images và dependencies.

---

## 4. Chuẩn Bị Dữ Liệu

### 4.1. Download Dataset

1. Truy cập: https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop
2. Download dataset (cần tài khoản Kaggle)
3. Giải nén file ZIP

### 4.2. Đặt Dữ Liệu Vào Thư Mục

```bash
# Tạo thư mục nếu chưa có
mkdir -p data/raw

# Copy các file CSV vào thư mục data/raw/
# Ví dụ:
cp ~/Downloads/ecommerce-events-history-in-cosmetics-shop/*.csv data/raw/
```

**Các file CSV cần có:**
- `2019-Oct.csv`
- `2019-Nov.csv`
- `2019-Dec.csv`
- `2020-Jan.csv`
- `2020-Feb.csv`
- (và các file khác nếu có)

### 4.3. Kiểm Tra Dữ Liệu

```bash
# Kiểm tra số lượng file
ls -lh data/raw/*.csv

# Kiểm tra kích thước (nên có vài GB)
du -sh data/raw/
```

---

## 5. Khởi Động Hệ Thống

### 5.1. Khởi Động Tất Cả Services

**Cách 1: Sử dụng Make (Khuyến Nghị)**

```bash
# Từ thư mục gốc của project
make up
```

**Cách 2: Sử dụng Docker Compose**

```bash
cd docker
docker-compose up -d
```

### 5.2. Kiểm Tra Services Đã Khởi Động

```bash
# Xem trạng thái tất cả containers
make status

# Hoặc
cd docker
docker-compose ps
```

**Các services cần chạy:**
- ✅ `minio` - Object Storage
- ✅ `iceberg-rest` - Iceberg REST Catalog
- ✅ `spark-master` - Spark Master
- ✅ `spark-worker` - Spark Worker
- ✅ `spark-thrift` - Spark Thrift Server (cho dbt)
- ✅ `clickhouse` - ClickHouse Database
- ✅ `superset-db` - PostgreSQL cho Superset
- ✅ `superset-cache` - Redis cho Superset
- ✅ `superset` - Apache Superset

### 5.3. Đợi Services Khởi Động Hoàn Tất

**Quan trọng:** Đợi 1-2 phút để tất cả services khởi động hoàn toàn.

Kiểm tra logs:

```bash
# Xem logs tất cả services
make logs

# Hoặc xem logs từng service
make logs-spark
make logs-clickhouse
make logs-superset
```

### 5.4. Truy Cập Web UIs

Sau khi services khởi động, truy cập các URL sau:

| Service | URL | Credentials |
|---------|-----|-------------|
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| **Spark Master UI** | http://localhost:8080 | - |
| **ClickHouse HTTP** | http://localhost:8123 | `default` / `clickhouse123` |
| **Superset** | http://localhost:8088 | `admin` / `admin` |
| **Iceberg REST** | http://localhost:8181 | - |

**Kiểm tra nhanh:**
- MinIO: Mở http://localhost:9001, đăng nhập và kiểm tra bucket `lakehouse` đã được tạo
- Spark: Mở http://localhost:8080, kiểm tra có 1 worker đang chạy
- ClickHouse: Chạy `make clickhouse-client` và test query `SELECT 1`

---

## 6. Chạy Data Pipeline

### 6.1. Chạy Toàn Bộ Pipeline (Khuyến Nghị)

**Cách 1: Sử dụng Make**

```bash
make pipeline-full
```

Lệnh này sẽ chạy tuần tự:
1. ✅ Bronze Layer - Ingestion
2. ✅ Silver Layer - Transformation
3. ✅ Gold Layer - Aggregation
4. ✅ ClickHouse - Create Tables
5. ✅ Sync to ClickHouse

**Cách 2: Sử dụng Script**

```bash
chmod +x scripts/run_pipeline.sh
./scripts/run_pipeline.sh
```

### 6.2. Chạy Từng Bước Riêng Lẻ

Nếu muốn chạy từng bước để kiểm tra:

```bash
# Bước 1: Ingest dữ liệu thô vào Bronze
make ingest-bronze

# Bước 2: Transform Bronze → Silver
make transform-silver

# Bước 3: Aggregate Silver → Gold
make aggregate-gold

# Bước 4: Sync Gold → ClickHouse
make sync-clickhouse
```

### 6.3. Kiểm Tra Tiến Độ

Trong khi pipeline chạy, bạn có thể:

1. **Xem Spark UI**: http://localhost:8080
   - Xem các jobs đang chạy
   - Xem executor status
   - Xem logs của từng task

2. **Xem Logs**:
   ```bash
   make logs-spark
   ```

3. **Kiểm Tra MinIO**:
   - Mở http://localhost:9001
   - Vào bucket `iceberg-warehouse`
   - Kiểm tra các thư mục `bronze/`, `silver/`, `gold/`

### 6.4. Thời Gian Chạy Dự Kiến

Với dataset ~20 triệu events:
- **Bronze Ingestion**: 5-10 phút
- **Silver Transformation**: 10-15 phút
- **Gold Aggregation**: 5-10 phút
- **ClickHouse Sync**: 5-10 phút

**Tổng cộng**: ~30-45 phút (tùy vào hardware)

---

## 7. Thiết Lập Superset Dashboard

### 7.1. Setup Superset

Sau khi pipeline chạy xong, setup Superset để kết nối với ClickHouse:

```bash
# Cài đặt Python dependencies (nếu chưa có)
pip install requests

# Chạy setup script
make setup-superset

# Hoặc chạy trực tiếp
python superset/setup_superset.py
```

Script sẽ:
- ✅ Tạo database connection đến ClickHouse
- ✅ Import dashboard templates (nếu có)
- ✅ Tạo các charts mẫu

### 7.2. Truy Cập Superset

1. Mở http://localhost:8088
2. Đăng nhập: `admin` / `admin`
3. Vào **Settings** → **Database Connections**
4. Kiểm tra connection `ClickHouse Lakehouse` đã được tạo

### 7.3. Tạo Dashboard Thủ Công (Nếu Cần)

Nếu setup script không tự động tạo dashboard, bạn có thể tạo thủ công:

1. **Tạo Dataset**:
   - Vào **Data** → **Datasets**
   - Click **+ Dataset**
   - Chọn database `ClickHouse Lakehouse`
   - Chọn table `daily_sales`, `funnel_analysis`, `customer_rfm`, etc.

2. **Tạo Charts**:
   - Vào **Charts** → **+ Chart**
   - Chọn dataset và tạo các charts:
     - **Line Chart**: Daily Revenue Trend
     - **Pie Chart**: Revenue by Category
     - **Funnel Chart**: Conversion Funnel
     - **Bar Chart**: Customer Segments
     - **Table**: Top Products

3. **Tạo Dashboard**:
   - Vào **Dashboards** → **+ Dashboard**
   - Thêm các charts vào dashboard

---

## 8. Kiểm Tra Kết Quả

### 8.1. Kiểm Tra Iceberg Tables

**Sử dụng Spark SQL:**

```bash
make spark-sql
```

Trong Spark SQL shell:

```sql
-- Liệt kê databases
SHOW DATABASES;

-- Liệt kê tables trong Bronze
SHOW TABLES IN iceberg.bronze;

-- Liệt kê tables trong Silver
SHOW TABLES IN iceberg.silver;

-- Liệt kê tables trong Gold
SHOW TABLES IN iceberg.gold;

-- Query dữ liệu Bronze
SELECT COUNT(*) FROM iceberg.bronze.events_raw;

-- Query dữ liệu Silver
SELECT COUNT(*) FROM iceberg.silver.events_cleaned;

-- Query dữ liệu Gold
SELECT * FROM iceberg.gold.daily_sales_by_category LIMIT 10;
```

### 8.2. Kiểm Tra ClickHouse Tables

**Sử dụng ClickHouse Client:**

```bash
make clickhouse-client
```

Trong ClickHouse client:

```sql
-- Liệt kê databases
SHOW DATABASES;

-- Liệt kê tables
SHOW TABLES FROM lakehouse;

-- Query dữ liệu
SELECT COUNT(*) FROM lakehouse.daily_sales;

-- Top categories by revenue
SELECT 
    category_level1, 
    SUM(total_revenue) as revenue
FROM lakehouse.daily_sales
GROUP BY category_level1
ORDER BY revenue DESC
LIMIT 10;

-- Conversion funnel
SELECT 
    SUM(views) as views,
    SUM(carts) as carts,
    SUM(purchases) as purchases,
    ROUND(SUM(carts) * 100.0 / SUM(views), 2) as view_to_cart_pct,
    ROUND(SUM(purchases) * 100.0 / SUM(carts), 2) as cart_to_purchase_pct
FROM lakehouse.funnel_analysis;
```

### 8.3. Kiểm Tra MinIO Storage

1. Mở http://localhost:9001
2. Đăng nhập: `minioadmin` / `minioadmin123`
3. Vào bucket `iceberg-warehouse`
4. Kiểm tra các thư mục:
   - `bronze/` - chứa Bronze tables
   - `silver/` - chứa Silver tables
   - `gold/` - chứa Gold tables

### 8.4. Kiểm Tra Superset Dashboards

1. Mở http://localhost:8088
2. Vào **Dashboards**
3. Mở dashboard đã tạo
4. Kiểm tra các charts hiển thị đúng dữ liệu

---

## 9. Các Lệnh Thường Dùng

### 9.1. Infrastructure Commands

```bash
# Khởi động services
make up

# Dừng services
make down

# Khởi động lại services
make restart

# Xem logs
make logs

# Xem logs từng service
make logs-spark
make logs-clickhouse
make logs-superset

# Xem trạng thái
make status

# Dọn dẹp (xóa containers, volumes)
make clean
```

### 9.2. Data Pipeline Commands

```bash
# Chạy toàn bộ pipeline
make pipeline-full

# Chạy từng bước
make ingest-bronze        # CSV → Bronze
make transform-silver     # Bronze → Silver
make aggregate-gold       # Silver → Gold
make sync-clickhouse      # Gold → ClickHouse
```

### 9.3. Development Commands

```bash
# Spark Shells
make spark-shell          # Scala shell
make spark-pyspark        # PySpark shell
make spark-sql            # Spark SQL shell

# ClickHouse
make clickhouse-client    # ClickHouse client

# dbt
make dbt-run              # Chạy dbt models
make dbt-test             # Chạy dbt tests
make dbt-docs             # Generate docs
```

### 9.4. Superset Commands

```bash
# Setup Superset
make setup-superset
```

### 9.5. Demo Commands

```bash
# Demo Schema Evolution
make demo-schema-evolution

# Quick start (build + up)
make quickstart

# Full demo (build + up + pipeline + setup)
make demo
```

---

## 10. Troubleshooting

### 10.1. Services Không Khởi Động

**Vấn đề:** Services không start hoặc crash ngay sau khi start.

**Giải pháp:**

```bash
# 1. Kiểm tra logs
make logs

# 2. Kiểm tra tài nguyên
docker stats

# 3. Kiểm tra ports đã bị chiếm
lsof -i :8080  # Spark
lsof -i :8123  # ClickHouse
lsof -i :8088  # Superset
lsof -i :9000  # MinIO

# 4. Dọn dẹp và khởi động lại
make clean
make up
```

### 10.2. Spark Jobs Thất Bại

**Vấn đề:** Spark jobs fail với lỗi OutOfMemory hoặc timeout.

**Giải pháp:**

1. **Tăng memory trong docker-compose.yml:**
   ```yaml
   spark-worker:
     environment:
       SPARK_WORKER_MEMORY: 4g  # Tăng từ 2g lên 4g
   ```

2. **Giảm số partitions:**
   - Chỉnh sửa `spark/jobs/*.py`
   - Thêm: `.coalesce(10)` sau các transformations lớn

3. **Xem logs chi tiết:**
   ```bash
   make logs-spark
   # Hoặc xem Spark UI: http://localhost:8080
   ```

### 10.3. ClickHouse Connection Error

**Vấn đề:** Không kết nối được ClickHouse hoặc query bị lỗi.

**Giải pháp:**

```bash
# 1. Kiểm tra ClickHouse đang chạy
docker ps | grep clickhouse

# 2. Test connection
docker exec clickhouse clickhouse-client \
    --password clickhouse123 \
    -q "SELECT 1"

# 3. Xem logs
make logs-clickhouse

# 4. Khởi động lại
docker-compose restart clickhouse
```

### 10.4. Superset Không Load Dashboard

**Vấn đề:** Superset không hiển thị dữ liệu hoặc connection error.

**Giải pháp:**

1. **Kiểm tra database connection:**
   - Vào http://localhost:8088
   - Settings → Database Connections
   - Test connection `ClickHouse Lakehouse`

2. **Khởi động lại Superset:**
   ```bash
   docker-compose restart superset
   ```

3. **Kiểm tra ClickHouse tables:**
   ```bash
   make clickhouse-client
   # Chạy: SHOW TABLES FROM lakehouse;
   ```

### 10.5. MinIO Connection Error

**Vấn đề:** Spark không kết nối được MinIO.

**Giải pháp:**

```bash
# 1. Kiểm tra MinIO đang chạy
docker ps | grep minio

# 2. Test MinIO từ Spark container
docker exec spark-master curl http://minio:9000/minio/health/live

# 3. Kiểm tra buckets
# Mở http://localhost:9001 và kiểm tra buckets đã được tạo
```

### 10.6. Out of Disk Space

**Vấn đề:** Hết dung lượng disk.

**Giải pháp:**

```bash
# 1. Xem dung lượng đã dùng
docker system df

# 2. Dọn dẹp Docker
docker system prune -a

# 3. Xóa volumes cũ (CẨN THẬN: sẽ mất dữ liệu)
make clean
```

### 10.7. Iceberg REST Catalog Error

**Vấn đề:** Spark không kết nối được Iceberg REST Catalog.

**Giải pháp:**

```bash
# 1. Kiểm tra iceberg-rest đang chạy
docker ps | grep iceberg-rest

# 2. Test REST API
curl http://localhost:8181/v1/config

# 3. Xem logs
docker logs iceberg-rest

# 4. Khởi động lại
docker-compose restart iceberg-rest
```

### 10.8. Dữ Liệu Không Hiển Thị Trong Superset

**Vấn đề:** Tables có dữ liệu nhưng Superset không query được.

**Giải pháp:**

1. **Kiểm tra table permissions trong ClickHouse:**
   ```sql
   -- Trong ClickHouse client
   SELECT * FROM system.tables WHERE database = 'lakehouse';
   ```

2. **Refresh dataset trong Superset:**
   - Vào Data → Datasets
   - Click vào dataset
   - Click "Sync columns from source"

3. **Kiểm tra SQL query:**
   - Tạo chart mới
   - Xem SQL query được generate
   - Test query trực tiếp trong ClickHouse

---

## 11. Tài Liệu Tham Khảo

### 11.1. Tài Liệu Chính Thức

- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [ClickHouse Documentation](https://clickhouse.com/docs/en/)
- [Apache Superset Documentation](https://superset.apache.org/docs/intro)
- [dbt Documentation](https://docs.getdbt.com/)

### 11.2. Tài Liệu Trong Project

- `README.md` - Tổng quan dự án
- `docs/ARCHITECTURE.md` - Kiến trúc chi tiết
- `Makefile` - Danh sách tất cả commands

### 11.3. Dataset

- [Kaggle Dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop)

---

## 12. Liên Hệ & Hỗ Trợ

Nếu gặp vấn đề không giải quyết được:

1. Kiểm tra logs: `make logs`
2. Xem troubleshooting section ở trên
3. Kiểm tra GitHub Issues (nếu có)
4. Liên hệ team qua email: hamic@hus.edu.vn

---

## 📝 Ghi Chú Quan Trọng

1. **Lần đầu chạy:** Build Docker images có thể mất 10-15 phút
2. **Tài nguyên:** Đảm bảo có đủ RAM (khuyến nghị 16GB)
3. **Dữ liệu:** Dataset cần được đặt trong `data/raw/` trước khi chạy pipeline
4. **Thời gian:** Pipeline đầy đủ mất ~30-45 phút tùy hardware
5. **Ports:** Đảm bảo các ports 8080, 8088, 8123, 9000, 9001 không bị chiếm

---

**Chúc bạn thành công với Data Lakehouse! 🚀**
