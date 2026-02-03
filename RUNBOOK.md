# Data Lakehouse - Complete Runbook

## Tổng Quan

Đây là hướng dẫn chạy toàn bộ Data Lakehouse project trên máy **8GB RAM**, được tối ưu qua cuộc trò chuyện này.

**Kiến trúc:**
```
CSV Data → Bronze (Iceberg) → Silver (Iceberg) → Gold (Iceberg)
                                                       ↓
                                              ClickHouse (Zero-Copy)
                                                       ↓
                                              dbt Models → Superset Dashboard
```

---

## Yêu Cầu Hệ Thống

- **RAM**: 8GB minimum
- **Docker**: Desktop hoặc Engine
- **Disk**: 10GB available space
- **OS**: macOS, Linux, hoặc Windows với WSL2

---

## BƯỚC 0: Initial Setup (Tùy chọn)

### 0.1 Chạy Setup Script

Script `setup.sh` sẽ tự động:
- Kiểm tra Docker và Docker Compose
- Tạo các thư mục cần thiết (`data/raw`, `notebooks`, `logs`)
- Kiểm tra disk space
- Build Docker images

```bash
cd /Users/koiita/Downloads/auto_scaling
chmod +x scripts/setup.sh
./scripts/setup.sh
```

**Lưu ý:** Nếu bạn đã có data và đã build images, có thể bỏ qua bước này.

---

## BƯỚC 1: Khởi Động Hạ Tầng

### 1.1 Start Docker Services

```bash
cd /Users/koiita/Downloads/auto_scaling

# Start lightweight version (tối ưu cho 8GB RAM)
docker compose up -d minio iceberg-rest clickhouse superset

# Verify services
docker ps
```

**Services chạy:**
- **MinIO** (S3-compatible storage): `http://localhost:9001`
- **Iceberg REST Catalog**: Port 8181
- **ClickHouse** (Analytics DB): `http://localhost:8123`
- **Superset** (Visualization): `http://localhost:8088`

### 1.2 Verify Services Health

```bash
./scripts/check_services.sh
```

---

## BƯỚC 2: Chạy Data Pipeline (Spark)

### 2.1 Start Spark (khi cần)

**LƯU Ý:** Vì RAM hạn chế, chạy Spark RIÊNG BIỆT với ClickHouse/Superset.

```bash
# Stop ClickHouse & Superset để giải phóng RAM
docker compose stop clickhouse superset

# Start Spark
docker compose up -d spark-master spark-worker

# Wait for Spark ready
sleep 10
```

### 2.2 Run Full Pipeline

```bash
# Chạy toàn bộ Bronze → Silver → Gold
./scripts/run_pipeline.sh

# Hoặc chạy từng bước:
# Bronze layer (Ingestion)
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/bronze/ingest_events.py

# Silver layer (Cleaning)
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/silver/clean_events.py

# Gold layer (Aggregation)
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/gold/aggregate_sales.py
```

**Output:**
- Bronze: `s3://iceberg-warehouse/bronze/events_raw/`
- Silver: `s3://iceberg-warehouse/silver/events_cleaned/`
- Gold: `s3://iceberg-warehouse/gold/daily_sales_by_category/` (+ 3 other tables)

### 2.3 Stop Spark, Restart ClickHouse

```bash
# Stop Spark để giải phóng RAM
docker compose stop spark-master spark-worker

# Restart ClickHouse + Superset
docker compose up -d clickhouse superset
```

---

## BƯỚC 3: Setup ClickHouse Iceberg Tables (Zero-Copy)

### 3.1 Create Iceberg Tables in ClickHouse

```bash
./scripts/setup_clickhouse_iceberg.sh
```

**Thực hiện:**
- Tạo database `lakehouse`
- Tạo 4 bảng Iceberg (đọc trực tiếp từ MinIO):
  - `lakehouse.daily_sales` (1,105 rows)
  - `lakehouse.funnel_analysis` (152 rows)
  - `lakehouse.customer_rfm` (110,518 rows)
  - `lakehouse.product_performance` (66,852 rows)

### 3.2 Verify Data

```bash
docker exec clickhouse clickhouse-client --password clickhouse123 -q "
SELECT 'daily_sales' as table_name, count(*) as rows FROM lakehouse.daily_sales
UNION ALL
SELECT 'customer_rfm', count(*) FROM lakehouse.customer_rfm
"
```

**Kết quả mong đợi:**
```
daily_sales      1105
customer_rfm     110518
...
```

---

## BƯỚC 4: Setup dbt Models

### 4.1 Run dbt

```bash
# Chạy dbt models (tạo views trên ClickHouse)
./scripts/run_dbt.sh run

# Run tests
./scripts/run_dbt.sh test

# Generate docs
./scripts/run_dbt.sh docs generate
```

**Models được tạo:**
- `lakehouse_silver.stg_daily_sales` (1,105 rows)
- `lakehouse_silver.stg_customer_rfm` (110,518 rows)
- `lakehouse_silver.stg_funnel_analysis` (152 rows)

### 4.2 Verify dbt Models

```bash
docker exec clickhouse clickhouse-client --password clickhouse123 -q "
SELECT table_name 
FROM system.tables 
WHERE database = 'lakehouse_silver'
"
```

---

## BƯỚC 5: Superset Dashboard

### 5.1 Login to Superset

1. Mở browser: `http://localhost:8088`
2. Login với:
   - **Username**: `admin`
   - **Password**: `admin`

### 5.2 Thêm ClickHouse Connection

**Settings → Database Connections → + Database**

**Connection Details:**
```
Database: ClickHouse Connect (Superset)
SQLAlchemy URI: clickhouse+http://default:clickhouse123@clickhouse:8123/lakehouse
```

**Test Connection** → **Connect**

### 5.3 Add Datasets

**Data → Datasets → + Dataset**

Thêm 3 datasets:
1. `lakehouse.daily_sales`
2. `lakehouse.customer_rfm`
3. `lakehouse.funnel_analysis`

### 5.4 Create Charts

**Charts → + Chart**

**Chart 1: Revenue by Category**
- Dataset: `daily_sales`
- Chart Type: **Bar Chart**
- X-axis: `category_level1`
- Metric: `SUM(total_revenue)`

**Chart 2: Customer Segments**
- Dataset: `customer_rfm`
- Chart Type: **Pie Chart**
- Dimension: `customer_segment`
- Metric: `COUNT(*)`

**Chart 3: Conversion Funnel**
- Dataset: `funnel_analysis`
- Chart Type: **Line Chart**
- X-axis: `event_date`
- Metrics: `views`, `carts`, `purchases`

**Save to Dashboard**: "Data Lakehouse KPIs"

---

## BƯỚC 6: Demo Features

### 6.1 Schema Evolution Demo

```bash
./scripts/run_p4_p5_demo.sh
```

**Demonstrates:**
- P4: Schema Evolution (thêm column không cần rewrite data)
- P5: Time Travel (query historical snapshots)

**Hoặc xem documentation:**
- `docs/SCHEMA_EVOLUTION_DEMO.md`
- `docs/TIME_TRAVEL_DEMO.md`

### 6.2 Manual Schema Evolution (với Spark)

```bash
# Start Spark
docker compose up -d spark-master spark-worker

# Chạy Spark SQL
docker exec -it spark-master spark-sql

# Add column
ALTER TABLE iceberg.gold.daily_sales_by_category
ADD COLUMN sales_channel STRING
COMMENT 'Sales channel: online, retail, mobile';

# Verify
DESCRIBE iceberg.gold.daily_sales_by_category;
```

---

## Kiểm Tra Tổng Thể

### Services Status

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

### Memory Usage

```bash
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}"
```

**Expected RAM (~1GB total):**
- ClickHouse: ~400-500MB
- Superset: ~300-400MB
- MinIO: ~150-200MB
- Iceberg REST: ~100MB

### Data Verification

```bash
# ClickHouse
docker exec clickhouse clickhouse-client --password clickhouse123 -q "
SELECT database, table, total_rows 
FROM system.tables 
WHERE database IN ('lakehouse', 'lakehouse_silver')
FORMAT PrettyCompact
"

# MinIO (S3)
docker exec minio mc ls -r local/iceberg-warehouse/
```

---

## Troubleshooting

### Issue: Out of Memory

**Solution:**
```bash
# Stop Spark before running ClickHouse
docker compose stop spark-master spark-worker

# Restart services
docker compose restart clickhouse superset
```

### Issue: ClickHouse Connection Failed

**Check password:**
```bash
docker exec clickhouse clickhouse-client --password clickhouse123 -q "SELECT 1"
```

**Expected:** `1`

### Issue: Superset Loading Slow

**This is normal.** Superset on lightweight config takes 30-60s to start.

```bash
# Check logs
docker logs superset -f
```

### Issue: dbt Connection Error

**Verify ClickHouse is running:**
```bash
docker ps --filter "name=clickhouse"
```

**Re-run debug:**
```bash
./scripts/run_dbt.sh debug
```

---

## Cleanup

### Stop All Services

```bash
docker compose down
```

### Remove Data (Start Fresh)

```bash
# WARNING: Deletes all data
docker compose down -v

# Remove MinIO data
rm -rf data/minio/*

# Restart
docker compose up -d
```

---

## Workflow Summary

**Development Workflow (8GB RAM):**

1. **Data Processing**: Start Spark → Run pipeline → Stop Spark
2. **Analytics**: Start ClickHouse → Setup Iceberg tables → Run dbt
3. **Visualization**: Keep ClickHouse + Superset running → Create dashboards

**Production Workflow (more RAM):**
- All services can run concurrently
- Use `docker-compose.yml` instead of `light` version

---

## Key Files Reference

### Configuration
- `docker/docker-compose.yml` - Lightweight services config (default)
- `dbt/profiles.yml` - dbt ClickHouse connection
- `docker/superset/superset_config.py` - Superset SQLite config

### Scripts
- `scripts/run_pipeline.sh` - Full Spark pipeline
- `scripts/setup_clickhouse_iceberg.sh` - ClickHouse Iceberg setup
- `scripts/run_dbt.sh` - dbt wrapper
- `scripts/run_p4_p5_demo.sh` - P4+P5 demos

### Documentation
- `docs/SCHEMA_EVOLUTION_DEMO.md` - P4 guide
- `docs/TIME_TRAVEL_DEMO.md` - P5 guide
- `README.md` - Project overview
- `HUONG_DAN_CHAY_PROJECT.md` - Original Vietnamese guide

### Spark Jobs
- `spark/jobs/bronze/ingest_events.py` - CSV → Bronze
- `spark/jobs/silver/clean_events.py` - Bronze → Silver
- `spark/jobs/gold/aggregate_sales.py` - Silver → Gold

### dbt Models
- `dbt/models/staging/stg_daily_sales.sql`
- `dbt/models/staging/stg_customer_rfm.sql`
- `dbt/models/staging/stg_funnel_analysis.sql`

---

## Quick Start (TL;DR)

```bash
# 1. Start services
cd /Users/koiita/Downloads/auto_scaling
docker compose up -d

# 2. Run pipeline (nếu chưa có data)
docker compose stop clickhouse superset
docker compose up -d spark-master spark-worker
./scripts/run_pipeline.sh
docker compose stop spark-master spark-worker

# 3. Setup ClickHouse + dbt
docker compose up -d clickhouse superset
./scripts/setup_clickhouse_iceberg.sh
./scripts/run_dbt.sh run

# 4. Access Superset
open http://localhost:8088
# Login: admin/admin
# Create connection to ClickHouse
# Add datasets and create charts

# 5. Demo features
./scripts/run_p4_p5_demo.sh
```

---

## Conclusion

Project này demonstrate:
- **Modern Data Lakehouse** với Iceberg
- **Zero-Copy Architecture** (ClickHouse đọc trực tiếp từ S3)
- **Resource Optimization** (chạy ổn định trên 8GB RAM)
- **dbt Integration** (transformation layer)
- **Superset Visualization** (lightweight SQLite mode)
- **Advanced Features**: Schema Evolution, Time Travel

**Total Setup Time**: ~10 minutes  
**Total RAM Usage**: ~1GB (khi không chạy Spark)

Đã tối ưu thành công cho máy local development!
