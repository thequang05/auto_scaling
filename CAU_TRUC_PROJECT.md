# 📁 Cấu Trúc Project - Data Lakehouse

> **Tài liệu giải thích cấu trúc thư mục và chức năng của từng component**

---

## 📂 Cấu Trúc Tổng Quan

```
auto_scaling/
├── 📁 docker/              # Docker configuration
├── 📁 spark/               # Spark jobs & applications
├── 📁 dbt/                 # dbt models & transformations
├── 📁 clickhouse/          # ClickHouse migrations & queries
├── 📁 superset/            # Superset dashboards & setup
├── 📁 data/                # Raw data files
├── 📁 scripts/             # Utility scripts
├── 📁 docs/                # Documentation
├── 📄 Makefile             # Automation commands
├── 📄 README.md            # Project overview
├── 📄 QUICK_START.md       # Quick start guide
├── 📄 HUONG_DAN_CHAY_PROJECT.md  # Full guide (Vietnamese)
└── 📄 CAU_TRUC_PROJECT.md  # This file
```

---

## 📁 Chi Tiết Từng Thư Mục

### 1. `docker/` - Docker Configuration

Chứa tất cả cấu hình Docker và Docker Compose.

```
docker/
├── docker-compose.yml      # Main orchestration file
├── spark/
│   ├── Dockerfile          # Custom Spark image với Iceberg
│   └── spark-defaults.conf # Spark configuration
├── clickhouse/
│   ├── config.xml          # ClickHouse server config
│   └── users.xml           # ClickHouse users & permissions
└── superset/
    ├── Dockerfile          # Custom Superset image
    └── superset_config.py  # Superset configuration
```

**Chức năng:**
- Định nghĩa tất cả services (MinIO, Spark, ClickHouse, Superset, etc.)
- Cấu hình networking và volumes
- Setup environment variables

---

### 2. `spark/` - Spark Jobs

Chứa tất cả Spark applications theo kiến trúc Medallion.

```
spark/
└── jobs/
    ├── bronze/
    │   ├── ingest_events.py          # CSV → Bronze Layer
    │   ├── ingest_events_test.py     # Test version
    │   ├── schema_evolution_demo.py  # Demo schema evolution
    │   └── time_travel_demo.py       # Demo time travel
    ├── silver/
    │   └── clean_events.py        # Bronze → Silver (cleaning)
    ├── gold/
    │   └── aggregate_sales.py        # Silver → Gold (aggregations)
    └── serving/
        └── sync_to_clickhouse.py     # Gold → ClickHouse
```

**Chức năng:**
- **Bronze**: Ingest raw data từ CSV vào Iceberg
- **Silver**: Clean và transform data
- **Gold**: Tạo business metrics và aggregations
- **Serving**: Sync data vào ClickHouse cho query nhanh

**Cách chạy:**
```bash
make ingest-bronze        # Chạy bronze job
make transform-silver     # Chạy silver job
make aggregate-gold      # Chạy gold job
make sync-clickhouse     # Sync to ClickHouse
```

---

### 3. `dbt/` - Data Build Tool

Chứa dbt models để transform data từ Silver → Gold.

```
dbt/
├── dbt_project.yml       # dbt project configuration
├── profiles.yml          # Connection profiles
├── models/
│   ├── staging/          # Bronze → Silver models
│   │   ├── stg_events.sql
│   │   ├── stg_products.sql
│   │   └── stg_users.sql
│   ├── marts/
│   │   └── core/         # Silver → Gold models
│   │       ├── fct_daily_sales.sql
│   │       ├── fct_funnel.sql
│   │       └── dim_customer_rfm.sql
│   └── schema.yml        # Model documentation
└── macros/
    └── utils.sql         # Reusable macros
```

**Chức năng:**
- Transform data từ Silver layer
- Tạo business metrics (RFM, Funnel, etc.)
- Data quality tests
- Documentation

**Cách chạy:**
```bash
make dbt-run      # Chạy tất cả models
make dbt-test     # Chạy tests
make dbt-docs     # Generate documentation
```

---

### 4. `clickhouse/` - ClickHouse

Chứa migrations và sample queries cho ClickHouse.

```
clickhouse/
├── migrations/
│   └── 001_create_iceberg_tables.sql  # Create tables với IcebergS3 engine
└── queries/
    └── sample_queries.sql              # Sample queries
```

**Chức năng:**
- Tạo tables trong ClickHouse
- Sample queries để test
- Migrations để version control schema

**Cách chạy:**
```bash
make clickhouse-migrate    # Chạy migrations
make clickhouse-client     # Mở ClickHouse client
```

---

### 5. `superset/` - Apache Superset

Chứa dashboards và setup script cho Superset.

```
superset/
├── dashboards/
│   └── dashboard_config.json  # Dashboard configuration
└── setup_superset.py          # Auto-setup script
```

**Chức năng:**
- Setup Superset connection đến ClickHouse
- Import dashboard templates
- Tạo charts và dashboards tự động

**Cách chạy:**
```bash
make setup-superset    # Chạy setup script
```

---

### 6. `data/` - Raw Data

Chứa dữ liệu thô (CSV files) từ Kaggle dataset.

```
data/
└── raw/
    ├── 2019-Oct.csv
    ├── 2019-Nov.csv
    ├── 2019-Dec.csv
    ├── 2020-Jan.csv
    └── 2020-Feb.csv
```

**Chức năng:**
- Lưu trữ raw data files
- Input cho Bronze layer ingestion

**Lưu ý:**
- Files này cần được download từ Kaggle
- Đặt vào `data/raw/` trước khi chạy pipeline

---

### 7. `scripts/` - Utility Scripts

Chứa các scripts tiện ích.

```
scripts/
├── setup.sh           # Initial setup script
├── run_pipeline.sh    # Run full pipeline
└── generate_sample_data.py  # Generate sample data (optional)
```

**Chức năng:**
- `setup.sh`: Kiểm tra môi trường, tạo thư mục, build images
- `run_pipeline.sh`: Chạy toàn bộ pipeline từ Bronze → ClickHouse
- `generate_sample_data.py`: Tạo dữ liệu mẫu (nếu cần)

**Cách chạy:**
```bash
./scripts/setup.sh
./scripts/run_pipeline.sh
```

---

### 8. `docs/` - Documentation

Chứa tài liệu kỹ thuật.

```
docs/
└── ARCHITECTURE.md    # Kiến trúc hệ thống chi tiết
```

**Chức năng:**
- Giải thích kiến trúc hệ thống
- Data flow diagrams
- Technology stack details

---

## 🔑 Các File Quan Trọng

### `Makefile`

File chứa tất cả automation commands.

**Các nhóm lệnh:**
- **Infrastructure**: `make up`, `make down`, `make logs`
- **Pipeline**: `make pipeline-full`, `make ingest-bronze`, etc.
- **Development**: `make spark-shell`, `make clickhouse-client`
- **dbt**: `make dbt-run`, `make dbt-test`

**Xem tất cả lệnh:**
```bash
make help
```

---

### `README.md`

File tổng quan về project.

**Nội dung:**
- Giới thiệu project
- Kiến trúc hệ thống
- Quick start
- Technology stack
- Links đến các tài liệu khác

---

### `HUONG_DAN_CHAY_PROJECT.md`

Tài liệu hướng dẫn đầy đủ bằng tiếng Việt.

**Nội dung:**
- Hướng dẫn setup từng bước
- Chạy pipeline
- Troubleshooting
- Các lệnh thường dùng

---

### `QUICK_START.md`

Hướng dẫn nhanh để bắt đầu.

**Nội dung:**
- 3 bước đơn giản
- Links đến tài liệu đầy đủ

---

## 🔄 Data Flow

```
1. data/raw/*.csv
   ↓
2. spark/jobs/bronze/ingest_events.py
   ↓
3. Iceberg Bronze Tables (MinIO)
   ↓
4. spark/jobs/silver/clean_events.py
   ↓
5. Iceberg Silver Tables (MinIO)
   ↓
6. spark/jobs/gold/aggregate_sales.py
   ↓
7. Iceberg Gold Tables (MinIO)
   ↓
8. spark/jobs/serving/sync_to_clickhouse.py
   ↓
9. ClickHouse Tables
   ↓
10. Superset Dashboards
```

---

## 🛠️ Development Workflow

### 1. Thêm Spark Job Mới

```bash
# Tạo file mới trong spark/jobs/
vim spark/jobs/bronze/my_new_job.py

# Test job
make spark-pyspark
# Hoặc
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/jobs/bronze/my_new_job.py
```

### 2. Thêm dbt Model Mới

```bash
# Tạo file SQL trong dbt/models/
vim dbt/models/marts/core/my_new_model.sql

# Test model
cd dbt
dbt run --select my_new_model
dbt test --select my_new_model
```

### 3. Thêm ClickHouse Table Mới

```bash
# Tạo migration file
vim clickhouse/migrations/002_create_new_table.sql

# Chạy migration
make clickhouse-migrate
```

### 4. Thêm Superset Dashboard

```bash
# Tạo dashboard config
vim superset/dashboards/my_dashboard.json

# Import trong Superset UI hoặc qua API
python superset/setup_superset.py
```

---

## 📊 Các Bảng Dữ Liệu

### Bronze Layer

| Table | Description | Location |
|-------|-------------|----------|
| `iceberg.bronze.events_raw` | Raw events từ CSV | MinIO: `s3://iceberg-warehouse/bronze/` |

### Silver Layer

| Table | Description | Location |
|-------|-------------|----------|
| `iceberg.silver.events_cleaned` | Events đã làm sạch | MinIO: `s3://iceberg-warehouse/silver/` |
| `iceberg.silver.dim_products` | Product dimension | MinIO: `s3://iceberg-warehouse/silver/` |
| `iceberg.silver.dim_users` | User dimension | MinIO: `s3://iceberg-warehouse/silver/` |

### Gold Layer

| Table | Description | Location |
|-------|-------------|----------|
| `iceberg.gold.daily_sales_by_category` | Daily sales | MinIO: `s3://iceberg-warehouse/gold/` |
| `iceberg.gold.funnel_analysis` | Conversion funnel | MinIO: `s3://iceberg-warehouse/gold/` |
| `iceberg.gold.customer_rfm` | RFM segmentation | MinIO: `s3://iceberg-warehouse/gold/` |
| `iceberg.gold.product_performance` | Product metrics | MinIO: `s3://iceberg-warehouse/gold/` |

### ClickHouse Tables

| Table | Description | Engine |
|-------|-------------|--------|
| `lakehouse.daily_sales` | Daily sales | IcebergS3 |
| `lakehouse.funnel_analysis` | Funnel metrics | IcebergS3 |
| `lakehouse.customer_rfm` | Customer segments | IcebergS3 |
| `lakehouse.product_performance` | Product metrics | IcebergS3 |

---

## 🔍 Tìm Kiếm File

### Tìm Spark Jobs

```bash
find spark/jobs -name "*.py"
```

### Tìm dbt Models

```bash
find dbt/models -name "*.sql"
```

### Tìm Configuration Files

```bash
find . -name "*.yml" -o -name "*.yaml"
find . -name "*.conf" -o -name "*.xml"
```

---

## 📝 Best Practices

1. **Naming Convention:**
   - Spark jobs: `snake_case.py`
   - dbt models: `snake_case.sql`
   - Tables: `snake_case` hoặc `camelCase`

2. **File Organization:**
   - Mỗi layer (bronze/silver/gold) trong thư mục riêng
   - Related files nên ở gần nhau

3. **Documentation:**
   - Comment code rõ ràng
   - Update README khi thêm feature mới
   - Document schema changes

4. **Version Control:**
   - Commit thường xuyên
   - Use meaningful commit messages
   - Tag releases

---

**Happy Coding! 🚀**
