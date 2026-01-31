# 🏛️ KIẾN TRÚC HỆ THỐNG DATA LAKEHOUSE

## 📋 Tổng Quan

Hệ thống Data Lakehouse được xây dựng theo **Kiến trúc Medallion** với 3 tầng dữ liệu:
- **Bronze Layer**: Dữ liệu thô (Raw Data)
- **Silver Layer**: Dữ liệu đã làm sạch (Cleaned Data)
- **Gold Layer**: Dữ liệu nghiệp vụ (Business-Ready Data)

## 🎯 Use Case: E-commerce Event History

**Dataset**: eCommerce Events History in Cosmetics Shop (Kaggle)
- ~20 triệu events
- Dữ liệu hành vi người dùng: view, cart, purchase
- Thời gian: Oct 2019 - Apr 2020

## 🔧 Stack Công Nghệ

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
│                     [IcebergS3 Engine]                               │
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
│                                                                      │
│     s3://lakehouse/bronze/    s3://lakehouse/silver/                │
│     s3://lakehouse/gold/      s3://lakehouse/warehouse/             │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       CATALOG SERVICE                                │
│                    Iceberg REST Catalog                              │
│              (Nessie / Polaris / Custom REST)                        │
└─────────────────────────────────────────────────────────────────────┘
```

## 📁 Cấu Trúc Thư Mục

```
lakehouse-project/
├── docker/
│   ├── docker-compose.yml          # Main orchestration
│   ├── spark/
│   │   └── Dockerfile              # Custom Spark image
│   ├── superset/
│   │   └── superset_config.py      # Superset configuration
│   └── clickhouse/
│       └── config.xml              # ClickHouse configuration
├── spark/
│   ├── jobs/
│   │   ├── bronze/
│   │   │   └── ingest_events.py    # Raw data ingestion
│   │   ├── silver/
│   │   │   └── clean_events.py     # Data cleaning
│   │   └── gold/
│   │       └── aggregate_sales.py  # Business aggregations
│   └── conf/
│       └── spark-defaults.conf
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/                # Bronze → Silver
│   │   ├── marts/                  # Silver → Gold
│   │   └── schema.yml
│   └── tests/
├── clickhouse/
│   ├── migrations/
│   │   └── 001_create_tables.sql
│   └── queries/
├── superset/
│   └── dashboards/
├── data/
│   └── raw/                        # Source CSV files
├── notebooks/
│   └── exploration.ipynb
├── scripts/
│   ├── setup.sh                    # Initial setup
│   ├── ingest.sh                   # Run ingestion
│   └── transform.sh                # Run transformations
├── README.md
└── Makefile                        # Automation commands
```

## 🔄 Data Flow Chi Tiết

### Step 1: Ingestion (Bronze Layer)
```
CSV Files → Spark → Iceberg Tables (Bronze) → MinIO
                         │
                         └── Metadata: _ingestion_time, _source_file
```

### Step 2: Transformation (Silver Layer)
```
Bronze Tables → dbt/Spark → Silver Tables
                    │
                    └── Cleaning: Deduplication, Type casting, Null handling
```

### Step 3: Aggregation (Gold Layer)
```
Silver Tables → dbt/Spark → Gold Tables
                    │
                    └── Metrics: daily_sales, customer_segments, funnel_analysis
```

### Step 4: Serving (ClickHouse)
```
Gold Tables (Iceberg/MinIO) → ClickHouse (IcebergS3 Engine)
                                      │
                                      └── Zero-Copy Architecture
```

### Step 5: Visualization (Superset)
```
ClickHouse → Superset → Dashboards
                 │
                 └── KPIs: Revenue, Conversion Rate, Customer Segmentation
```

## 📊 Bảng Dữ Liệu

### Bronze Layer
| Table Name | Description | Partitioning |
|------------|-------------|--------------|
| `events_raw` | Raw event data | `event_date` (day) |

### Silver Layer
| Table Name | Description | Partitioning |
|------------|-------------|--------------|
| `events_cleaned` | Cleaned events | `event_date`, `event_type` |
| `products` | Product dimension | None |
| `users` | User dimension | None |

### Gold Layer
| Table Name | Description | Partitioning |
|------------|-------------|--------------|
| `daily_sales` | Daily sales by category | `sale_date` |
| `funnel_analysis` | Conversion funnel | `analysis_date` |
| `customer_rfm` | RFM segmentation | `segment_date` |

## 🔐 Cấu Hình Kết Nối

### MinIO
```
Endpoint: http://minio:9000
Access Key: minioadmin
Secret Key: minioadmin123
Bucket: lakehouse
```

### Iceberg REST Catalog
```
URI: http://iceberg-rest:8181
Warehouse: s3://lakehouse/warehouse
```

### ClickHouse
```
Host: clickhouse
Port: 8123 (HTTP), 9000 (Native)
Database: lakehouse
```

### Superset
```
URL: http://localhost:8088
Admin: admin / admin
```

## ⚡ Tối Ưu Hiệu Năng

### Iceberg Optimizations
- **Partitioning**: Theo `event_date` (daily partitions)
- **Z-Ordering**: Theo `user_id`, `product_id` cho truy vấn nhanh
- **Compaction**: Merge small files định kỳ

### ClickHouse Optimizations
- **Primary Key**: `(event_date, user_id, event_type)`
- **Skip Indices**: Bloom filter trên `product_id`
- **Materialized Views**: Pre-aggregated metrics

## 🕐 Schema Evolution Demo

Để demo tính năng Schema Evolution của Iceberg:

1. **Ngày T**: Dữ liệu ban đầu (không có `payment_method`)
2. **Ngày T+1**: Thêm cột `payment_method` vào schema
3. **Iceberg**: Tự động xử lý, không cần rewrite data

```python
# Thêm cột mới
spark.sql("""
    ALTER TABLE bronze.events_raw 
    ADD COLUMN payment_method STRING
""")
```

## 🔄 Time Travel Demo

```sql
-- Query data as of 2 days ago
SELECT * FROM bronze.events_raw 
VERSION AS OF 123456789;

-- Query data at specific timestamp
SELECT * FROM bronze.events_raw 
TIMESTAMP AS OF '2024-01-15 10:00:00';
```
