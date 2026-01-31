# ⚡ QUICK START - Data Lakehouse

> **Hướng dẫn nhanh để chạy project trong 5 phút**

---

## 🚀 3 Bước Đơn Giản

### Bước 1: Chuẩn Bị Dữ Liệu

```bash
# Tạo thư mục và đặt CSV files vào
mkdir -p data/raw
# Copy các file CSV từ Kaggle dataset vào data/raw/
```

**Dataset:** [eCommerce Events History in Cosmetics Shop](https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop)

### Bước 2: Khởi Động Hệ Thống

```bash
# Build và khởi động tất cả services
make up

# Đợi 1-2 phút để services khởi động hoàn tất
```

### Bước 3: Chạy Pipeline

```bash
# Chạy toàn bộ data pipeline
make pipeline-full

# Setup Superset dashboards
make setup-superset
```

---

## ✅ Kiểm Tra Kết Quả

| Service | URL | Login |
|---------|-----|-------|
| **Superset** | http://localhost:8088 | `admin` / `admin` |
| **Spark UI** | http://localhost:8080 | - |
| **MinIO** | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| **ClickHouse** | http://localhost:8123 | `default` / `clickhouse123` |

---

## 📚 Tài Liệu Đầy Đủ

Xem file **[HUONG_DAN_CHAY_PROJECT.md](./HUONG_DAN_CHAY_PROJECT.md)** để biết:
- Hướng dẫn chi tiết từng bước
- Troubleshooting
- Các lệnh nâng cao
- Kiến trúc hệ thống

---

## 🆘 Gặp Vấn Đề?

```bash
# Xem logs
make logs

# Kiểm tra trạng thái
make status

# Khởi động lại
make restart
```

Xem section **Troubleshooting** trong file hướng dẫn đầy đủ.

---

**Happy Data Engineering! 🎉**
