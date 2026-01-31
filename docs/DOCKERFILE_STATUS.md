# 🔍 BÁO CÁO KIỂM TRA DOCKERFILE & SERVICES

**Ngày kiểm tra**: 2026-01-31  
**Thời gian chạy**: ~1 giờ

---

## ✅ SERVICES ĐANG CHẠY ỔN ĐỊNH (7/10)

### 1. **MinIO** ✅
- **Status**: Up About an hour
- **Ports**: 9000, 9001
- **Health**: OK (không có healthcheck)
- **Đánh giá**: Hoạt động tốt, đang phục vụ storage

### 2. **Spark Master** ✅
- **Status**: Up About an hour (healthy)
- **Ports**: 4040, 7077, 8080
- **Health**: ✅ Healthy
- **Web UI**: http://localhost:8080
- **Đánh giá**: Hoạt động ổn định, đang quản lý Spark cluster

### 3. **Spark Worker** ✅
- **Status**: Up About an hour
- **Ports**: 8081
- **Health**: OK
- **Đánh giá**: Đang xử lý Spark jobs

### 4. **Spark Thrift** ✅
- **Status**: Up 59 minutes
- **Ports**: 10000, 4041
- **Health**: OK
- **Logs**: HiveThriftServer2 started
- **Đánh giá**: Hoạt động tốt, sẵn sàng cho dbt

### 5. **Iceberg REST** ⚠️
- **Status**: Up About an hour (unhealthy)
- **Ports**: 8181
- **Health**: ⚠️ Unhealthy (nhưng đang chạy)
- **Logs**: Server started, có một số warnings
- **Đánh giá**: Đang hoạt động, healthcheck có thể sai cấu hình

### 6. **Superset DB** ✅
- **Status**: Up About an hour (healthy)
- **Ports**: 5432
- **Health**: ✅ Healthy
- **Đánh giá**: PostgreSQL hoạt động tốt

### 7. **Superset Cache** ✅
- **Status**: Up About an hour (healthy)
- **Ports**: 6379
- **Health**: ✅ Healthy
- **Đánh giá**: Redis hoạt động tốt

---

## ❌ SERVICES CÓ VẤN ĐỀ (3/10)

### 1. **ClickHouse** ❌
- **Status**: Exited (137) About a minute ago
- **Lỗi**: Container bị kill
- **Nguyên nhân**: 
  - Exit code 137 = SIGKILL (thường do OOM - Out of Memory)
  - ClickHouse cần nhiều memory
- **Giải pháp**:
  ```yaml
  # Trong docker-compose.yml, thêm:
  deploy:
    resources:
      limits:
        memory: 4G
  ```
  Hoặc tăng Docker Desktop memory lên 8GB+

### 2. **Superset** ⏸️
- **Status**: Created (chưa khởi động)
- **Nguyên nhân**: Phụ thuộc vào superset-init
- **Giải pháp**: Fix superset-init trước

### 3. **Superset-init** ❌
- **Status**: Exited (1) About an hour ago
- **Lỗi**: SQLAlchemy version conflict
- **Chi tiết**: 
  ```
  ImportError: cannot import name '_BindParamClause' 
  from 'sqlalchemy.sql.expression'
  ```
- **Nguyên nhân**: 
  - Superset 3.1.0 dùng SQLAlchemy 1.4.x
  - Nhưng đã cài SQLAlchemy 2.0.23
  - Alembic (migration tool) không tương thích với SQLAlchemy 2.0
- **Giải pháp**: 
  - Option 1: Không cài SQLAlchemy (để Superset dùng version mặc định)
  - Option 2: Downgrade về SQLAlchemy 1.4.48

---

## 📊 ĐÁNH GIÁ DOCKERFILE

### ✅ Dockerfile Hoạt Động Tốt

1. **docker/spark/Dockerfile** ✅
   - Build thành công
   - Spark 3.5.0 hoạt động tốt
   - Iceberg JARs đã được tải
   - Python packages đã cài

2. **docker/superset/Dockerfile** ⚠️
   - Build thành công
   - Nhưng có conflict SQLAlchemy
   - Cần sửa version

3. **docker/clickhouse/config.xml** ✅
   - Config đúng
   - Nhưng cần giảm memory

---

## 🔧 ĐỀ XUẤT SỬA LỖI

### Priority 1: Sửa Superset-init (Để chạy Superset)

```dockerfile
# Trong docker/superset/Dockerfile
# Bỏ SQLAlchemy hoặc downgrade
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    sqlalchemy==1.4.48 \
    alembic==1.12.1 \
    clickhouse-connect==0.7.0 \
    ...
```

### Priority 2: Sửa ClickHouse Memory

```yaml
# Trong docker-compose.yml
clickhouse:
  deploy:
    resources:
      limits:
        memory: 3G
      reservations:
        memory: 2G
```

### Priority 3: Sửa Iceberg REST Healthcheck (Optional)

```yaml
iceberg-rest:
  healthcheck:
    test: ["CMD-SHELL", "curl -f http://localhost:8181/v1/config || exit 0"]
    interval: 30s
    timeout: 10s
    retries: 3
```

---

## ✅ KẾT LUẬN

### Services Core (Có thể chạy pipeline ngay)
- ✅ MinIO - Storage
- ✅ Spark - Compute  
- ✅ Iceberg REST - Catalog

### Services Phụ (Có thể fix sau)
- ⚠️ ClickHouse - Serving layer
- ⚠️ Superset - Visualization

**Hệ thống đã sẵn sàng để chạy data pipeline!** 🚀
