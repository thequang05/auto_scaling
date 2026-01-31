#!/bin/bash
# =============================================================================
# Kiểm Tra Trạng Thái Services
# =============================================================================

GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m'

echo ""
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║          KIỂM TRA TRẠNG THÁI SERVICES                        ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

cd /Users/koiita/Downloads/auto_scaling/docker

# Kiểm tra từng service
services=("minio" "spark-master" "spark-worker" "spark-thrift" "iceberg-rest" "clickhouse" "superset" "superset-db" "superset-cache")

for service in "${services[@]}"; do
    status=$(docker-compose ps $service 2>/dev/null | grep $service | awk '{print $4}')
    
    if [[ "$status" == *"Up"* ]] && [[ "$status" != *"Exited"* ]]; then
        if [[ "$status" == *"healthy"* ]]; then
            echo -e "${GREEN}✅ $service${NC} - $status"
        elif [[ "$status" == *"unhealthy"* ]]; then
            echo -e "${YELLOW}⚠️  $service${NC} - $status (đang chạy nhưng healthcheck fail)"
        else
            echo -e "${GREEN}✅ $service${NC} - $status"
        fi
    elif [[ "$status" == *"Exited"* ]]; then
        exit_code=$(echo $status | grep -o "Exited ([0-9]*)")
        echo -e "${RED}❌ $service${NC} - $exit_code"
    else
        echo -e "${RED}❌ $service${NC} - Chưa khởi động"
    fi
done

echo ""
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                    TỔNG KẾT                                   ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

# Đếm số services đang chạy
running=$(docker-compose ps 2>/dev/null | grep -c "Up" || echo "0")
total=$(docker-compose ps 2>/dev/null | grep -c "NAME\|Up\|Exited" || echo "0")

echo "Services đang chạy: $running"
echo ""
echo "🔗 URLs:"
echo "  - Spark Master UI:    http://localhost:8080"
echo "  - MinIO Console:      http://localhost:9001"
echo "  - Iceberg REST:       http://localhost:8181"
echo "  - Spark Worker UI:    http://localhost:8081"
echo ""
