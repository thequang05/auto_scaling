#!/bin/bash
# =============================================================================
# Setup ClickHouse Iceberg Tables (Zero-Copy from MinIO)
# =============================================================================
# Usage: ./setup_clickhouse_iceberg.sh
# =============================================================================

set -e

echo "=============================================="
echo "Setting up ClickHouse Iceberg Tables"
echo "=============================================="

# Wait for ClickHouse to be ready
echo "Waiting for ClickHouse..."
until docker exec clickhouse clickhouse-client --password clickhouse123 -q "SELECT 1" > /dev/null 2>&1; do
    sleep 2
    echo "  Still waiting..."
done
echo "✓ ClickHouse is ready"

# Create database
echo ""
echo "Creating database..."
docker exec clickhouse clickhouse-client --password clickhouse123 -q \
    "CREATE DATABASE IF NOT EXISTS lakehouse"
echo "✓ Database 'lakehouse' created"

# Create Iceberg tables (Zero-Copy from MinIO)
echo ""
echo "Creating Iceberg tables..."

docker exec clickhouse clickhouse-client --password clickhouse123 -q "
DROP TABLE IF EXISTS lakehouse.daily_sales
"
docker exec clickhouse clickhouse-client --password clickhouse123 -q "
CREATE TABLE lakehouse.daily_sales
ENGINE = Iceberg('http://minio:9000/iceberg-warehouse/gold/daily_sales_by_category/', 'minioadmin', 'minioadmin123')
"
echo "✓ Created: lakehouse.daily_sales"

docker exec clickhouse clickhouse-client --password clickhouse123 -q "
DROP TABLE IF EXISTS lakehouse.funnel_analysis
"
docker exec clickhouse clickhouse-client --password clickhouse123 -q "
CREATE TABLE lakehouse.funnel_analysis
ENGINE = Iceberg('http://minio:9000/iceberg-warehouse/gold/funnel_analysis/', 'minioadmin', 'minioadmin123')
"
echo "✓ Created: lakehouse.funnel_analysis"

docker exec clickhouse clickhouse-client --password clickhouse123 -q "
DROP TABLE IF EXISTS lakehouse.customer_rfm
"
docker exec clickhouse clickhouse-client --password clickhouse123 -q "
CREATE TABLE lakehouse.customer_rfm
ENGINE = Iceberg('http://minio:9000/iceberg-warehouse/gold/customer_rfm/', 'minioadmin', 'minioadmin123')
"
echo "✓ Created: lakehouse.customer_rfm"

docker exec clickhouse clickhouse-client --password clickhouse123 -q "
DROP TABLE IF EXISTS lakehouse.product_performance
"
docker exec clickhouse clickhouse-client --password clickhouse123 -q "
CREATE TABLE lakehouse.product_performance
ENGINE = Iceberg('http://minio:9000/iceberg-warehouse/gold/product_performance/', 'minioadmin', 'minioadmin123')
"
echo "✓ Created: lakehouse.product_performance"

# Verify tables
echo ""
echo "=============================================="
echo "Verification - Row counts:"
echo "=============================================="
docker exec clickhouse clickhouse-client --password clickhouse123 -q "
SELECT 'daily_sales' as table_name, count(*) as rows FROM lakehouse.daily_sales
UNION ALL
SELECT 'funnel_analysis', count(*) FROM lakehouse.funnel_analysis
UNION ALL
SELECT 'customer_rfm', count(*) FROM lakehouse.customer_rfm
UNION ALL
SELECT 'product_performance', count(*) FROM lakehouse.product_performance
FORMAT PrettyCompact
"

echo ""
echo "=============================================="
echo "✅ ClickHouse Iceberg Setup Complete!"
echo "=============================================="
echo ""
echo "You can now query data directly from Iceberg/MinIO:"
echo "  docker exec -it clickhouse clickhouse-client --password clickhouse123"
echo ""
echo "Example queries:"
echo "  SELECT * FROM lakehouse.daily_sales LIMIT 10;"
echo "  SELECT category_level1, SUM(total_revenue) FROM lakehouse.daily_sales GROUP BY 1;"
