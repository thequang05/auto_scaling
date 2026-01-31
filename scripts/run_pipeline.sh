#!/bin/bash
# =============================================================================
# DATA LAKEHOUSE - PIPELINE RUNNER
# =============================================================================
# Script chạy toàn bộ data pipeline
# =============================================================================

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
SPARK_MASTER="spark://spark-master:7077"
START_TIME=$(date +%s)

log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1"
}

step() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

# =============================================================================
# MAIN PIPELINE
# =============================================================================

echo ""
echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║          DATA LAKEHOUSE - PIPELINE EXECUTION                   ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check if services are running
log "Checking services..."
if ! docker ps | grep -q spark-master; then
    error "Spark Master is not running. Please run 'make up' first."
    exit 1
fi

if ! docker ps | grep -q minio; then
    error "MinIO is not running. Please run 'make up' first."
    exit 1
fi

log "All required services are running ✓"

# =============================================================================
# STEP 1: Bronze Layer - Ingestion
# =============================================================================
step "STEP 1: BRONZE LAYER - Data Ingestion"

log "Ingesting raw data to Iceberg Bronze tables..."

docker exec spark-master spark-submit \
    --master ${SPARK_MASTER} \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 2 \
    /opt/spark-apps/jobs/bronze/ingest_events.py

log "Bronze Layer ingestion complete ✓"

# =============================================================================
# STEP 2: Silver Layer - Transformation
# =============================================================================
step "STEP 2: SILVER LAYER - Data Transformation"

log "Cleaning and transforming data..."

docker exec spark-master spark-submit \
    --master ${SPARK_MASTER} \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 2 \
    /opt/spark-apps/jobs/silver/clean_events.py

log "Silver Layer transformation complete ✓"

# =============================================================================
# STEP 3: Gold Layer - Aggregation
# =============================================================================
step "STEP 3: GOLD LAYER - Business Aggregation"

log "Creating aggregated business tables..."

docker exec spark-master spark-submit \
    --master ${SPARK_MASTER} \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 2 \
    /opt/spark-apps/jobs/gold/aggregate_sales.py

log "Gold Layer aggregation complete ✓"

# =============================================================================
# STEP 4: ClickHouse - Serving Layer
# =============================================================================
step "STEP 4: CLICKHOUSE - Create Tables"

log "Creating ClickHouse tables..."

docker exec -i clickhouse clickhouse-client \
    --password clickhouse123 \
    < clickhouse/migrations/001_create_iceberg_tables.sql

log "ClickHouse tables created ✓"

# =============================================================================
# STEP 5: Sync to ClickHouse (Optional)
# =============================================================================
step "STEP 5: SYNC TO CLICKHOUSE (Native Tables)"

log "Syncing Gold layer to native ClickHouse tables..."

docker exec spark-master spark-submit \
    --master ${SPARK_MASTER} \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
    --packages com.github.housepower:clickhouse-spark-runtime-3.4_2.12:0.7.3 \
    /opt/spark-apps/jobs/serving/sync_to_clickhouse.py || {
    log "Note: Native sync skipped (IcebergS3 Engine will be used instead)"
}

log "ClickHouse sync complete ✓"

# =============================================================================
# SUMMARY
# =============================================================================
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo ""
echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║                    PIPELINE COMPLETE!                          ║${NC}"
echo -e "${GREEN}╠═══════════════════════════════════════════════════════════════╣${NC}"
echo -e "${GREEN}║  Duration: ${MINUTES}m ${SECONDS}s                                             ║${NC}"
echo -e "${GREEN}║                                                                ║${NC}"
echo -e "${GREEN}║  Tables Created:                                               ║${NC}"
echo -e "${GREEN}║    Bronze: iceberg.bronze.events_raw                           ║${NC}"
echo -e "${GREEN}║    Silver: iceberg.silver.events_cleaned                       ║${NC}"
echo -e "${GREEN}║            iceberg.silver.dim_products                         ║${NC}"
echo -e "${GREEN}║            iceberg.silver.dim_users                            ║${NC}"
echo -e "${GREEN}║    Gold:   iceberg.gold.daily_sales_by_category                ║${NC}"
echo -e "${GREEN}║            iceberg.gold.funnel_analysis                        ║${NC}"
echo -e "${GREEN}║            iceberg.gold.customer_rfm                           ║${NC}"
echo -e "${GREEN}║            iceberg.gold.product_performance                    ║${NC}"
echo -e "${GREEN}║                                                                ║${NC}"
echo -e "${GREEN}║  Next: Run 'make setup-superset' to configure dashboards       ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""
