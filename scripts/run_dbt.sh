#!/bin/bash
# =============================================================================
# Run dbt with ClickHouse adapter
# =============================================================================
# Usage: ./run_dbt.sh [command]
# Examples:
#   ./run_dbt.sh debug     - Test connection
#   ./run_dbt.sh run       - Run all models
#   ./run_dbt.sh test      - Run tests
#   ./run_dbt.sh docs      - Generate docs
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DBT_DIR="$(dirname "$SCRIPT_DIR")/dbt"

# Default command
CMD=${1:-debug}

echo "=============================================="
echo "Running dbt $CMD"
echo "=============================================="

# Run dbt in Docker
docker run --rm \
    --network lakehouse-network \
    -v "$DBT_DIR:/dbt" \
    -e DBT_PROFILES_DIR=/dbt \
    -w /dbt \
    python:3.9-slim \
    bash -c "
        pip install --quiet dbt-core dbt-clickhouse && \
        dbt $CMD --profiles-dir /dbt
    "

echo ""
echo "=============================================="
echo "✅ dbt $CMD completed!"
echo "=============================================="
