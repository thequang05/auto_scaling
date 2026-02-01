# =============================================================================
# dbt Dockerfile for ClickHouse adapter
# =============================================================================
FROM python:3.9-slim

WORKDIR /dbt

# Install dbt with ClickHouse adapter
RUN pip install --no-cache-dir \
    dbt-core==1.7.0 \
    dbt-clickhouse==1.7.0

# Copy dbt project
COPY . /dbt/

# Set environment variables
ENV DBT_PROFILES_DIR=/dbt

# Default command
CMD ["dbt", "debug"]
