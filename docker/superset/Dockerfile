# =============================================================================
# Apache Superset with ClickHouse Driver
# =============================================================================

FROM apache/superset:3.1.0

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
# Note: Không cài SQLAlchemy để Superset dùng version mặc định (tương thích với Alembic)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    clickhouse-connect==0.7.0 \
    clickhouse-driver==0.2.6 \
    "redis>=4.5.4,<5.0" \
    psycopg2-binary==2.9.9

# Create directories
RUN mkdir -p /app/pythonpath /app/superset_home

# Set permissions
RUN chown -R superset:superset /app/pythonpath /app/superset_home

USER superset

ENV PYTHONPATH=/app/pythonpath:$PYTHONPATH
ENV SUPERSET_HOME=/app/superset_home

WORKDIR /app

EXPOSE 8088

CMD ["gunicorn", "--bind", "0.0.0.0:8088", "--workers", "2", "--worker-class", "gthread", "--threads", "20", "--timeout", "120", "superset.app:create_app()"]
