"""
=============================================================================
Apache Superset Configuration for Data Lakehouse
=============================================================================
"""

import os
from datetime import timedelta

# =============================================================================
# General Configuration
# =============================================================================
ROW_LIMIT = 5000
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "supersecretkey123456789")

# Flask App Config
APP_NAME = "Data Lakehouse Dashboard"
APP_ICON = "/static/assets/images/superset-logo-horiz.png"

# =============================================================================
# Database Configuration
# =============================================================================
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "DATABASE_URL",
    "postgresql://superset:superset123@superset-db:5432/superset"
)
SQLALCHEMY_TRACK_MODIFICATIONS = False

# =============================================================================
# Cache Configuration (Redis)
# =============================================================================
REDIS_URL = os.environ.get("REDIS_URL", "redis://superset-cache:6379/0")

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_URL": REDIS_URL,
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,  # 24 hours
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_URL": REDIS_URL,
}

FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "superset_filter_",
    "CACHE_REDIS_URL": REDIS_URL,
}

EXPLORE_FORM_DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "superset_explore_",
    "CACHE_REDIS_URL": REDIS_URL,
}

# =============================================================================
# Celery Configuration (for async queries)
# =============================================================================
class CeleryConfig:
    broker_url = REDIS_URL
    imports = ("superset.sql_lab",)
    result_backend = REDIS_URL
    worker_prefetch_multiplier = 1
    task_acks_late = False


CELERY_CONFIG = CeleryConfig

# =============================================================================
# Feature Flags
# =============================================================================
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_NATIVE_FILTERS_SET": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ENABLE_EXPLORE_DRAG_AND_DROP": True,
    "ENABLE_ADVANCED_DATA_TYPES": True,
    "ALERT_REPORTS": True,
    "EMBEDDED_SUPERSET": True,
    "DRILL_TO_DETAIL": True,
    "TAGGING_SYSTEM": True,
}

# =============================================================================
# Security Configuration
# =============================================================================
PUBLIC_ROLE_LIKE = "Gamma"
AUTH_ROLE_ADMIN = "Admin"
AUTH_USER_REGISTRATION = False
WTF_CSRF_ENABLED = True

# =============================================================================
# SQL Lab Configuration
# =============================================================================
SQLLAB_ASYNC_TIME_LIMIT_SEC = 600
SQLLAB_TIMEOUT = 300
SQL_MAX_ROW = 100000
DISPLAY_MAX_ROW = 10000

# =============================================================================
# ClickHouse Specific Settings
# =============================================================================
# Allow bigger queries for ClickHouse
SQLALCHEMY_POOL_SIZE = 5
SQLALCHEMY_MAX_OVERFLOW = 10
SQLALCHEMY_POOL_RECYCLE = 3600

# =============================================================================
# Dashboard Configuration
# =============================================================================
DASHBOARD_VIRTUALIZATION = True

# Time Zone
DEFAULT_TIMEZONE = "Asia/Ho_Chi_Minh"

# =============================================================================
# Logging
# =============================================================================
LOG_FORMAT = "%(asctime)s:%(levelname)s:%(name)s:%(message)s"
LOG_LEVEL = "INFO"

# =============================================================================
# Database Connection Templates
# =============================================================================
# ClickHouse connection string format:
# clickhousedb+connect://default:clickhouse123@clickhouse:8123/lakehouse

# Example preset connections (can be added via UI)
SQLALCHEMY_EXAMPLES_URI = None
PREVENT_UNSAFE_DB_CONNECTIONS = False
