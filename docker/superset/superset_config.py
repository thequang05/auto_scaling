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
# Database Configuration (SQLite for lightweight deployments)
# =============================================================================
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "sqlite:////app/superset_home/superset.db"
)
SQLALCHEMY_TRACK_MODIFICATIONS = False

# =============================================================================
# Cache Configuration (SimpleCache for lightweight deployments)
# =============================================================================
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
}

FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
}

EXPLORE_FORM_DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
}

# Disable Celery for lightweight deployments
# CELERY_CONFIG = None

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
# Note: Pool settings removed for SQLite compatibility
# When using PostgreSQL, add back:
# SQLALCHEMY_POOL_SIZE = 5
# SQLALCHEMY_MAX_OVERFLOW = 10
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
