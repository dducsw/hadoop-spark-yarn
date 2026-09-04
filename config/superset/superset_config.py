import os

# ------------------------------------------------------------------------------
# Apache Superset Configuration
# ------------------------------------------------------------------------------
ROW_LIMIT = 5000
SUPERSET_WEBSERVER_PORT = 8088

# Secret key for session cryptography (override in production via env)
SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY", "SUPERSET_SECRET_KEY_FINTECH_BIGDATA_PLATFORM_SECURE_2026"
)

# Metadata Database (PostgreSQL)
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://superset:supersetpassword@postgres:5432/superset",
)

# Local Development conveniences
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False

# Feature Flags
FEATURE_FLAGS = {
    "ALERT_REPORTS": False,
    "DASHBOARD_NATIVE_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}
