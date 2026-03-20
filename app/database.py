"""
AgroPrix - Database models and initialisation (SQLAlchemy Core / Table pattern).
"""

from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    func,
)
from sqlalchemy.engine import Engine

from app.config import DATABASE_URL

metadata = MetaData()

# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

prices = Table(
    "prices",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("country", String(64), nullable=False),
    Column("market", String(128), nullable=False),
    Column("commodity", String(128), nullable=False),
    Column("price", Float, nullable=False),
    Column("currency", String(8), default="XOF"),
    Column("unit", String(16), default="KG"),
    Column("date", String(32), nullable=False),
    Column("source", String(32), default="WFP"),
    Column("latitude", Float, nullable=True),
    Column("longitude", Float, nullable=True),
    Column("created_at", DateTime, default=lambda: datetime.now(timezone.utc)),
)

weather = Table(
    "weather",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("country", String(64), nullable=False),
    Column("lat", Float, nullable=False),
    Column("lon", Float, nullable=False),
    Column("year", Integer, nullable=False),
    Column("month", Integer, nullable=False),
    Column("temperature", Float, nullable=True),
    Column("precipitation", Float, nullable=True),
    Column("humidity", Float, nullable=True),
    Column("solar_radiation", Float, nullable=True),
    Column("source", String(32), default="NASA_POWER"),
    Column("created_at", DateTime, default=lambda: datetime.now(timezone.utc)),
)

exchange_rates = Table(
    "exchange_rates",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("base_currency", String(8), default="XOF"),
    Column("target_currency", String(8), nullable=False),
    Column("rate", Float, nullable=False),
    Column("date", String(32), nullable=False),
    Column("created_at", DateTime, default=lambda: datetime.now(timezone.utc)),
)

sync_log = Table(
    "sync_log",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source", String(32), nullable=False),
    Column("country", String(64), nullable=True),
    Column("records_fetched", Integer, nullable=False),
    Column("records_inserted", Integer, default=0),
    Column("status", String(16), default="success"),
    Column("error_message", Text, nullable=True),
    Column("synced_at", DateTime, default=lambda: datetime.now(timezone.utc)),
)

# ---------------------------------------------------------------------------
# Indexes
# ---------------------------------------------------------------------------

users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("email", String, unique=True, nullable=False),
    Column("password_hash", String, nullable=False),
    Column("name", String, nullable=False),
    Column("role", String, default="free"),  # free, pro, expert, admin
    Column("phone", String, nullable=True),
    Column("country", String, default="benin"),
    Column("created_at", DateTime, default=func.now()),
    Column("last_login", DateTime, nullable=True),
)

Index("ix_users_email", users.c.email)
Index("ix_prices_country", prices.c.country)
Index("ix_prices_commodity", prices.c.commodity)
Index("ix_prices_date", prices.c.date)
Index("ix_weather_country", weather.c.country)

# ---------------------------------------------------------------------------
# Engine helpers
# ---------------------------------------------------------------------------

_engine = None  # type: Engine or None


def get_engine() -> Engine:
    """Return a singleton SQLAlchemy engine."""
    global _engine
    if _engine is None:
        connect_args = {}
        if DATABASE_URL.startswith("sqlite"):
            connect_args["check_same_thread"] = False
        _engine = create_engine(DATABASE_URL, connect_args=connect_args, echo=False)
    return _engine


def is_sqlite():
    """Check if using SQLite database."""
    return DATABASE_URL.startswith("sqlite")


def sql_year_month(col):
    """Return SQL expression for YYYY-MM extraction, compatible with SQLite and PostgreSQL."""
    if is_sqlite():
        return "strftime('%Y-%m', {})".format(col)
    else:
        return "to_char({}::date, 'YYYY-MM')".format(col)


def sql_month_num(col):
    """Return SQL for extracting month number (1-12), compatible with SQLite and PostgreSQL."""
    if is_sqlite():
        return "CAST(strftime('%m', {}) AS INTEGER)".format(col)
    else:
        return "EXTRACT(MONTH FROM {}::date)::INTEGER".format(col)


def sql_year_month_from_ym(year_col, month_col):
    """Build YYYY-MM from separate year and month columns."""
    if is_sqlite():
        return "printf('%04d-%02d', {}, {})".format(year_col, month_col)
    else:
        return "LPAD({}::text, 4, '0') || '-' || LPAD({}::text, 2, '0')".format(year_col, month_col)


def sql_date_months_ago(months: int) -> str:
    """Return SQL expression for a date N months in the past, compatible with SQLite and PostgreSQL."""
    if is_sqlite():
        return "date('now', '-{} months')".format(months)
    else:
        return "(CURRENT_DATE - INTERVAL '{} months')".format(months)


def init_db() -> None:
    """Create all tables if they do not exist yet."""
    # Ensure the data/ directory exists for SQLite
    if DATABASE_URL.startswith("sqlite"):
        db_path = DATABASE_URL.replace("sqlite:///", "")
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    engine = get_engine()
    metadata.create_all(engine)
