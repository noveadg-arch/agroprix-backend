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
    # Enriched profile (CGU data)
    Column("cultures", String, nullable=True),          # JSON array: ["mais","cajou"]
    Column("superficie", Float, nullable=True),          # hectares
    Column("genre", String, nullable=True),              # homme/femme
    Column("age", Integer, nullable=True),
    Column("experience", Integer, nullable=True),        # years
    Column("type_exploitation", String, nullable=True),  # individuel/cooperative/entreprise
    Column("membre_cooperative", String, nullable=True), # yes/no + name
    Column("profil_type", String, nullable=True),        # producteur/negociant/exportateur/proprietaire
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
        _engine = create_engine(DATABASE_URL, connect_args=connect_args, echo=False, pool_pre_ping=True)
    return _engine


def is_sqlite():
    """Check if using SQLite database."""
    return DATABASE_URL.startswith("sqlite")


def sql_year_month(col):
    """Return SQL expression for YYYY-MM extraction, compatible with SQLite and PostgreSQL.
    Uses SUBSTR(col, 1, 7) which works on both since date is stored as VARCHAR 'YYYY-MM-DD'.
    Avoids to_char(date::date) which fails when 'date' is a reserved word in PostgreSQL.
    """
    return "SUBSTR({}, 1, 7)".format(col)


def sql_month_num(col):
    """Return SQL for extracting month number (1-12), compatible with SQLite and PostgreSQL."""
    return "CAST(SUBSTR({}, 6, 2) AS INTEGER)".format(col)


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


def _ensure_enriched_columns(engine) -> None:
    """Add enriched profile columns to users table if they don't exist (safe migration)."""
    columns_to_add = [
        ("cultures", "VARCHAR"),
        ("superficie", "FLOAT"),
        ("genre", "VARCHAR"),
        ("age", "INTEGER"),
        ("experience", "INTEGER"),
        ("type_exploitation", "VARCHAR"),
        ("membre_cooperative", "VARCHAR"),
        ("profil_type", "VARCHAR"),
    ]
    from sqlalchemy import text, inspect
    try:
        inspector = inspect(engine)
        existing = [c["name"] for c in inspector.get_columns("users")]
        with engine.begin() as conn:
            for col_name, col_type in columns_to_add:
                if col_name not in existing:
                    conn.execute(text(f"ALTER TABLE users ADD COLUMN {col_name} {col_type}"))
                    print(f"[AgroPrix] Added column users.{col_name}")
    except Exception as e:
        print(f"[AgroPrix] Migration note: {e}")


def init_db() -> None:
    """Create all tables if they do not exist yet."""
    # Ensure the data/ directory exists for SQLite
    if DATABASE_URL.startswith("sqlite"):
        db_path = DATABASE_URL.replace("sqlite:///", "")
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    try:
        engine = get_engine()
        metadata.create_all(engine)
        _ensure_enriched_columns(engine)
        print("[AgroPrix] Database initialized successfully")
    except Exception as e:
        print(f"[AgroPrix] WARNING: Database init failed: {e}")
        print("[AgroPrix] App will start but DB operations may fail")
