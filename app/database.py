import sqlalchemy as sa
from sqlalchemy import create_engine, MetaData

from app.config import settings

metadata = MetaData()

# --- Price records from WFP / SIM ---
prices = sa.Table(
    "prices",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("country", sa.String(30), nullable=False, index=True),
    sa.Column("market", sa.String(100), nullable=False),
    sa.Column("commodity", sa.String(50), nullable=False, index=True),
    sa.Column("price", sa.Float, nullable=False),
    sa.Column("currency", sa.String(10), default="XOF"),
    sa.Column("unit", sa.String(20), default="KG"),
    sa.Column("date", sa.Date, nullable=False, index=True),
    sa.Column("source", sa.String(30), default="WFP"),
    sa.Column("latitude", sa.Float, nullable=True),
    sa.Column("longitude", sa.Float, nullable=True),
    sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
)

# --- Weather data from NASA POWER ---
weather = sa.Table(
    "weather",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("country", sa.String(30), nullable=False, index=True),
    sa.Column("latitude", sa.Float, nullable=False),
    sa.Column("longitude", sa.Float, nullable=False),
    sa.Column("year", sa.Integer, nullable=False),
    sa.Column("month", sa.Integer, nullable=False),
    sa.Column("temperature", sa.Float),          # T2M (°C)
    sa.Column("precipitation", sa.Float),         # PRECTOTCORR (mm/day)
    sa.Column("humidity", sa.Float),              # RH2M (%)
    sa.Column("solar_radiation", sa.Float),       # ALLSKY_SFC_SW_DWN (kW-hr/m²/day)
    sa.Column("source", sa.String(30), default="NASA_POWER"),
    sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
)

# --- Exchange rates ---
exchange_rates = sa.Table(
    "exchange_rates",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("base_currency", sa.String(10), default="XOF"),
    sa.Column("target_currency", sa.String(10), nullable=False),
    sa.Column("rate", sa.Float, nullable=False),
    sa.Column("date", sa.Date, nullable=False),
    sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
)

# --- Commodity world prices ---
commodity_prices = sa.Table(
    "commodity_prices",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("commodity", sa.String(50), nullable=False),
    sa.Column("price_usd", sa.Float, nullable=False),
    sa.Column("unit", sa.String(20), default="MT"),
    sa.Column("date", sa.Date, nullable=False),
    sa.Column("source", sa.String(30), default="WORLD_BANK"),
    sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
)

# --- Sync log ---
sync_log = sa.Table(
    "sync_log",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("source", sa.String(30), nullable=False),
    sa.Column("country", sa.String(30)),
    sa.Column("records_fetched", sa.Integer, default=0),
    sa.Column("records_inserted", sa.Integer, default=0),
    sa.Column("status", sa.String(20), default="success"),
    sa.Column("error_message", sa.Text, nullable=True),
    sa.Column("synced_at", sa.DateTime, server_default=sa.func.now()),
)


def init_db():
    """Create all tables."""
    engine = create_engine(settings.DATABASE_URL.replace("sqlite:///", "sqlite:///"))
    metadata.create_all(engine)
    engine.dispose()
    return True


def get_engine():
    return create_engine(settings.DATABASE_URL)
