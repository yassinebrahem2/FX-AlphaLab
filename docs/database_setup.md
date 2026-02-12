Database Setup Guide (FX-AlphaLab)

This document explains how to install, configure, and use the PostgreSQL database for FX-AlphaLab data pipelines.

🐳 Option 1 — Run PostgreSQL with Docker (Recommended)

From the project root:

docker compose up -d


Check that the container is running:

docker ps


You should see a container named fx_alphalab_db.

🗄 Database Details
Setting	Value
Database Name	fx_alphalab
User	postgres
Password	postgres
Host	localhost
Port	5432
📦 Create Database Tables

Run the schema file inside the Docker container:

docker exec -i fx_alphalab_db psql -U postgres -d fx_alphalab < src/shared/db/schema.sql


Verify tables were created:

docker exec -it fx_alphalab_db psql -U postgres -d fx_alphalab
\dt


You should see:

fx_prices

economic_events

ecb_policy_rates

ecb_exchange_rates

macro_indicators

Exit with:

\q

🧠 Using the Database in Python

All helper functions and ORM models are located in:

src/shared/db/

The system uses **SQLAlchemy ORM** for type-safe, production-grade database access.

Connect to Database (Using ORM)
from src.shared.db import get_db, FXPrice

# Query using ORM
with get_db() as session:
    prices = session.query(FXPrice).filter(FXPrice.pair == "EURUSD").limit(10).all()
    for price in prices:
        print(f"{price.timestamp_utc}: {price.close}")

Insert FX Prices (High-Level API)
from src.shared.db import insert_fx_prices

# Returns number of records inserted (duplicates are skipped)
inserted = insert_fx_prices([
    {
        "timestamp_utc": "2024-01-01 12:00:00",
        "pair": "EURUSD",
        "timeframe": "H1",
        "open": 1.10,
        "high": 1.11,
        "low": 1.09,
        "close": 1.105,
        "volume": 1234,
        "source": "mt5"
    }
])
print(f"Inserted {inserted} records")

Insert Using ORM Models Directly
from src.shared.db import get_db, FXPrice
from datetime import datetime

with get_db() as session:
    price = FXPrice(
        timestamp_utc=datetime(2024, 1, 1, 12, 0, 0),
        pair="EURUSD",
        timeframe="H1",
        open=1.10,
        high=1.11,
        low=1.09,
        close=1.105,
        volume=1234,
        source="mt5"
    )
    session.add(price)
    # Automatically committed by context manager

Query with ORM
from src.shared.db import get_db, FXPrice, EconomicEvent
from datetime import datetime, timedelta

with get_db() as session:
    # Get recent prices
    recent = session.query(FXPrice)\
        .filter(FXPrice.timestamp_utc >= datetime.now() - timedelta(days=7))\
        .order_by(FXPrice.timestamp_utc.desc())\
        .limit(100)\
        .all()

    # High-impact events
    events = session.query(EconomicEvent)\
        .filter(EconomicEvent.impact == "High")\
        .all()

Export Table to CSV (For Week 6 Deliverable)
from src.shared.db import export_to_csv, ALLOWED_TABLES

# SQL injection protected - only whitelisted tables allowed
export_to_csv("fx_prices", "fx_prices_export.csv")

# View allowed tables
print(f"Allowed tables: {ALLOWED_TABLES}")
# {'fx_prices', 'economic_events', 'ecb_policy_rates', 'ecb_exchange_rates', 'macro_indicators'}

Available ORM Models

- **FXPrice** - OHLCV price data
- **EconomicEvent** - Calendar events
- **ECBPolicyRate** - ECB interest rates
- **ECBExchangeRate** - ECB FX rates
- **MacroIndicator** - FRED macro data

All models include:
- Automatic timestamp indexing
- Uniqueness constraints (prevents duplicates)
- Type safety via SQLAlchemy ORM

🧪 Run Unit Tests

Make sure the database container is running, then execute:

.venv/Scripts/python -m pytest tests/shared/db/test_storage.py -v


All tests should pass.

⚠️ Common Issues

Connection refused

Make sure Docker is running

Check container status with docker ps

psycopg2 not found

Activate virtual environment

Run: pip install psycopg2-binary

Tables not found

Re-run the schema command shown above

✅ Summary

This database system allows all ingestion pipelines to:

Store FX price data

Store economic calendar events

Store ECB rates and exchange rates

Store macroeconomic indicators

Export data to CSV for project deliverables
