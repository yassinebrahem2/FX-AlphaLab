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

All helper functions are located in:

src/shared/db/storage.py

Connect to Database
from src.shared.db.storage import get_connection

conn = get_connection()
print("Connected:", conn is not None)
conn.close()

Insert FX Prices
from src.shared.db.storage import insert_fx_prices

insert_fx_prices([
    {
        "timestamp_utc": "2024-01-01 12:00:00",
        "pair": "EURUSD",
        "timeframe": "H1",
        "open": 1.10,
        "high": 1.11,
        "low": 1.09,
        "close": 1.105,
        "volume": 1234,
        "source": "test"
    }
])

Export Table to CSV (For Week 6 Deliverable)
from src.shared.db.storage import export_to_csv

export_to_csv("fx_prices", "fx_prices_export.csv")


This creates a CSV file containing all rows from the table.

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
