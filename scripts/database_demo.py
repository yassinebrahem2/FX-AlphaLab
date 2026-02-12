"""
Demonstration of the SQLAlchemy ORM database layer.

This script shows how to use the production-grade database implementation
with SQLAlchemy ORM, connection pooling, and SQL injection protection.

Requirements:
    - PostgreSQL must be running (docker-compose up -d)
    - Database schema must be applied (see docs/database_setup.md)

Run:
    python examples/database_demo.py
"""

from datetime import datetime, timedelta

from src.shared.db import (
    ALLOWED_TABLES,
    EconomicEvent,
    FXPrice,
    export_to_csv,
    get_db,
    insert_fx_prices,
)


def demo_high_level_insert():
    """Demo: High-level insert functions with duplicate handling."""
    print("\n=== High-Level Insert API ===")

    # Insert FX prices (duplicates are automatically skipped)
    data = [
        {
            "timestamp_utc": "2024-01-01 12:00:00",
            "pair": "EURUSD",
            "timeframe": "H1",
            "open": 1.1000,
            "high": 1.1050,
            "low": 1.0990,
            "close": 1.1020,
            "volume": 1234,
            "source": "demo",
        },
        {
            "timestamp_utc": "2024-01-01 13:00:00",
            "pair": "EURUSD",
            "timeframe": "H1",
            "open": 1.1020,
            "high": 1.1080,
            "low": 1.1015,
            "close": 1.1070,
            "volume": 1567,
            "source": "demo",
        },
    ]

    inserted = insert_fx_prices(data)
    print(f"✓ Inserted {inserted} FX price records")

    # Insert the same data again - duplicates will be skipped
    duplicate_attempt = insert_fx_prices(data)
    print(f"✓ Duplicate insert attempt: {duplicate_attempt} records (correctly rejected)")


def demo_orm_insert():
    """Demo: Direct ORM model usage for type-safe inserts."""
    print("\n=== ORM Model Insert ===")

    with get_db() as session:
        # Create using ORM models (type-safe)
        event = EconomicEvent(
            timestamp_utc=datetime(2024, 1, 15, 10, 0, 0),
            country="US",
            event_name="CPI YoY",
            impact="High",
            actual=3.4,
            forecast=3.2,
            previous=3.1,
            source="demo",
        )
        session.add(event)
        print("✓ Added economic event using ORM model")


def demo_orm_query():
    """Demo: Querying with SQLAlchemy ORM."""
    print("\n=== ORM Query Examples ===")

    with get_db() as session:
        # Query recent FX prices
        recent_prices = (
            session.query(FXPrice)
            .filter(FXPrice.pair == "EURUSD")
            .filter(FXPrice.timestamp_utc >= datetime.now() - timedelta(days=365))
            .order_by(FXPrice.timestamp_utc.desc())
            .limit(5)
            .all()
        )

        print(f"✓ Found {len(recent_prices)} recent EURUSD prices:")
        for price in recent_prices:
            print(f"  {price.timestamp_utc}: O={price.open} C={price.close}")

        # Count records by source
        price_count = session.query(FXPrice).count()
        event_count = session.query(EconomicEvent).count()
        print("\n✓ Database contains:")
        print(f"  FX Prices: {price_count}")
        print(f"  Economic Events: {event_count}")


def demo_csv_export():
    """Demo: Export to CSV with SQL injection protection."""
    print("\n=== CSV Export (SQL Injection Protected) ===")

    # Valid export
    export_to_csv("fx_prices", "demo_fx_prices.csv")
    print("✓ Exported fx_prices to demo_fx_prices.csv")

    # Show allowed tables
    print(f"✓ Whitelisted tables: {', '.join(sorted(ALLOWED_TABLES))}")

    # Attempt SQL injection (will be blocked)
    try:
        export_to_csv("fx_prices; DROP TABLE fx_prices; --", "malicious.csv")
        print("✗ SQL injection was NOT blocked (BUG!)")
    except ValueError as e:
        print(f"✓ SQL injection blocked: {e}")


def demo_relationship_queries():
    """Demo: Advanced queries across multiple tables."""
    print("\n=== Advanced Queries ===")

    with get_db() as session:
        # Get high-impact events
        high_impact = (
            session.query(EconomicEvent)
            .filter(EconomicEvent.impact == "High")
            .filter(EconomicEvent.timestamp_utc >= datetime.now() - timedelta(days=30))
            .all()
        )
        print(f"✓ High-impact events (last 30 days): {len(high_impact)}")

        # Get price statistics
        from sqlalchemy import func

        stats = (
            session.query(
                FXPrice.pair,
                func.count(FXPrice.id).label("count"),
                func.min(FXPrice.timestamp_utc).label("first"),
                func.max(FXPrice.timestamp_utc).label("last"),
            )
            .group_by(FXPrice.pair)
            .all()
        )

        print("\n✓ Price data coverage:")
        for pair, count, first, last in stats:
            print(f"  {pair}: {count} records from {first} to {last}")


def main():
    """Run all database demonstrations."""
    print("=" * 70)
    print("FX-AlphaLab Database System Demo")
    print("SQLAlchemy ORM with Connection Pooling & SQL Injection Protection")
    print("=" * 70)

    try:
        demo_high_level_insert()
        demo_orm_insert()
        demo_orm_query()
        demo_csv_export()
        demo_relationship_queries()

        print("\n" + "=" * 70)
        print("✓ Demo completed successfully")
        print("=" * 70)

    except Exception as e:
        print(f"\n✗ Demo failed: {e}")
        print("\nMake sure PostgreSQL is running:")
        print("  docker-compose up -d")
        print(
            "  docker exec -i fx_alphalab_db psql -U postgres -d fx_alphalab < src/shared/db/schema.sql"
        )
        raise


if __name__ == "__main__":
    main()
