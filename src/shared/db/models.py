from sqlalchemy import TIMESTAMP, Column, Float, Index, Integer, String, UniqueConstraint

from .base import Base


class FXPrice(Base):
    __tablename__ = "fx_prices"

    id = Column(Integer, primary_key=True)
    timestamp_utc = Column(TIMESTAMP, nullable=False)
    pair = Column(String(10), nullable=False)
    timeframe = Column(String(10))
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    source = Column(String(50))

    __table_args__ = (
        UniqueConstraint("timestamp_utc", "pair", "timeframe"),
        Index("idx_fx_prices_time", "timestamp_utc"),
    )


class EconomicEvent(Base):
    __tablename__ = "economic_events"

    id = Column(Integer, primary_key=True)
    timestamp_utc = Column(TIMESTAMP, nullable=False)
    country = Column(String(50))
    event_name = Column(String(100))
    impact = Column(String(20))
    actual = Column(Float)
    forecast = Column(Float)
    previous = Column(Float)
    source = Column(String(50))

    __table_args__ = (
        UniqueConstraint("timestamp_utc", "event_name"),
        Index("idx_economic_events_time", "timestamp_utc"),
    )


class ECBPolicyRate(Base):
    __tablename__ = "ecb_policy_rates"

    id = Column(Integer, primary_key=True)
    timestamp_utc = Column(TIMESTAMP, nullable=False)
    rate_type = Column(String(50))
    rate = Column(Float)
    frequency = Column(String(20))
    unit = Column(String(20))
    source = Column(String(50))

    __table_args__ = (
        UniqueConstraint("timestamp_utc", "rate_type"),
        Index("idx_ecb_policy_rates_time", "timestamp_utc"),
    )


class ECBExchangeRate(Base):
    __tablename__ = "ecb_exchange_rates"

    id = Column(Integer, primary_key=True)
    timestamp_utc = Column(TIMESTAMP, nullable=False)
    currency_pair = Column(String(20))
    rate = Column(Float)
    frequency = Column(String(20))
    source = Column(String(50))

    __table_args__ = (
        UniqueConstraint("timestamp_utc", "currency_pair"),
        Index("idx_ecb_exchange_rates_time", "timestamp_utc"),
    )


class MacroIndicator(Base):
    __tablename__ = "macro_indicators"

    id = Column(Integer, primary_key=True)
    timestamp_utc = Column(TIMESTAMP, nullable=False)
    series_id = Column(String(100))
    value = Column(Float)
    source = Column(String(50))

    __table_args__ = (
        UniqueConstraint("timestamp_utc", "series_id"),
        Index("idx_macro_indicators_time", "timestamp_utc"),
    )
