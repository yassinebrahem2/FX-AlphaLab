# Global configuration (Week 4 – Foundation only)

FX_PAIRS = [
    "EURUSD",
    "GBPUSD",
    "USDCHF",
    "USDJPY",
    "EURGBP",
]

PRICE_TIMEFRAMES = [
    "H1",
    "H4",
    "D1",
]

TIMEZONE_STANDARD = "UTC"

DATA_SOURCES = {
    "price": "MT5",
    "macro": "economic_calendar",
    "central_bank": "official_websites",
}
