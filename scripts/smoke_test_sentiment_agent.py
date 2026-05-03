"""Smoke test for SentimentAgent on a specific date."""

from datetime import datetime, timedelta, timezone

from src.agents.sentiment.agent import SentimentAgent
from src.agents.sentiment.gdelt_node import GDELTSignalNode
from src.agents.sentiment.google_trends_node import GoogleTrendsSignalNode
from src.agents.sentiment.reddit_node import RedditSignalNode
from src.agents.sentiment.stocktwits_node import StocktwitsSignalNode
from src.shared.config import Config

# Test date: fixed 2024 date for reproducible smoke test
TEST_DATE = datetime(2024, 6, 15, tzinfo=timezone.utc)

print(f"Testing SentimentAgent for: {TEST_DATE.date()}")
print("=" * 80)

# Initialize nodes with real data sources
try:
    print("\n1. Initializing nodes...")
    stocktwits_node = StocktwitsSignalNode(
        checkpoint_path=(
            Config.DATA_DIR
            / "processed"
            / "sentiment"
            / "source=stockwits"
            / "labels_checkpoint.jsonl"
        ),
        log_file=Config.LOGS_DIR / "agents" / "stocktwits_node.log",
    )
    reddit_node = RedditSignalNode(
        checkpoint_path=(
            Config.DATA_DIR
            / "processed"
            / "sentiment"
            / "source=reddit"
            / "reddit_labels_checkpoint.jsonl"
        ),
        log_file=Config.LOGS_DIR / "agents" / "reddit_node.log",
    )
    gdelt_node = GDELTSignalNode(
        silver_dir=Config.DATA_DIR / "processed" / "sentiment" / "source=gdelt",
        log_file=Config.LOGS_DIR / "agents" / "gdelt_node.log",
    )
    gtrends_node = GoogleTrendsSignalNode(
        silver_dir=Config.DATA_DIR / "processed" / "sentiment" / "source=google_trends",
        log_file=Config.LOGS_DIR / "agents" / "gtrends_node.log",
    )
    print("   ✓ All nodes initialized")

    # Create agent
    print("\n2. Creating SentimentAgent...")
    agent = SentimentAgent(
        stocktwits_node=stocktwits_node,
        reddit_node=reddit_node,
        gdelt_node=gdelt_node,
        gtrends_node=gtrends_node,
        log_file=Config.LOGS_DIR / "agents" / "sentiment_agent.log",
    )
    print("   ✓ SentimentAgent created")

    # Test compute_batch
    print(
        f"\n3. Running compute_batch({(TEST_DATE - timedelta(days=7)).date()} to {TEST_DATE.date()})..."
    )
    batch_df = agent.compute_batch(
        TEST_DATE - timedelta(days=7),
        TEST_DATE,
        include_context=False,
    )
    print(f"   ✓ Returned {len(batch_df)} rows")
    print("\n   Sample output (last 3 rows):")
    print(
        batch_df[
            [
                "timestamp_utc",
                "usdjpy_stocktwits_net_sentiment",
                "usdjpy_stocktwits_active",
                "gdelt_tone_zscore",
                "gdelt_attention_zscore",
                "macro_attention_zscore",
                "composite_stress_flag",
            ]
        ]
        .tail(3)
        .to_string()
    )

    # Test get_signal
    print(f"\n4. Running get_signal({TEST_DATE.date()})...")
    signal = agent.get_signal(TEST_DATE, include_context=False)
    print("   ✓ SentimentSignal generated")
    print("\n   Signal details:")
    print(f"     timestamp_utc: {signal.timestamp_utc}")
    print(f"     usdjpy_stocktwits_net_sentiment: {signal.usdjpy_stocktwits_net_sentiment}")
    print(f"     usdjpy_stocktwits_active: {signal.usdjpy_stocktwits_active}")
    print(f"     gdelt_tone_zscore: {signal.gdelt_tone_zscore}")
    print(f"     gdelt_attention_zscore: {signal.gdelt_attention_zscore}")
    print(f"     macro_attention_zscore: {signal.macro_attention_zscore}")
    print(f"     composite_stress_flag: {signal.composite_stress_flag}")

    # Test get_signal with context
    print(f"\n5. Running get_signal({TEST_DATE.date()}) with context...")
    signal_ctx = agent.get_signal(TEST_DATE, include_context=True)
    print("   ✓ SentimentSignal with context generated")
    print(f"\n   Context type: {type(signal_ctx.context)}")
    if signal_ctx.context:
        print(f"   Context keys: {list(signal_ctx.context.keys())}")

    print("\n" + "=" * 80)
    print("✓ SMOKE TEST PASSED")

except Exception as e:
    print(f"\n✗ SMOKE TEST FAILED: {e}")
    import traceback

    traceback.print_exc()
    exit(1)
