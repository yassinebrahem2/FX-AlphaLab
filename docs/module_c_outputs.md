# Module C Calendar Outputs

## Primary Downstream Output

**calendar_state_snapshot_latest.csv** is the primary downstream-ready output for calendar analysis.

## Published Outputs

### calendar_events_latest.csv
Raw scored event data from the calendar source. Contains all individual events with:
- date, currency, impact, final_score, score_available

Use this for granular event-level analysis and debugging.

### calendar_currency_signals_latest.csv
Daily aggregated signals per currency. Groups events by date and currency, providing:
- date, currency, event_count, high_impact_count, net_macro_score, dominant_direction

Use this for daily signal analysis per currency.

### calendar_state_snapshot_latest.csv
**Primary downstream output.** Merged calendar state per currency combining recent signals and future outlook. Key columns:
- snapshot_time_utc: timestamp of the snapshot
- currency: currency code (3-letter)
- recent_net_macro_score: aggregated score from recent events
- recent_event_count: count of recent events
- recent_high_impact_count: high-impact event count
- dominant_direction: positive, negative, or neutral bias
- time_to_next_event_hours: hours until next calendar event
- time_to_next_high_impact_event_hours: hours until next high-impact event
- events_next_4h, events_next_24h: event counts by horizon
- high_impact_events_next_24h: high-impact event count in 24h
- calendar_pressure_label: high_upcoming_risk, moderate_upcoming_risk, or quiet_calendar
- macro_bias_label: positive_macro_bias, negative_macro_bias, or neutral_macro_bias
- calendar_state_label: fused state label combining macro bias and pressure

Use this for portfolio and trading decisions.

## Access Pattern

```python
from src.ingestion.calendar_interface import (
    load_calendar_state_snapshot,
    get_currency_calendar_state,
    load_scored_calendar_events,
)

# Load full snapshot
snapshot = load_calendar_state_snapshot()

# Get state for one currency
usd_state = get_currency_calendar_state("USD")

# Load raw events
events = load_scored_calendar_events()
```
