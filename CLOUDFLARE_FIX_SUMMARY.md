## ForexFactory Collector - Cloudflare Resilience Fixes

**Goal**: Handle Cloudflare anti-bot challenges gracefully without crashing the pipeline

---

### Changes Made to `src/ingestion/collectors/forexfactory_collector.py`

#### 1. **Explicit Cloudflare Challenge Detection** (lines ~800-840)
**Before**: Only checked for "just a moment" in page title
**After**: Detects multiple indicators:
- Page title: "Un instant" or "Just a moment"
- HTML content: "challenge" or "cloudflare" keywords

#### 2. **Longer Wait + Refresh on Detection** (lines ~800-840)
**Before**: Quick 2-second retry loop
**After**: When Cloudflare challenge detected:
- Wait 15 seconds for challenge to clear
- Refresh page (force re-evaluation)
- Wait 10 more seconds
- Check if still blocked

#### 3. **Graceful Month Skipping** (lines ~800, ~968)
**Before**: Fails pipeline if month times out
**After**:
- If challenge persists after retry: `return None` (graceful skip)
- Logs clearly: `"Skipping month due to persistent anti-bot challenge"`
- Pipeline continues to next month

#### 4. **Human-Like Behavior Enhancements**
| Change | Before | After |
|--------|--------|-------|
| Initial stabilization delay | 2.0-4.0s | 2.0-5.0s |
| Scroll timing | 0.4-0.9s | 0.5-1.0s |

---

### Code Changes Summary

| Component | Type | Locations | Impact |
|-----------|------|-----------|--------|
| Human delay | Timing | Line ~758 | Wider random range before first interaction |
| Cloudflare detection | Logic | Lines ~800-840 | Explicit challenge identification + retry |
| Refresh handling | Behavior | Lines ~810-840 | Attempt to clear challenge without crashing |
| Scroll delays | Timing | Line ~876 | More human-like scroll behavior |
| Skip logging | Logging | Lines ~810, ~968 | Clear signals when month is skipped |

---

### What Did NOT Change

✓ **Parsing logic** - Calendar table parsing unchanged
✓ **Scoring logic** - Event score calculation unchanged
✓ **Output format** - CSV schema and columns unchanged
✓ **Other dependencies** - No changes to imports or external calls
✓ **API contracts** - `collect()`, `collect_events()` signatures unchanged

---

### Behavior Flow

```
1. Navigate to Forex Factory month view
2. Wait 2-5s (human stabilization)
3. Configure timezone (first time only)
4. ↓
   └─ Cloudflare challenge detected?
      ├─ YES → Wait 15s → Refresh → Wait 10s → Re-check
      │        ├─ Still blocked? → Skip month, log warning, return None ✓
      │        └─ Cleared? → Continue to next step ✓
      └─ NO → Continue to next step ✓
5. Wait for calendar table
6. Scroll with 0.5-1.0s delays (human-like)
7. Parse events (unchanged logic)
8. Return events
```

---

### Testing via Collection Scripts

The existing scripts still work unchanged:
```bash
# Fixture mode (unchanged)
python scripts/collect_calendar_data.py --source fixture

# Live mode (now resilient to Cloudflare)
python scripts/collect_calendar_data.py --source live --start 2026-03-06 --end 2026-04-06 --preprocess
```

If a month hits Cloudflare and can't recover:
- Collection continues (doesn't crash)
- Log shows: `Skipping month due to persistent anti-bot challenge`
- Next month is fetched
- Final result has partial data (expected/acceptable)

---

### Verification

All modifications confirmed via `verify_cloudflare_fix.py`:
- ✓ Random 2-5s human delay
- ✓ Cloudflare detection ('Un instant')
- ✓ Refresh on challenge
- ✓ Persistent Cloudflare check
- ✓ Skip logging
- ✓ Increased scroll delay (0.5-1.0s)
- ✓ 15-second wait on challenge
- ✓ 10-second wait after refresh
