## ForexFactory Collector - Anti-Bot Hardening

**Status**: ✓ All 17 hardening features implemented

### Problem
Current Selenium collector was skipping blocked months gracefully, but Cloudflare was still blocking both month pages with minimal recovery attempts.

### Solution: Multi-Layer Hardening
Enhanced anti-bot evasion across driver initialization, page stabilization, intelligent polling, and recovery mechanisms.

---

## Key Modifications

### 1. **Driver Stealth Initialization** (`_init_driver()`)
Enhanced Chrome options to reduce automation fingerprint:

| Feature | Before | After |
|---------|--------|-------|
| automation-blocking args | 4 | 9 |
| JS-based masking | 1 | 3 |
| Experimental options | 2 | 3 |

**New Options Added**:
```
--disable-background-networking  (prevents background activity detection)
--disable-sync                     (disables sync service)
--disable-plugins                  (blocks plugin enumeration)
--disable-features=TranslateUI     (removes translation popup)
--metrics-recording-only           (minimal metrics recording)
```

**JavaScript Masking** (in addition to navigator.webdriver):
```javascript
Object.defineProperty(navigator, 'chromeFlags', {get: () => ''})
Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]})
```

### 2. **Improved Page Stabilization** (before challenge decision)
**Problem**: Was deciding too early that challenge was active.
**Solution**: Wait for page to be genuinely ready before checking.

```python
# 1-3s randomized idle before navigation (human-like)
initial_idle = random.uniform(1.0, 3.0)
time.sleep(initial_idle)

# Wait for document ready state (up to 5 seconds)
for _ in range(5):
    ready_state = driver.execute_script("return document.readyState")
    if ready_state in ["complete", "interactive"]:
        break
    time.sleep(1.0)
```

### 3. **Intelligent Challenge Polling with Recovery** (major improvement)
**Before**: Single detection → wait 15s → refresh → wait 10s → skip if blocked.
**After**: 4-cycle polling + recovery path (visit homepage then retry).

**Polling Strategy**:
```python
for poll_attempt in range(4):                    # 4 attempts
    time.sleep(random.uniform(4, 6))             # 5s interval
    check_if_challenge_cleared()
    if calendar_found or challenge_cleared:
        break
```

**Recovery Path** (if polling fails):
```python
# 1. Visit calendar homepage to establish session
driver.get(CALENDAR_URL)
time.sleep(random.uniform(3, 5))

# 2. Navigate back to target month
driver.get(target_month_url)
time.sleep(5)

# 3. Check again
```

### 4. **Improved Scrolling** (more human-like)

| Aspect | Before | After |
|--------|--------|-------|
| Scroll distance ratio | 0.18-0.32 viewport | 0.12-0.24 viewport |
| Delay between scrolls | 0.5-1.0s | 0.8-1.2s |
| Base distance | 120px min | 80px min |

Slower, smaller scrolls appear more human.

### 5. **Enhanced Diagnostics When Skipping**
**Before**:
```
Skipping month due to persistent anti-bot challenge
```

**After**:
```
Skipping month due to persistent anti-bot challenge |
title=Calendar | Forex Factory |
calendar_found=False |
challenge_indicators=True |
html_length=2847
```

---

## Behavior Flow

```
1. Randomized idle (1-3s)
2. Navigate to month URL
3. Stabilization wait (2-5s)
4. Check document.readyState (up to 5s)
   ↓
5. Detect challenge indicators?
   ├─ NO → Continue to calendar/scroll (unchanged parsing)
   └─ YES → Enter polling loop...
            ├─ Poll 4 times (5s intervals)
            ├─ Each cycle: check if cleared
            ├─ If calendar appears → Continue normally ✓
            ├─ If challenge clears → Continue normally ✓
            └─ If still blocked after 4 polls → Attempt recovery...
                 ├─ Visit calendar homepage
                 ├─ Wait 3-5s
                 ├─ Navigate back to month URL
                 ├─ Wait 5s and check again
                 ├─ If cleared now → Continue normally ✓
                 └─ If still blocked → Skip month + log details ✓
6. Scroll (slower, smaller pattern: 0.8-1.2s delays)
7. Parse events (unchanged logic)
8. Return events or None
```

---

## What Did NOT Change

✓ Parsing logic (calendar table extraction → event rows)
✓ Scoring logic (impact level, value extraction)
✓ Output format (CSV schema, column names)
✓ API signatures (`collect()`, `collect_events()`)
✓ Downstream pipeline (preprocessor, validator unchanged)
✓ Core dependencies

---

## Expected Impact

| Scenario | Before | After |
|----------|--------|-------|
| Page loads cleanly | ✓ Works | ✓ Works (same) |
| Cloudflare challenge (clears quickly) | ✗ Skip | ✓ Recover (polling catches it) |
| Cloudflare challenge (persistent) | ✗ Skip | ✓ Skip gracefully + good logs |
| Mixed months (some blocked, some not) | ⚠ Partial data | ✓ Better partial data |

**Goal**: Increase success rate for live collection on blocked months (April, etc.) by 20-30% through intelligent polling and recovery.

---

## Testing

All changes validated:
- ✓ 17/17 hardening features confirmed in place
- ✓ No syntax errors
- ✓ Collector imports successfully
- ✓ Backward compatible (no API changes)

**Run command** (unchanged):
```powershell
python scripts/collect_calendar_data.py --source live --start 2026-03-06 --end 2026-04-06 --preprocess
```

Expected new log output when month hits challenge:
```
Challenge indicators detected. Starting polling loop...
Polling attempt 1/4
Polling attempt 2/4
...
Challenge persisted after polling. Attempting recovery...
Visiting Forex Factory homepage for session establishment...
Navigating back to target month URL after recovery...
Calendar content found after recovery path!  [OR]
Skipping month due to persistent anti-bot challenge | title=... | calendar_found=False | ...
```
