# Rescue Fixes Report — 2026-03-01

## 1. ✅ Weekly Report Generation — Fixed

**Problem:** No weekly report generation script existed. The `weekly_recalibrate.py` only does model recalibration, not trading/P&L reporting. No cron job for weekly reports.

**Fix:** Created `generate_weekly_report.py` with:
- **Live Kalshi data pull:** `kalshi_client.get_balance()`, `get_positions()`, `get_orders(status="resting")`
- **P&L from fills/settlements:** Reads `trades.json`, computes realized P&L from settled trades (not event logs)
- **Reconciliation block:** Start equity ($510.76) → realized + unrealized − fees = expected equity, compared to Kalshi-reported balance, shows discrepancy
- **1σ coverage per city:** Reads `sigma_optimization_results.json`, shows x/y for each city, flags "LOW SAMPLE" when samples < 10
- **Unit labels fixed:** `source_spread` labeled as °F, prices as ¢

**File:** `generate_weekly_report.py` — can be added to crontab as:
```
0 15 * * 1 cd /home/ubuntu/.openclaw/workspace/weather-trading && python3 generate_weekly_report.py >> /tmp/weekly_report.log 2>&1
```

---

## 2. ✅ Journal Dedup — Fixed

**Problem:** `trade_journal.json` had 525 entries with massive duplication. `KXHIGHMIA-26FEB21-B85.5` appeared 35 times (34 ADDs from scanner re-scans + 1 EXIT).

**Fix (two-part):**

### A. Cleanup script (`cleanup_journal.py`)
Consolidates all ADD/ENTRY records per ticker into one ENTRY with total contracts and avg price. Keeps only the last EXIT per ticker.

**Results:**
| Ticker | Before | After |
|--------|--------|-------|
| KXHIGHMIA-26FEB21-B85.5 | 35 | 2 (1 entry + 1 exit) |
| KXHIGHTATL-26FEB23-T38 | 30 | 2 |
| KXHIGHTATL-26FEB21-T65 | 16 | 2 |
| KXHIGHTATL-26FEB22-T55 | 16 | 2 |
| **Total entries** | **525** | **229** |

### B. Fixed `trade_journal.py` `log_action()`
- ADD/ENTRY: If same ticker already has an entry today, rolls contracts into existing record (updates qty, avg price, add_count)
- EXIT actions: If same ticker+action already logged today, returns existing (no duplicate)
- Backup preserved at `trade_journal.json.backup_20260301_194756`

---

## 3. ✅ Denver Disabled (Reversible)

**Config change in `trading_config.json`:**
- `allowed_cities`: `["Chicago", "Denver", "Minneapolis", "Phoenix", "Las Vegas"]` → `["Chicago", "Minneapolis", "Phoenix", "Las Vegas"]`
- Added `disabled_cities.Denver` with timestamp and reason (reversible — just remove entry and re-add to allowed_cities)

**Scanner fix in `fast_scanner.py`:**
- Added check for `disabled_cities` dict in config before the allowed_cities check
- Logs `city_disabled_{city}` as skip reason

**Why Denver is bad:**
- 1σ coverage: **52.8%** (worst of all calibrated cities, vs 72% Chicago, 80% Miami)
- All 71 decision_log entries from Denver were bracket markets → all blocked by rescue mode
- High source_spread (4.6-6.5°F consistently)

**Replacement city suggestion:**
- **Oklahoma City** — 7 model families enabled (same as Denver), already tradeable in city_model_config, and historically better calibrated
- Note: sigma optimization was only run for 5 cities (Miami, Austin, Chicago, Denver, NY). OKC doesn't have sigma optimization data yet but has 7/7 model families

---

## 4. ✅ Zero Trades in Rescue Mode — Explained

**Full analysis in `rescue_mode_analysis.md`**

**TL;DR:** Two rescue-mode config flags create a complete blockade:
1. `threshold_only: true` → kills 71% of opportunities (all bracket `-B` markets)
2. `allowed_sides: ["NO"]` → kills remaining 29% (YES-side threshold markets)

**100% of opportunities blocked before reaching quality filters.**

The best candidate was `KXHIGHTMIN-26FEB26-T40 YES @ 12¢` with 41.6¢ net EV and 53.6% win probability — a genuinely good trade killed by `side_disabled_rescue`.

**Recommendation:** Set `threshold_only: false` to unlock bracket markets. The downstream quality filters (min_net_ev: 8¢, max_spread: 4¢, abstain zone) are well-configured to filter bad trades without blanket bans.
