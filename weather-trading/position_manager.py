#!/usr/bin/env python3
"""
Position Manager v2 - Smart intraday monitoring & dynamic exit/re-entry.

Runs every 10 min via system crontab. Zero AI token cost.

Key improvements over v1:
- Dynamic danger zone based on time-of-day + city volatility (not fixed 2°F)
- Only locks profit when forecast is SHIFTING, not just because price moved
- Partial exits: can sell half (NO RE-ENTRY/HEDGE — disabled to stop value destruction)
- Sends webhook/notification only when action taken
"""

import json
import os
import sys
import math
import time
import statistics
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kalshi_client
import analyzer
import trade_journal
import city_logger

# ── Files ───────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRADES_FILE = os.path.join(BASE_DIR, "trades.json")
LOG_FILE = os.path.join(BASE_DIR, "position_manager_log.json")
STATE_FILE = os.path.join(BASE_DIR, "pm_state.json")
NOTIFY_FILE = os.path.join(BASE_DIR, "pm_notifications.json")

# ── City Metadata ───────────────────────────────────────────────────

SERIES_META = {
    # Coordinates = NWS official measurement stations (what Kalshi settles on)
    "KXHIGHNY":  {"city": "New York",     "lat": 40.7789, "lon": -73.9692, "tz": "America/New_York",    "peak_hour": 15},  # Central Park
    "KXHIGHCHI": {"city": "Chicago",      "lat": 41.7868, "lon": -87.7522, "tz": "America/Chicago",     "peak_hour": 15},  # Midway (KMDW)
    "KXHIGHMIA": {"city": "Miami",        "lat": 25.7959, "lon": -80.2870, "tz": "America/New_York",    "peak_hour": 15},  # KMIA
    "KXHIGHDEN": {"city": "Denver",       "lat": 39.8561, "lon": -104.6737, "tz": "America/Denver",     "peak_hour": 14},  # KDEN
    "KXHIGHLAX": {"city": "Los Angeles",  "lat": 33.9425, "lon": -118.4081, "tz": "America/Los_Angeles","peak_hour": 14},  # KLAX
    "KXHIGHAUS": {"city": "Austin",       "lat": 30.1945, "lon": -97.6699, "tz": "America/Chicago",     "peak_hour": 15},  # KAUS

    # Extended cities — all using NWS official station coords
    "KXHIGHTMIN": {"city": "Minneapolis",    "lat": 44.8831, "lon": -93.2289, "tz": "America/Chicago",      "peak_hour": 15},  # KMSP
    "KXHIGHTDC":  {"city": "Washington DC",  "lat": 38.8512, "lon": -77.0402, "tz": "America/New_York",     "peak_hour": 15},  # KDCA
    "KXHIGHTATL": {"city": "Atlanta",        "lat": 33.6407, "lon": -84.4277, "tz": "America/New_York",     "peak_hour": 15},  # KATL
    "KXHIGHPHIL": {"city": "Philadelphia",   "lat": 39.8721, "lon": -75.2411, "tz": "America/New_York",     "peak_hour": 15},  # KPHL
    "KXHIGHTHOU": {"city": "Houston",        "lat": 29.6454, "lon": -95.2789, "tz": "America/Chicago",      "peak_hour": 15},  # KHOU (Hobby — Kalshi CLIHOU)
    "KXHIGHTDAL": {"city": "Dallas",         "lat": 32.8998, "lon": -97.0403, "tz": "America/Chicago",      "peak_hour": 15},  # KDFW
    "KXHIGHTSEA": {"city": "Seattle",        "lat": 47.4502, "lon": -122.3088, "tz": "America/Los_Angeles", "peak_hour": 14},  # KSEA
    "KXHIGHTBOS": {"city": "Boston",         "lat": 42.3656, "lon": -71.0096, "tz": "America/New_York",     "peak_hour": 15},  # KBOS
    "KXHIGHTPHX": {"city": "Phoenix",        "lat": 33.4373, "lon": -112.0078, "tz": "America/Phoenix",     "peak_hour": 15},  # KPHX
    "KXHIGHTOKC": {"city": "Oklahoma City",  "lat": 35.3931, "lon": -97.6007, "tz": "America/Chicago",      "peak_hour": 15},  # KOKC
    "KXHIGHTLV":  {"city": "Las Vegas",      "lat": 36.0840, "lon": -115.1537, "tz": "America/Los_Angeles", "peak_hour": 15},  # KLAS
    "KXHIGHTSFO": {"city": "San Francisco",  "lat": 37.6213, "lon": -122.3790, "tz": "America/Los_Angeles", "peak_hour": 14},  # KSFO
    "KXHIGHTSATX":{"city": "San Antonio",    "lat": 29.5337, "lon": -98.4698, "tz": "America/Chicago",      "peak_hour": 15},  # KSAT
    "KXHIGHTNOLA":{"city": "New Orleans",    "lat": 29.9934, "lon": -90.2580, "tz": "America/Chicago",      "peak_hour": 15},  # KMSY
}

MARKET_SELL_DISCOUNT = 2  # Legacy flat value — now overridden by dynamic discount in exit_position()
RE_ENTRY_MIN_EDGE = 0.08  # 8% min edge for re-entry (dead code — re-entry disabled)

# Load config flags
def _load_pm_config():
    cfg_path = os.path.join(BASE_DIR, "trading_config.json")
    try:
        with open(cfg_path) as f:
            return json.load(f).get("position_manager", {})
    except:
        return {}
_PM_CFG = _load_pm_config()
HEDGE_ENABLED = _PM_CFG.get("hedge_enabled", False)  # DISABLED — hedging was destroying value
REENTRY_ENABLED = _PM_CFG.get("reentry_enabled", False)  # DISABLED — same reason

# IMPROVED: Portfolio exposure limits
MAX_PORTFOLIO_EXPOSURE_PCT = 0.60  # 60% of portfolio value
RESERVE_CENTS = 5000  # Must match fast_scanner.py

# FIX 2: Re-entry lockout file — prevents scanner from re-entering sold tickers for 2 hours
LOCKOUT_FILE = os.path.join(BASE_DIR, "reentry_lockouts.json")
LOCKOUT_HOURS = 2

def add_lockout(ticker, series, target_date):
    """Record a lockout after PM sells a position."""
    lockouts = {}
    if os.path.exists(LOCKOUT_FILE):
        try:
            with open(LOCKOUT_FILE) as f:
                lockouts = json.load(f)
        except:
            lockouts = {}
    key = f"{ticker}|{series}|{target_date}"
    lockouts[key] = datetime.now(timezone.utc).isoformat()
    # Prune old lockouts (> 24 hours)
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    lockouts = {k: v for k, v in lockouts.items() if v > cutoff}
    with open(LOCKOUT_FILE, "w") as f:
        json.dump(lockouts, f, indent=2)


def log_event(event):
    event["ts"] = datetime.now(timezone.utc).isoformat()
    logs = []
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE) as f:
                logs = json.load(f)
        except:
            logs = []
    logs.append(event)
    if len(logs) > 1000:
        logs = logs[-500:]
    with open(LOG_FILE, "w") as f:
        json.dump(logs, f, indent=2)


def add_notification(msg):
    """Queue notification for the agent to pick up."""
    notifs = []
    if os.path.exists(NOTIFY_FILE):
        try:
            with open(NOTIFY_FILE) as f:
                notifs = json.load(f)
        except:
            notifs = []
    notifs.append({"ts": datetime.now(timezone.utc).isoformat(), "msg": msg})
    with open(NOTIFY_FILE, "w") as f:
        json.dump(notifs, f, indent=2)


import fcntl

def load_trades():
    if not os.path.exists(TRADES_FILE):
        return {"trades": []}
    with open(TRADES_FILE) as f:
        fcntl.flock(f, fcntl.LOCK_SH)  # Shared lock for reading
        data = json.load(f)
        fcntl.flock(f, fcntl.LOCK_UN)
    return data


def save_trades(data):
    with open(TRADES_FILE, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)  # Exclusive lock for writing
        json.dump(data, f, indent=2)
        f.flush()
        fcntl.flock(f, fcntl.LOCK_UN)


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except:
            pass
    return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def parse_ticker(ticker, trade=None):
    parts = ticker.split("-")
    if len(parts) != 3:
        return None
    series = parts[0]
    date_str = parts[1]
    strike_str = parts[2]

    if strike_str.startswith("B"):
        strike_type = "between"
        strike_val = float(strike_str[1:])
    elif strike_str.startswith("T"):
        # Check trade metadata for strike_type, fall back to Kalshi API lookup
        strike_type = None
        if trade:
            strike_type = trade.get("strike_type")
        if not strike_type:
            try:
                import kalshi_client
                mkt = kalshi_client.get_market(ticker)
                strike_type = mkt.get("strike_type", "above")
            except:
                strike_type = "above"  # default for T-markets
        # Normalize: "greater" → "above", "less" → "below"
        if strike_type == "greater":
            strike_type = "above"
        elif strike_type == "less":
            strike_type = "below"
        strike_val = float(strike_str[1:])
    else:
        return None

    months = {"JAN":"01","FEB":"02","MAR":"03","APR":"04","MAY":"05","JUN":"06",
              "JUL":"07","AUG":"08","SEP":"09","OCT":"10","NOV":"11","DEC":"12"}
    year = "20" + date_str[:2]
    month = months.get(date_str[2:5], "01")
    day = date_str[5:]
    target_date = f"{year}-{month}-{day}"

    return {
        "series": series,
        "target_date": target_date,
        "strike_type": strike_type,
        "strike_val": strike_val,
        "ticker": ticker,
    }


def get_local_hour(tz_name):
    """Get current local hour for a timezone (approximate using UTC offset)."""
    offsets = {
        "America/New_York": -5,
        "America/Chicago": -6,
        "America/Denver": -7,
        "America/Los_Angeles": -8,
    }
    # During DST (roughly Mar-Nov), add 1
    now = datetime.now(timezone.utc)
    offset = offsets.get(tz_name, -5)
    # Simple DST check: March 9 - Nov 2 (approximate)
    if 3 <= now.month <= 10 or (now.month == 11 and now.day < 3):
        offset += 1
    local_hour = (now.hour + offset) % 24
    return local_hour


def compute_dynamic_danger_zone(local_hour, peak_hour, forecast_std):
    """
    Danger zone is 100% data-driven:
    - forecast_std: from live ensemble data, updates every run per city
    - peak_hour: from today's hourly forecast (shifts with seasons automatically)
    - local_hour: where we are in the day

    Returns danger zone in °F.
    """
    if forecast_std is None:
        forecast_std = 3.0

    hours_to_peak = peak_hour - local_hour

    if hours_to_peak <= 0:
        # Past peak. Daily max is locked in. Temps only drop from here.
        return 0.0
    elif hours_to_peak <= 1:
        # At peak - tiny buffer
        return forecast_std * 0.2
    elif hours_to_peak <= 3:
        # Approaching peak
        return forecast_std * (hours_to_peak * 0.25)
    else:
        # Morning - full uncertainty
        return forecast_std * 1.0


def get_current_temp(lat, lon):
    url = (f"https://api.open-meteo.com/v1/forecast?"
           f"latitude={lat}&longitude={lon}"
           f"&current=temperature_2m&temperature_unit=fahrenheit")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "KingClaw-PM/2.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        return data.get("current", {}).get("temperature_2m")
    except Exception as e:
        print(f"  ! Current temp fetch failed: {e}")
        return None


def get_todays_hourly(lat, lon, target_date, tz_name=None):
    """Fetch hourly forecast for target date. Returns (max_so_far, peak_hour, hourly_temps).

    Open-Meteo returns 24 hourly values indexed by UTC hour (default timezone=GMT).
    For US cities, early UTC hours (0-5) correspond to the previous LOCAL evening,
    so we filter to local daytime hours (8AM+) when computing max_so_far to avoid
    treating yesterday's evening temps as today's observed high.
    """
    url = (f"https://api.open-meteo.com/v1/forecast?"
           f"latitude={lat}&longitude={lon}"
           f"&hourly=temperature_2m&temperature_unit=fahrenheit"
           f"&start_date={target_date}&end_date={target_date}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "KingClaw-PM/2.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        temps = data.get("hourly", {}).get("temperature_2m", [])

        if not temps:
            return None, 15, []

        now_utc = datetime.now(timezone.utc)

        # Compute local daytime start (8AM local) in UTC terms
        utc_offset_hrs = 0
        if tz_name:
            try:
                from zoneinfo import ZoneInfo
                local_dt = now_utc.astimezone(ZoneInfo(tz_name))
                utc_offset_hrs = local_dt.utcoffset().total_seconds() / 3600
            except Exception:
                pass
        daytime_start_utc = int((8 - utc_offset_hrs) % 24)

        # Only use OBSERVED hours (up to current UTC hour) AND local daytime (8AM+)
        # This prevents yesterday's evening temps from inflating today's max_so_far
        observed_daytime = [temps[i] for i in range(min(now_utc.hour + 1, len(temps)))
                            if i >= daytime_start_utc and i < len(temps)
                            and temps[i] is not None]
        max_so_far = max(observed_daytime) if observed_daytime else None

        # Use hardcoded peak hour from city config (passed via caller), not forecast-derived
        # Forecast-derived peak can produce absurd values (e.g. 4AM) from noisy overnight data
        clean2 = [t for t in temps if t is not None]
        fallback_peak = temps.index(max(clean2)) if clean2 else 15

        return max_so_far, fallback_peak, temps
    except Exception as e:
        print(f"  ! Hourly fetch failed: {e}")
        return None, 15, []


def get_forecast_high(lat, lon, tz, target_date, cached_weights=None, city=None):
    """Get updated forecast high using batched ensemble. Returns (mean, std, forecasts_dict).
    Uses analyzer.collect_forecasts_batched() — same sources as fast_scanner.py."""
    try:
        weights = cached_weights
        if not weights:
            weights_data = analyzer.load_source_weights()
            weights = weights_data[0] if isinstance(weights_data, tuple) else weights_data
        
        # Use lightweight batched API calls (3 calls/city — Open-Meteo only, saves API quota)
        raw_forecasts = analyzer.collect_forecasts_batched(lat, lon, tz, target_date, lightweight=True)
        forecasts = {k: v for k, v in raw_forecasts.items() if not k.startswith("__")}
        
        # Filter out weight=0 sources for stats (but still log them)
        active_forecasts = {k: v for k, v in forecasts.items() if weights.get(k, 1.0) > 0}
        
        if not active_forecasts:
            return None, None, {}

        stats = analyzer.weighted_ensemble_stats(active_forecasts, weights, city=city)
        return stats.get("ensemble_mean"), stats.get("calibrated_std", 3.0), forecasts
    except Exception as e:
        print(f"  ! Forecast fetch failed: {e}")
        return None, None, {}


def get_market_price(ticker):
    try:
        mkt = kalshi_client.get_market(ticker)
        market_data = mkt.get("market", mkt)
        return {
            "yes_price": market_data.get("yes_bid") or market_data.get("last_price"),
            "no_price": market_data.get("no_bid"),
            "yes_ask": market_data.get("yes_ask"),
            "no_ask": market_data.get("no_ask"),
        }
    except Exception as e:
        print(f"  ! Market price fetch failed for {ticker}: {e}")
        return None


def _norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def calc_our_probability(strike, strike_type, forecast_high, forecast_std):
    """Calculate our probability that a given side wins.

    Kalshi bracket markets:
    - B82.5 → "Will temp be 82-83°?" YES = temp in [floor, cap], NO = temp outside that range
    - T85 → "Will temp be 85° or above?" YES = temp >= 85, NO = temp < 85

    Returns (yes_prob, no_prob).
    """
    if forecast_high is None or forecast_std is None:
        return None, None
    std = max(forecast_std, 1.0)

    if strike_type == "between":
        # Bracket market: YES = temp falls in 2°F bracket
        # e.g., B82.5 → bracket is [82, 83]. P(YES) = P(81.5 < temp < 83.5)
        # Continuity correction: NWS reports integer °F, so expand by ±0.5
        floor = strike - 1.0   # e.g. 82.5 - 1.0 = 81.5
        cap = strike + 1.0     # e.g. 82.5 + 1.0 = 83.5
        p_in_bracket = _norm_cdf((cap - forecast_high) / std) - _norm_cdf((floor - forecast_high) / std)
        p_in_bracket = max(0.0, min(1.0, p_in_bracket))
        return p_in_bracket, 1.0 - p_in_bracket  # (yes_prob, no_prob)
    elif strike_type == "below":  # T-type "less" — e.g. "60° or below"
        # YES = temp <= (strike-1), NO = temp >= strike
        # Continuity correction: boundary at strike - 0.5
        prob_below = _norm_cdf(((strike - 0.5) - forecast_high) / std)
        return prob_below, 1 - prob_below
    else:  # "above" (T-type) — e.g. "69° or above"
        # YES = temp >= (strike+1), NO = temp <= strike
        # Continuity correction: boundary at strike + 0.5
        prob_above = 1 - _norm_cdf(((strike + 0.5) - forecast_high) / std)
        return prob_above, 1 - prob_above


def evaluate_position(trade, current_temp, max_so_far, forecast_high, forecast_std, peak_hour, local_hour, prev_forecast, is_today=True):
    """
    Exit decisions are based ONLY on data shifts - not static probability cutoffs.

    Core principle: We entered because the data said X. If the data still says X, hold.
    Only react when the data changes against us, proportionally to how much it shifted.

    Returns: (action, reason, details)
      action: "hold" | "exit_blown" | "graduated_exit"
    """
    parsed = parse_ticker(trade["ticker"], trade=trade)
    if not parsed:
        return "hold", "unparseable ticker", {}

    strike = parsed["strike_val"]
    strike_type = parsed["strike_type"]
    direction = trade["direction"].upper()
    entry_price = trade["entry_price_cents"]
    contracts = trade["contracts"]

    danger_zone = compute_dynamic_danger_zone(local_hour, peak_hour, forecast_std)
    # FIX 3: Don't default to 0 — use None to indicate missing data
    temp_candidates = [t for t in [max_so_far, current_temp] if t is not None]
    effective_high = max(temp_candidates) if temp_candidates else None

    # What did the data say when we entered?
    entry_forecast = trade.get("entry_forecast_high")
    entry_std = trade.get("entry_forecast_std")
    entry_prob = trade.get("our_prob")

    # Current probability
    current_prob = None
    if forecast_high is not None and forecast_std is not None:
        yes_p, no_p = calc_our_probability(strike, strike_type, forecast_high, forecast_std)
        current_prob = no_p if direction == "NO" else yes_p

    details = {
        "strike": strike,
        "direction": direction,
        "current_temp": current_temp,
        "max_so_far": max_so_far,
        "forecast_high": forecast_high,
        "forecast_std": forecast_std,
        "danger_zone": round(danger_zone, 1),
        "local_hour": local_hour,
        "effective_high": effective_high,
        "entry_forecast": entry_forecast,
        "entry_prob": entry_prob,
        "current_prob": current_prob,
    }

    # ═══════════════════════════════════════════════════════════════
    # RULE 1: BLOWN - Actual temp already killed our position
    # This is the only hard exit. The market has settled against us.
    # ONLY applies to TODAY's positions — future dates have no observed temps yet.
    # ═══════════════════════════════════════════════════════════════

    past_peak = local_hour > peak_hour + 1

    # FIX 3: Only run blown checks if we have REAL temperature data (effective_high is not None)
    have_real_temps = effective_high is not None
    
    if is_today and have_real_temps:
        # Blown checks use integer boundaries since NWS settles on whole °F
        # B82.5 = "82 to 83", T68 greater = "69 or above"
        if direction == "NO" and strike_type == "between":
            # NO bracket only blown if PAST PEAK and daily max settled in the bracket
            # Before peak, temp could still rise above bracket (making our NO win)
            bracket_low = int(strike - 0.5)   # e.g. 82.5 → 82
            bracket_high = int(strike + 0.5)   # e.g. 82.5 → 83
            if past_peak and bracket_low <= effective_high <= bracket_high:
                return "exit_blown", f"Past peak, temp {effective_high}°F landed IN bracket [{bracket_low}-{bracket_high}°F]", details
        elif direction == "NO" and strike_type == "above":
            threshold = int(strike + 1)  # T68 greater → temp >= 69 triggers blown
            if effective_high >= threshold:
                return "exit_blown", f"Temp {effective_high}°F hit/exceeded {threshold}°F", details
        elif direction == "NO" and strike_type == "below":
            # NO on "60° or below" = betting temp >= 61. Blown if past peak and max <= strike-1
            threshold = int(strike - 1)  # T61 less → temp <= 60 triggers blown for NO
            if past_peak and effective_high <= threshold:
                return "exit_blown", f"Past peak ({peak_hour}:00), max only {effective_high}°F ≤ {threshold}°F — below market settled YES", details
        elif direction == "YES" and strike_type == "above":
            threshold = int(strike + 1)  # Need temp >= this
            if past_peak and effective_high < threshold - 1:
                return "exit_blown", f"Past peak ({peak_hour}:00), max only {effective_high}°F vs {threshold}°F needed", details
        elif direction == "YES" and strike_type == "below":
            # YES on "60° or below" = betting temp <= 60. Blown if temp already >= strike
            threshold = int(strike)  # If temp hits strike or above, can't go back down for daily high
            if effective_high >= threshold:
                return "exit_blown", f"Temp {effective_high}°F already ≥ {threshold}°F — below market YES blown", details
        elif direction == "YES" and strike_type == "between":
            bracket_low = int(strike - 0.5)
            bracket_high = int(strike + 0.5)
            if past_peak and (effective_high < bracket_low - 1 or effective_high > bracket_high + 1):
                return "exit_blown", f"Past peak, temp {effective_high}°F missed bracket [{bracket_low}-{bracket_high}°F]", details
    elif is_today and not have_real_temps:
        return "hold", "No temperature data available (API rate limited) — skipping blown check", details

    # ═══════════════════════════════════════════════════════════════
    # RULE 2: FORECAST SHIFT - Data changed since we entered
    # The ONLY reason to exit before settlement (besides blown).
    # How much we sell = how much the data shifted against us.
    # ═══════════════════════════════════════════════════════════════

    if forecast_high is None or forecast_std is None:
        return "hold", "No forecast data - holding", details

    # Calculate how much the forecast shifted against our position
    # Compare against ENTRY forecast if available, otherwise last check
    reference_forecast = entry_forecast or prev_forecast

    if reference_forecast is None:
        # First run, no reference. Just store and hold.
        details["current_prob"] = current_prob
        return "hold", "First check - establishing baseline", details

    forecast_shift = forecast_high - reference_forecast  # positive = warmer

    # Is this shift BAD for us?
    shift_hurts = False
    if direction == "NO" and strike_type == "below":
        # NO on "below" = betting warm. Cooling hurts.
        shift_hurts = forecast_shift < 0
    elif direction == "NO":
        # NO on "above" or bracket = betting cold/outside. Warming hurts.
        shift_hurts = forecast_shift > 0
    elif direction == "YES" and strike_type == "below":
        # YES on "below" = betting cold. Warming hurts.
        shift_hurts = forecast_shift > 0
    elif direction == "YES" and strike_type == "above":
        # YES on "above" = betting warm. Cooling hurts.
        shift_hurts = forecast_shift < 0
    else:
        # Bracket YES/NO — shift away from bracket center hurts
        shift_hurts = abs(forecast_high - strike) > abs(reference_forecast - strike)

    if not shift_hurts:
        details["current_prob"] = current_prob
        details["forecast_shift"] = round(forecast_shift, 1)
        
        # Reset adverse counters when forecast moves in our favor
        trade["pm_adverse_count"] = 0
        trade["pm_severity_history"] = []
        
        # === FORECAST RECOVERY DETECTION ===
        # If we previously sold some contracts (pm_total_sold_pct > 0) and forecast 
        # is now shifting BACK in our favor, track consecutive recovery scans.
        # After 2+ consecutive favorable shifts, reset severity tracking so 
        # the scanner can add back to this position.
        already_sold_pct = trade.get("pm_total_sold_pct", 0)
        if already_sold_pct > 0:
            recovery_count = trade.get("pm_recovery_count", 0) + 1
            trade["pm_recovery_count"] = recovery_count
            
            if recovery_count >= 2:
                # 2+ consecutive scans showing recovery — reset severity tracking
                old_severity = trade.get("pm_last_severity", 0)
                trade["pm_last_severity"] = 0  # Reset so cascade can re-trigger if needed
                # Cancel any outstanding resting sell orders for this position
                resting = trade.get("resting_orders", [])
                if resting:
                    for ro in resting:
                        try:
                            kalshi_client.cancel_order(ro["order_id"])
                            print(f"    🔄 Canceled sell order {ro['order_id']} — forecast recovered")
                        except:
                            pass
                    trade["resting_orders"] = []
                    trade["resting_count"] = 0
                details["recovery"] = True
                details["recovery_count"] = recovery_count
                return "hold", (
                    f"Forecast RECOVERING {forecast_shift:+.1f}°F ({recovery_count} consecutive) — "
                    f"reset severity tracking (was {old_severity:.2f}), "
                    f"{already_sold_pct:.0%} already sold, scanner can re-add"
                ), details
            else:
                return "hold", (
                    f"Forecast shift {forecast_shift:+.1f}°F is in our favor "
                    f"(recovery #{recovery_count}, need 2+ to reset)"
                ), details
        
        return "hold", f"Forecast shift {forecast_shift:+.1f}°F is in our favor", details

    # Shift is against us — reset recovery counter
    trade["pm_recovery_count"] = 0
    
    abs_shift = abs(forecast_shift)
    shift_severity = abs_shift / max(forecast_std, 0.5)

    # Calculate probability drop from ENTRY (not from last check)
    prob_drop = 0
    if entry_prob and current_prob is not None:
        prob_drop = entry_prob - current_prob
    elif current_prob is not None and reference_forecast is not None:
        prev_yes, prev_no = calc_our_probability(strike, strike_type, reference_forecast, entry_std or forecast_std)
        prev_prob = prev_no if direction == "NO" else prev_yes
        if prev_prob:
            prob_drop = prev_prob - current_prob

    details["forecast_shift"] = round(forecast_shift, 1)
    details["shift_severity"] = round(shift_severity, 2)
    details["prob_drop"] = round(prob_drop, 3) if prob_drop else 0
    details["current_prob"] = current_prob

    # ── CONSECUTIVE SHIFT CONFIRMATION ──
    # One scan showing a bad shift could be a storm blip or model hiccup.
    # Require CONSECUTIVE scans confirming the shift before selling.
    #
    # pm_adverse_count: how many consecutive scans showed shift against us
    # pm_adverse_severity_sum: running sum of severities for averaging
    #
    # Rules (PM runs every 10 min):
    #   severity < 1.0 std  → need 2 consecutive confirmations (20 min)
    #   severity 1.0-2.0    → need 2 consecutive confirmations (20 min)
    #   severity > 2.0 std  → sell immediately (emergency, forecast cratered)
    
    adverse_count = trade.get("pm_adverse_count", 0) + 1
    trade["pm_adverse_count"] = adverse_count
    
    # Track severity history to detect if it's sustained vs one-off
    severity_history = trade.get("pm_severity_history", [])
    severity_history.append(round(shift_severity, 2))
    if len(severity_history) > 5:
        severity_history = severity_history[-5:]
    trade["pm_severity_history"] = severity_history
    
    # How many consecutive confirmations do we need?
    if shift_severity > 2.0:
        required_confirmations = 1  # Emergency — sell now
    elif shift_severity >= 1.0:
        required_confirmations = 2  # Significant shift — 2 scans (20 min)
    else:
        required_confirmations = 3  # Moderate shift — 3 scans (30 min)
    
    if adverse_count < required_confirmations:
        return "hold", (
            f"Shift {forecast_shift:+.1f}°F (severity {shift_severity:.2f}) — "
            f"confirmation {adverse_count}/{required_confirmations}, holding"
        ), details

    # ── ANTI-CASCADE: Check what we already reacted to ──
    last_reacted_severity = trade.get("pm_last_severity", 0)
    severity_delta = shift_severity - last_reacted_severity

    # Require minimum 0.3 std severity INCREASE beyond last sell
    # Delta required between consecutive sells
    # Consecutive confirmations handle noise filtering, this just ensures
    # each sell requires meaningfully worse conditions than the last
    MIN_SEVERITY_DELTA = 0.20
    if last_reacted_severity > 0 and (shift_severity <= last_reacted_severity or severity_delta < MIN_SEVERITY_DELTA):
        return "hold", (
            f"Shift {forecast_shift:+.1f}°F (severity {shift_severity:.2f}) — "
            f"confirmed {adverse_count}x but already reacted at {last_reacted_severity:.2f}, "
            f"need +{MIN_SEVERITY_DELTA} delta to sell more"
        ), details

    # Shift IS confirmed AND worse than what we last reacted to.
    # noise threshold
    if shift_severity < 0.5:
        return "hold", f"Shift {forecast_shift:+.1f}°F is noise (severity {shift_severity:.1f}x std)", details

    if shift_severity < 0.75 and prob_drop < 0.10:
        return "hold", f"Minor shift {forecast_shift:+.1f}°F, prob only dropped {prob_drop:.0%}", details

    # ── CONTINUOUS EXIT FORMULA ──
    # Maps shift_severity to exit percentage:
    #   severity 0.5  → ~10% exit
    #   severity 1.0  → ~33% exit
    #   severity 1.5  → ~67% exit
    #   severity 2.0+ → 100% exit
    #
    # Formula: exit_pct = min(1.0, (severity - 0.5) / 1.5)

    raw_exit_pct = min(1.0, max(0.0, (shift_severity - 0.5) / 1.5))

    # But we also factor in prob_drop - if probability cratered, be more aggressive
    if prob_drop > 0.25:
        raw_exit_pct = max(raw_exit_pct, 0.80)
    elif prob_drop > 0.15:
        raw_exit_pct = max(raw_exit_pct, raw_exit_pct + 0.15)

    raw_exit_pct = min(1.0, raw_exit_pct)

    # ── INCREMENTAL: Only sell the ADDITIONAL amount beyond what we already sold ──
    # If we already sold 30% last time and now formula says 50%,
    # we only sell the incremental 20% of ORIGINAL contracts.
    #
    # trade["pm_original_contracts"] = contracts at first entry
    # trade["pm_total_sold_pct"] = cumulative % already sold

    already_sold_pct = trade.get("pm_total_sold_pct", 0)
    incremental_pct = raw_exit_pct - already_sold_pct

    if incremental_pct < 0.05:
        # Less than 5% incremental - not worth the trade fees
        return "hold", (
            f"Shift worsened slightly (severity {shift_severity:.2f}) but "
            f"incremental sell only {incremental_pct:.0%} - holding"
        ), details

    # Calculate contracts to sell from CURRENT remaining
    original_contracts = trade.get("pm_original_contracts", contracts)
    contracts_to_sell = max(1, int(original_contracts * incremental_pct))
    contracts_to_sell = min(contracts_to_sell, contracts)  # Can't sell more than we have

    details["exit_pct"] = round(raw_exit_pct, 3)
    details["incremental_pct"] = round(incremental_pct, 3)
    details["contracts_to_sell"] = contracts_to_sell
    details["shift_severity_for_tracking"] = shift_severity

    return "graduated_exit", (
        f"Forecast shifted {forecast_shift:+.1f}°F "
        f"({reference_forecast:.1f}→{forecast_high:.1f}°F), "
        f"severity {shift_severity:.2f}x std, prob drop {prob_drop:.0%} - "
        f"selling {contracts_to_sell}/{contracts} ({raw_exit_pct:.0%} total, {incremental_pct:.0%} incremental)"
    ), details


def exit_position(trade, reason, contracts_to_sell=None, use_resting=False):
    """Sell position. If contracts_to_sell is None, sell all.
    
    If use_resting=True, places a resting limit order instead of trying to fill immediately.
    Resting orders get better prices but may not fill. PM checks and cancels stale ones.
    
    Blown exits always use immediate (not resting) to get out ASAP.
    Graduated exits use resting orders — place at fair price, let market come to us.
    """
    ticker = trade["ticker"]
    direction = trade["direction"].lower()
    sell_count = contracts_to_sell or trade["contracts"]

    print(f"  🚨 EXIT: {ticker} ({direction} x{sell_count}) - {reason}")

    try:
        mkt_info = get_market_price(ticker)
        if not mkt_info:
            print(f"    ! No market price, skipping")
            return False

        if direction == "no":
            yes_p = mkt_info.get("yes_price") or 50
            raw_price = 100 - yes_p
        else:
            raw_price = mkt_info.get("yes_price") or 50

        if use_resting:
            # Resting order: place at current bid (no discount) — let market come to us
            sell_price = max(1, raw_price)
            print(f"    📋 Placing resting sell @ {sell_price}¢ (will manage hourly)")
        else:
            # Immediate: discount to fill now
            if raw_price > 20:
                discount = max(1, int(raw_price * 0.05))
            else:
                discount = 1
            sell_price = max(1, raw_price - discount)

        success, result, audit = kalshi_client.safe_sell_position(
            ticker=ticker,
            contract_side=direction,
            contracts=sell_count,
            price_cents=sell_price,
        )
        
        # Log audit trail
        log_event({"action": "safe_sell_audit", "ticker": ticker, **audit})
        
        if not success:
            print(f"    ✗ Safe sell rejected: {audit.get('error', 'unknown')}")
            return False

        order_data = result.get("order", {})
        order_id = order_data.get("order_id", "unknown")
        order_status = order_data.get("status", "unknown")
        filled = order_data.get("fill_count", 0)
        
        if use_resting and (order_status == "resting" or filled < sell_count):
            # Resting order placed — track it for management
            resting_orders = trade.get("resting_orders", [])
            resting_orders.append({
                "order_id": order_id,
                "price_cents": sell_price,
                "contracts": sell_count,
                "filled": filled,
                "placed_at": datetime.now(timezone.utc).isoformat(),
                "reason": reason,
            })
            trade["resting_orders"] = resting_orders
            trade["resting_count"] = len(resting_orders)
            
            if filled > 0:
                print(f"    📋 Partial fill {filled}/{sell_count}, rest resting @ {sell_price}¢")
            else:
                print(f"    📋 Resting order placed @ {sell_price}¢ — will check next run")
            
            log_event({
                "action": "resting_sell_placed",
                "ticker": ticker, "direction": direction,
                "contracts": sell_count, "filled": filled,
                "sell_price": sell_price, "order_id": order_id,
                "reason": reason,
            })
            return True  # Order placed (may not have filled yet)

        pnl_per_contract = sell_price - trade["entry_price_cents"]
        total_pnl = pnl_per_contract * sell_count

        log_event({
            "action": "exit",
            "ticker": ticker,
            "direction": direction,
            "contracts": sell_count,
            "sell_price": sell_price,
            "entry_price": trade["entry_price_cents"],
            "pnl_cents": total_pnl,
            "reason": reason,
        })

        emoji = "💰" if total_pnl > 0 else "🔻"
        print(f"    ✓ Sold @ {sell_price}¢ | P&L: {emoji} {total_pnl:+d}¢ (${total_pnl/100:+.2f})")
        return True

    except Exception as e:
        print(f"    ✗ Exit failed: {e}")
        log_event({"action": "exit_failed", "ticker": ticker, "error": str(e)})
        return False


def find_reentry(series, target_date, forecast_high, forecast_std, exited_ticker):
    """Find better strike to re-enter after exit.
    Respects: 2-per-city-per-date limit, 3°F overlap filter.
    Uses KALSHI positions as source of truth for counting."""
    meta = SERIES_META.get(series)
    if not meta or not forecast_high:
        return None

    city = meta["city"]
    print(f"  🔍 Scanning re-entry for {series} {target_date}...")

    # Convert target_date (YYYY-MM-DD) to ticker date format (e.g. 26FEB21)
    months_rev = {"01":"FEB","02":"FEB","03":"MAR","04":"APR","05":"MAY","06":"JUN",
                  "07":"JUL","08":"AUG","09":"SEP","10":"OCT","11":"NOV","12":"DEC"}
    months_rev["01"] = "JAN"
    months_rev["02"] = "FEB"
    td_parts = target_date.split("-")
    ticker_date_str = td_parts[0][2:] + months_rev.get(td_parts[1], "???") + td_parts[2]

    # Count from KALSHI (source of truth)
    open_for_city_date = []
    try:
        positions = kalshi_client.get_positions()
        for p in positions.get("market_positions", []):
            if p.get("position", 0) == 0:
                continue
            tk = p.get("ticker", "")
            tk_parts = tk.split("-")
            if len(tk_parts) != 3:
                continue
            tk_series = tk_parts[0]
            tk_date = tk_parts[1]
            tk_meta = SERIES_META.get(tk_series, {})
            if tk_meta.get("city") == city and tk_date == ticker_date_str:
                try:
                    open_for_city_date.append(float(tk_parts[2][1:]))
                except:
                    pass
    except:
        # Fallback to trades.json
        try:
            with open(TRADES_FILE) as f:
                trades_data = json.load(f)
            for t in trades_data.get("trades", []):
                if t.get("status") != "open":
                    continue
                t_parsed = parse_ticker(t.get("ticker", ""))
                t_series = t.get("ticker", "").split("-")[0]
                t_meta = SERIES_META.get(t_series, {})
                if t_meta.get("city") == city and t_parsed and t_parsed.get("target_date") == target_date:
                    open_for_city_date.append(t_parsed["strike_val"])
        except:
            pass

    MAX_PER_CITY_DATE = 2
    MIN_STRIKE_SEPARATION = 3.0

    if len(open_for_city_date) >= MAX_PER_CITY_DATE:
        print(f"    - Already {len(open_for_city_date)} positions for {city}/{target_date}, skip re-entry")
        return None

    try:
        markets = kalshi_client.get_markets(series_ticker=series, status="open")
        market_list = markets.get("markets", [])

        best = None
        best_edge = 0

        for mkt in market_list:
            ticker = mkt.get("ticker", "")
            if ticker == exited_ticker:
                continue

            parsed = parse_ticker(ticker)
            if not parsed or parsed["target_date"] != target_date:
                continue

            strike = parsed["strike_val"]

            # Check 3°F overlap with existing positions
            too_close = False
            for existing_strike in open_for_city_date:
                if abs(strike - existing_strike) < MIN_STRIKE_SEPARATION:
                    too_close = True
                    break
            if too_close:
                continue

            yes_prob, no_prob = calc_our_probability(strike, parsed["strike_type"], forecast_high, forecast_std)
            if yes_prob is None:
                continue

            # BLOWN CHECK: Skip positions that are physically impossible
            # Need current observed temp for today's markets
            strike_type = parsed["strike_type"]
            if strike_type == "below":
                # YES on "below" blown if any observed temp >= strike
                # (can't check here without temp data — rely on probability being near 0)
                if yes_prob < 0.02:
                    continue  # Effectively blown
            elif strike_type == "above":
                # NO on "above" blown if temp already >= strike+1
                if no_prob < 0.02:
                    continue  # Effectively blown

            yes_price_cents = mkt.get("yes_bid") or mkt.get("last_price") or 50
            market_yes = yes_price_cents / 100.0

            # Check NO edge
            no_edge = no_prob - (1 - market_yes)
            if no_edge > RE_ENTRY_MIN_EDGE and no_edge > best_edge:
                best_edge = no_edge
                best = {
                    "ticker": ticker,
                    "series": series,
                    "direction": "NO",
                    "edge": no_edge,
                    "our_prob": no_prob,
                    "market_price": 1 - market_yes,
                    "strike": strike,
                    "strike_type": parsed["strike_type"],
                }

            # Check YES edge
            yes_edge = yes_prob - market_yes
            if yes_edge > RE_ENTRY_MIN_EDGE and yes_edge > best_edge:
                best_edge = yes_edge
                best = {
                    "ticker": ticker,
                    "series": series,
                    "direction": "YES",
                    "edge": yes_edge,
                    "our_prob": yes_prob,
                    "market_price": market_yes,
                    "strike": strike,
                    "strike_type": parsed["strike_type"],
                }

        if best:
            print(f"    ✓ Best re-entry: {best['ticker']} {best['direction']} "
                  f"edge={best['edge']:.1%} (strike {best['strike']}°F)")
        else:
            print(f"    - No re-entry with >{RE_ENTRY_MIN_EDGE:.0%} edge and 3°F+ separation")

        return best

    except Exception as e:
        print(f"    ! Re-entry search failed: {e}")
        return None


RESERVE_CENTS = 5000  # $50 reserve - MUST match fast_scanner.py

def place_reentry(opportunity, available_capital_cents):
    """Place re-entry trade."""
    # HARD STOP: Never trade below reserve
    if available_capital_cents <= RESERVE_CENTS:
        print(f"  📥 SKIP RE-ENTRY: Cash ${available_capital_cents/100:.2f} <= ${RESERVE_CENTS/100:.2f} reserve")
        return False
    
    # IMPROVED: Check total portfolio exposure before re-entry
    try:
        bal = kalshi_client.get_balance()
        portfolio_value = bal.get("portfolio_value", available_capital_cents)
        current_exposure = portfolio_value - available_capital_cents
        max_exposure = int(portfolio_value * MAX_PORTFOLIO_EXPOSURE_PCT)
        
        if current_exposure >= max_exposure:
            print(f"  📥 SKIP RE-ENTRY: Portfolio exposure ${current_exposure/100:.2f}/${max_exposure/100:.2f} ({MAX_PORTFOLIO_EXPOSURE_PCT:.0%})")
            return False
    except Exception as e:
        print(f"  ⚠ Portfolio exposure check failed: {e}")
        # Continue with trade if check fails

    ticker = opportunity["ticker"]
    direction = opportunity["direction"].lower()
    market_price = opportunity["market_price"]

    entry_price_cents = max(1, min(99, int(market_price * 100)))

    # Size: 2-3% of tradeable capital (above reserve)
    tradeable = available_capital_cents - RESERVE_CENTS
    size_cents = int(tradeable * 0.025)
    size_cents = max(200, min(size_cents, 1000))  # $2-$10 max per re-entry
    contracts = max(1, size_cents // entry_price_cents)

    print(f"  📥 RE-ENTER: {ticker} {direction.upper()} x{contracts} @ {entry_price_cents}¢")

    try:
        result = kalshi_client.place_order(
            ticker=ticker,
            side=direction,
            contracts=contracts,
            price_cents=entry_price_cents,
        )

        trades_data = load_trades()
        trades_data["trades"].append({
            "ticker": ticker,
            "series": opportunity.get("series", ticker.split("-")[0]),
            "city": SERIES_META.get(ticker.split("-")[0], {}).get("city", "Unknown"),
            "direction": direction.upper(),
            "entry_price_cents": entry_price_cents,
            "contracts": contracts,
            "cost_cents": contracts * entry_price_cents,
            "fees_cents": 0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "open",
            "mode": "LIVE",
            "order_ids": [result.get("order", {}).get("order_id", "unknown")],
            "position_count": contracts,
            "market_exposure": contracts * entry_price_cents,
            "realized_pnl_cents": 0,
            "pnl_cents": None,
            "result": None,
            "resting_orders": [],
            "resting_count": 0,
            "source": "position_manager_reentry",
            "strike_type": opportunity.get("strike_type"),
            "edge_at_entry": round(opportunity["edge"], 4),
        })
        save_trades(trades_data)

        log_event({
            "action": "reentry",
            "ticker": ticker,
            "direction": direction,
            "contracts": contracts,
            "price": entry_price_cents,
            "edge": opportunity["edge"],
        })

        print(f"    ✓ Placed, cost ${contracts * entry_price_cents / 100:.2f}")
        
        trade_journal.log_action(
            action="HEDGE", ticker=ticker, direction=direction.upper(),
            contracts=contracts, price_cents=entry_price_cents,
            city=SERIES_META.get(opportunity.get("series", ticker.split("-")[0]), {}).get("city"),
            series=opportunity.get("series", ticker.split("-")[0]),
            reasoning=f"Hedge/re-entry after exit — edge {opportunity['edge']:.1%}, "
                      f"our prob {opportunity['our_prob']:.0%} vs market {opportunity['market_price']:.0%}",
            edge=opportunity["edge"], our_prob=opportunity["our_prob"],
            market_price=opportunity["market_price"],
        )
        
        return True

    except Exception as e:
        print(f"    ✗ Re-entry failed: {e}")
        return False


def run():
    start_time = time.time()
    now = datetime.now(timezone.utc)

    print("=" * 55)
    print("  KingClaw Position Manager v2 (Smart)")
    print(f"  {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 55)

    # Full sync: reconcile trades.json with Kalshi positions (source of truth)
    try:
        positions = kalshi_client.get_positions()
        kalshi_map = {}
        resting_count = 0
        for p in positions.get('market_positions', []):
            count = p.get('position', 0)
            if count != 0:
                kalshi_map[p['ticker']] = count  # signed: positive=YES, negative=NO
            resting_count += p.get('resting_orders_count', 0)
        
        trades_data_sync = load_trades()
        our_open_tickers = {t["ticker"] for t in trades_data_sync.get("trades", []) if t.get("status") == "open"}
        sync_fixes = 0
        
        # Fix 1: Update counts for known positions
        for t in trades_data_sync.get("trades", []):
            if t.get("status") != "open":
                continue
            ticker = t["ticker"]
            kalshi_count = kalshi_map.get(ticker)
            
            if kalshi_count is None or kalshi_count == 0:
                t["status"] = "exited"
                t["result"] = "unknown"
                sync_fixes += 1
            else:
                abs_count = abs(kalshi_count)
                # Update contract count to match Kalshi
                if abs_count != t.get("contracts", 0):
                    t["contracts"] = abs_count
                    t["position_count"] = abs_count
                    sync_fixes += 1
                # Always update market_exposure from Kalshi (source of truth for cap checks)
                kalshi_pos = next((p for p in positions.get("market_positions", [])
                                   if p["ticker"] == ticker), None)
                if kalshi_pos:
                    t["market_exposure"] = kalshi_pos.get("market_exposure", 0)
                    # CRITICAL: Sync direction from Kalshi (prevents position-flip bugs)
                    kalshi_dir = "YES" if kalshi_pos.get("position", 0) > 0 else "NO"
                    if t.get("direction") != kalshi_dir:
                        print(f"  🚨 Direction fix: {ticker} {t.get('direction')} → {kalshi_dir}")
                        t["direction"] = kalshi_dir
                        sync_fixes += 1
        
        # Fix 2: Add ghost positions (on Kalshi but not in trades.json)
        SERIES_TO_CITY_PM = {s: m["city"] for s, m in SERIES_META.items()}
        for ticker, count in kalshi_map.items():
            if ticker in our_open_tickers:
                continue
            parts = ticker.split("-")
            series = parts[0] if parts else ""
            city = SERIES_TO_CITY_PM.get(series, "Unknown")
            direction = "YES" if count > 0 else "NO"
            abs_count = abs(count)
            
            # Compute real avg entry price from Kalshi market_exposure
            kalshi_pos = next((p for p in positions.get("market_positions", [])
                              if p["ticker"] == ticker), None)
            if kalshi_pos and abs_count > 0:
                exposure = kalshi_pos.get("market_exposure", 0)
                avg_entry = max(1, round(exposure / abs_count))
            else:
                avg_entry = 50  # fallback estimate
            
            kalshi_exposure = kalshi_pos.get("market_exposure", avg_entry * abs_count) if kalshi_pos else avg_entry * abs_count
            trades_data_sync.setdefault("trades", []).append({
                "ticker": ticker,
                "series": series,
                "city": city,
                "direction": direction,
                "entry_price_cents": avg_entry,
                "contracts": abs_count,
                "cost_cents": avg_entry * abs_count,
                "market_exposure": kalshi_exposure,
                "fees_cents": 0,
                "timestamp": now.isoformat(),
                "status": "open",
                "mode": "LIVE",
                "order_ids": ["synced-from-kalshi"],
                "source": "kalshi_sync",
            })
            sync_fixes += 1
            print(f"  🔄 Synced ghost position: {ticker} {direction} x{abs_count} ({city})")
        
        if sync_fixes > 0:
            save_trades(trades_data_sync)
        
        bal = kalshi_client.get_balance()
        available_capital = bal.get("balance", 0)
        portfolio_value = bal.get("portfolio_value", 0)
        total_pnl = (available_capital + portfolio_value) - 51076  # from $510.76 start
        open_count = len([t for t in trades_data_sync['trades'] if t.get('status')=='open'])
        print(f"Synced {open_count} trades "
              f"({resting_count} resting) | P&L: ${total_pnl/100:.2f} | Balance: ${available_capital/100:.2f}")
        if sync_fixes:
            print(f"  🔄 {sync_fixes} sync fixes applied")
    except Exception as e:
        print(f"  ! Kalshi sync failed: {e}")
        available_capital = 0
        portfolio_value = 0

    # === RESTING ORDER MANAGEMENT ===
    # 1. Check for filled resting orders and update trades
    # 2. Cancel stale orders older than 1 hour (tighter than before — if not filled in 1hr, reprice)
    # 3. If forecast recovered, cancel sell orders (data no longer supports exit)
    STALE_ORDER_HOURS = 1.0
    try:
        all_orders = kalshi_client.get_orders(status="resting")
        stale_canceled = 0
        for order in all_orders.get("orders", []):
            created = order.get("created_time", "")
            if created:
                try:
                    created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                    age_hours = (datetime.now(timezone.utc) - created_dt).total_seconds() / 3600
                    if age_hours > STALE_ORDER_HOURS:
                        kalshi_client.cancel_order(order["order_id"])
                        print(f"  🗑️ Canceled stale order {order.get('ticker','')} ({age_hours:.1f}h old)")
                        stale_canceled += 1
                except Exception:
                    pass
        if stale_canceled:
            print(f"  Cleaned up {stale_canceled} stale resting orders (>{STALE_ORDER_HOURS}h)")
    except Exception as e:
        print(f"  ! Stale order cleanup failed: {e}")
    
    # Clean up resting_orders tracking in trades that got filled/canceled
    trades_data_pre = load_trades()
    for t in trades_data_pre.get("trades", []):
        if t.get("resting_orders"):
            # Remove tracking entries for orders that are no longer resting on Kalshi
            try:
                resting_ids = {o.get("order_id") for o in all_orders.get("orders", []) if o.get("order_id")}
            except:
                resting_ids = set()
            t["resting_orders"] = [r for r in t["resting_orders"] if r.get("order_id") in resting_ids]
            t["resting_count"] = len(t["resting_orders"])
    save_trades(trades_data_pre)

    state = load_state()
    trades_data = load_trades()
    open_trades = [t for t in trades_data.get("trades", []) if t.get("status") == "open"]

    # Track hedges placed this run to prevent multiple hedges per city/date
    hedges_placed_this_run = set()  # (city, target_date) tuples

    if not open_trades:
        print("\nNo open trades.")
        return

    # Balance already fetched in sync above
    print(f"\n💰 Cash: ${available_capital/100:.2f} | Portfolio: ${portfolio_value/100:.2f} | Total: ${(available_capital+portfolio_value)/100:.2f}")

    print(f"📊 {len(open_trades)} open positions\n")

    # Load weights once for entire run
    weights_data = analyzer.load_source_weights()
    cached_weights = weights_data[0] if isinstance(weights_data, tuple) else weights_data
    excluded = [s for s, w in cached_weights.items() if w == 0]
    if excluded:
        print(f"🚫 Excluded sources: {', '.join(excluded)}")
    
    # Group by series+date
    groups = defaultdict(list)
    for t in open_trades:
        parsed = parse_ticker(t["ticker"])
        if parsed:
            groups[(parsed["series"], parsed["target_date"])].append((t, parsed))

    actions_taken = []

    for (series, target_date), trade_list in groups.items():
        meta = SERIES_META.get(series)
        if not meta:
            continue

        city = meta["city"]
        lat, lon, tz = meta["lat"], meta["lon"], meta["tz"]

        today = now.strftime("%Y-%m-%d")
        is_today = target_date == today
        is_past = target_date < today

        print(f"{'─'*50}")
        print(f"📍 {city} - {target_date} {'(TODAY)' if is_today else '(past)' if is_past else '(future)'}")

        if is_past:
            print(f"  ⏰ Awaiting settlement")
            continue

        local_hour = get_local_hour(tz)

        # Fetch weather data
        current_temp = get_current_temp(lat, lon)

        # Get hourly forecast for peak hour detection + max so far
        # Default peak_hour from city config (safe fallback, not forecast-derived)
        config_peak_hour = meta.get("peak_hour", 15)
        if is_today:
            max_so_far, _forecast_peak, hourly = get_todays_hourly(lat, lon, target_date, tz_name=tz)
            peak_hour = config_peak_hour  # Use config default; peak_detector may override below
        else:
            max_so_far, peak_hour, hourly = None, config_peak_hour, []

        forecast_high, forecast_std, raw_forecasts = get_forecast_high(lat, lon, tz, target_date, cached_weights=cached_weights, city=city)

        # Data-driven peak detection (now that we have forecast_std)
        if is_today and hourly and len(hourly) == 24:
            import peak_detector
            from zoneinfo import ZoneInfo
            try:
                local_dt = now.astimezone(ZoneInfo(tz))
                utc_offset_hrs = local_dt.utcoffset().total_seconds() / 3600
            except:
                utc_offset_hrs = 0
            peak_info = peak_detector.detect_peak(hourly, now.hour, 
                                                   utc_offset_hours=utc_offset_hrs,
                                                   forecast_std=forecast_std)
            peak_hour = peak_info["peak_hour"]
            if peak_info["past_peak"]:
                obs_max_str = f"{peak_info['observed_max']:.1f}" if peak_info['observed_max'] else "?"
                print(f"  📉 Peak CONFIRMED ({peak_info['confidence']}): "
                      f"max {obs_max_str}°F at UTC hr {peak_hour}, "
                      f"rate={peak_info['rate_of_change']:+.1f}°F/hr, "
                      f"gap={peak_info.get('forecast_gap', '?')}°F from forecast, "
                      f"{peak_info['consecutive_declines']} consecutive declines")
            elif peak_info.get("confidence") == "possible_dip":
                print(f"  ⛈ Possible storm dip — obs max {peak_info['observed_max']:.1f}°F "
                      f"vs forecast {peak_info['forecast_max']:.1f}°F "
                      f"(gap {peak_info['forecast_gap']:.1f}°F > threshold {peak_info.get('gap_threshold', '?')}°F)")

        danger_zone = compute_dynamic_danger_zone(local_hour, peak_hour, forecast_std)

        # Get previous forecast from state for shift detection
        state_key = f"{series}_{target_date}"
        prev_forecast = state.get(state_key, {}).get("forecast_high")

        # Save current forecast to state
        if forecast_high is not None:
            state[state_key] = {
                "forecast_high": forecast_high,
                "forecast_std": forecast_std,
                "updated": now.isoformat(),
            }

        print(f"  🌡 Current: {current_temp}°F | Day max: {max_so_far}°F | Local: {local_hour}:00")
        print(f"  📈 Forecast: {forecast_high}°F ± {forecast_std}°F | Peak hour: {peak_hour}:00 | Danger zone: {danger_zone:.1f}°F")
        if prev_forecast:
            shift = (forecast_high or 0) - prev_forecast
            if abs(shift) > 0.3:
                print(f"  📊 Forecast shift: {shift:+.1f}°F since last check")

        for trade, parsed in trade_list:
            ticker = trade["ticker"]
            strike = parsed["strike_val"]
            direction = trade["direction"]
            contracts = trade["contracts"]
            entry = trade["entry_price_cents"]

            # Position-level P&L
            try:
                mkt = kalshi_client.get_market(ticker)
                if direction == "YES":
                    current_val = mkt.get("yes_bid", entry) or entry
                else:
                    current_val = mkt.get("no_bid", entry) or entry
                unrealized_pnl = (current_val - entry) * contracts
                pnl_str = f"{'+'if unrealized_pnl>=0 else ''}{unrealized_pnl/100:.2f}"
                print(f"\n  🎯 {ticker} - {direction} x{contracts} @ {entry}¢ → now {current_val}¢ (P&L: ${pnl_str})")
            except Exception:
                print(f"\n  🎯 {ticker} - {direction} x{contracts} @ {entry}¢")

            action, reason, details = evaluate_position(
                trade, current_temp, max_so_far,
                forecast_high, forecast_std,
                peak_hour, local_hour, prev_forecast,
                is_today=is_today
            )

            if action == "exit_blown":
                # Position already lost - full exit, no question
                print(f"  💀 BLOWN: {reason}")
                if exit_position(trade, reason):
                    trade["status"] = "exited_blown"
                    trade["exit_timestamp"] = datetime.now(timezone.utc).isoformat()
                    # FIX 2: Add lockout so scanner won't re-enter for 2 hours
                    add_lockout(ticker, series, target_date)
                    actions_taken.append(f"BLOWN EXIT {ticker}: {reason}")
                    
                    # === PHASE A: Blown exit logging (at trigger time) ===
                    # Phase B (settlement outcome) will be appended by settle_trades.py
                    blown_phase_a = {
                        "blown_trigger_temp": max_so_far,
                        "blown_peak_confirmed": details.get("past_peak", False),
                        "blown_exit_timestamp": datetime.now(timezone.utc).isoformat(),
                        "blown_forecast_at_exit": forecast_high,
                        "blown_forecast_std_at_exit": forecast_std,
                        "blown_entry_forecast": trade.get("entry_forecast_high"),
                        "blown_entry_price_cents": trade.get("entry_price_cents"),
                        "blown_contracts": contracts,
                        "blown_reason": reason,
                    }
                    trade["blown_phase_a"] = blown_phase_a
                    
                    trade_journal.log_action(
                        action="EXIT_BLOWN", ticker=ticker, direction=direction,
                        contracts=contracts, price_cents=trade["entry_price_cents"],
                        city=city, series=series,
                        reasoning=f"BLOWN — {reason}",
                        forecast_snapshot=raw_forecasts, ensemble_mean=forecast_high, ensemble_std=forecast_std,
                        entry_forecast=trade.get("entry_forecast_high"),
                        current_temp=current_temp, max_so_far=max_so_far,
                        extra={"phase_a": blown_phase_a},
                    )
                    try:
                        city_logger.log_position_action(city, "exit_blown", ticker, {
                            "reason": reason, "contracts": contracts,
                            "forecast_high": forecast_high, "forecast_std": forecast_std,
                            "phase_a": blown_phase_a,
                        })
                    except Exception:
                        pass

                    # RE-ENTRY after blown exit — config-gated (default OFF)
                    if REENTRY_ENABLED:
                        city_key = (meta["city"], target_date)
                        if city_key not in hedges_placed_this_run:
                            opp = find_reentry(series, target_date, forecast_high, forecast_std, ticker)
                            if opp and available_capital > RESERVE_CENTS:
                                if place_reentry(opp, available_capital):
                                    hedges_placed_this_run.add(city_key)
                                    actions_taken.append(f"RE-ENTER {opp['ticker']} {opp['direction']} edge={opp['edge']:.1%}")
                            time.sleep(1)

            elif action == "graduated_exit":
                # RESCUE MODE: If hold_to_settlement is enabled, skip graduated exits
                pm_cfg = _load_pm_config()
                if pm_cfg.get("hold_to_settlement", False):
                    print(f"  🛟 HOLD TO SETTLEMENT — skipping graduated exit: {reason}")
                    actions_taken.append(f"HELD (rescue mode) {ticker}")
                    continue
                
                contracts_to_sell = details.get("contracts_to_sell", max(1, int(contracts * details.get("exit_pct", 0.5))))
                contracts_to_sell = min(contracts_to_sell, contracts)
                remaining = contracts - contracts_to_sell

                print(f"  ⚡ GRADUATED EXIT: {reason}")
                print(f"     Selling {contracts_to_sell}/{contracts} contracts, keeping {remaining}")

                # Graduated exits use resting orders — place at fair price, let market come to us
                if exit_position(trade, reason, contracts_to_sell=contracts_to_sell, use_resting=True):
                    trade["contracts"] = remaining
                    if remaining <= 0:
                        trade["status"] = "exited_graduated"
                        # FIX 2: Add lockout so scanner won't re-enter for 2 hours
                        add_lockout(ticker, series, target_date)

                    # Track reaction for anti-cascade
                    if "pm_original_contracts" not in trade:
                        trade["pm_original_contracts"] = contracts
                    trade["pm_last_severity"] = details.get("shift_severity_for_tracking", details.get("shift_severity", 0))
                    trade["pm_total_sold_pct"] = details.get("exit_pct", 0)
                    # Reset adverse count — need fresh confirmations for next sell
                    trade["pm_adverse_count"] = 0
                    trade["pm_severity_history"] = []

                    actions_taken.append(
                        f"SELL {contracts_to_sell}/{contracts} {ticker} "
                        f"(severity={details.get('shift_severity',0):.2f}, "
                        f"total sold {details.get('exit_pct',0):.0%}, keeping {remaining})"
                    )
                    trade_journal.log_action(
                        action="EXIT_GRADUATED", ticker=ticker, direction=direction,
                        contracts=contracts_to_sell, price_cents=trade["entry_price_cents"],
                        city=city, series=series,
                        reasoning=reason,
                        forecast_snapshot=raw_forecasts, ensemble_mean=forecast_high, ensemble_std=forecast_std,
                        entry_forecast=trade.get("entry_forecast_high"),
                        forecast_shift=details.get("forecast_shift"),
                        severity=details.get("shift_severity"),
                        prob_drop=details.get("prob_drop"),
                        current_temp=current_temp, max_so_far=max_so_far,
                        position_context={
                            "original_contracts": trade.get("pm_original_contracts", contracts),
                            "remaining": remaining,
                            "total_sold_pct": details.get("exit_pct", 0),
                            "incremental_pct": details.get("incremental_pct", 0),
                        },
                    )

                    # HEDGE after graduated exit — config-gated (default OFF)
                    if HEDGE_ENABLED:
                        city_key = (meta["city"], target_date)
                        if city_key not in hedges_placed_this_run:
                            opp = find_reentry(series, target_date, forecast_high, forecast_std, ticker)
                            if opp and available_capital > RESERVE_CENTS:
                                if place_reentry(opp, available_capital):
                                    hedges_placed_this_run.add(city_key)
                                    actions_taken.append(f"HEDGE {opp['ticker']} {opp['direction']} edge={opp['edge']:.1%}")
                            time.sleep(1)
            else:
                # Check win probability for context
                if forecast_high and forecast_std:
                    yes_p, no_p = calc_our_probability(strike, parsed["strike_type"], forecast_high, forecast_std)
                    our_p = no_p if direction == "NO" else yes_p
                    if our_p:
                        print(f"  ✅ HOLD ({our_p:.0%} win prob) - {reason}")
                    else:
                        print(f"  ✅ HOLD - {reason}")
                else:
                    print(f"  ✅ HOLD - {reason}")

    save_trades(trades_data)
    save_state(state)

    elapsed = time.time() - start_time

    print(f"\n{'='*55}")
    print(f"  Done in {elapsed:.1f}s | Actions: {len(actions_taken)}")
    if actions_taken:
        for a in actions_taken:
            print(f"  → {a}")
    print(f"{'='*55}")

    # Log actions but don't notify — Tucker only wants the daily report
    if actions_taken:
        log_event({"action": "run_with_actions", "actions": actions_taken, "elapsed": elapsed})
    else:
        log_event({"action": "run_clean", "positions": len(open_trades), "elapsed": elapsed})


if __name__ == "__main__":
    run()
