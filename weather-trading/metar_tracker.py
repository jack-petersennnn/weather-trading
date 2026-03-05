#!/usr/bin/env python3
"""
METAR Intraday Tracker — Real-time observed temperature monitoring.

Polls NWS METAR stations every 5 minutes on settlement day.
Tracks observed daily high so far and determines if positions are
already won, lost, or trending a certain way.

Exports: get_intraday_status(city, target_date) for spike_monitor integration.

Station data comes from api.weather.gov/stations/{ICAO}/observations
which updates every ~5 minutes from ASOS sensors — the same sensors
that generate the daily high temp Kalshi settles on.
"""

import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from threading import Thread, Lock

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
METAR_CACHE_FILE = os.path.join(BASE_DIR, "metar_intraday.json")

# ICAO station codes — these are the NWS measurement stations Kalshi settles on
CITY_STATIONS = {
    "New York":      {"icao": "KNYC", "tz": "America/New_York"},      # Central Park (matches Kalshi ACIS settlement station)
    "Chicago":       {"icao": "KMDW", "tz": "America/Chicago"},       # Midway
    "Miami":         {"icao": "KMIA", "tz": "America/New_York"},      # Miami Intl
    "Denver":        {"icao": "KDEN", "tz": "America/Denver"},        # Denver Intl
    "Austin":        {"icao": "KAUS", "tz": "America/Chicago"},       # Bergstrom
    "Minneapolis":   {"icao": "KMSP", "tz": "America/Chicago"},      # MSP
    "Washington DC": {"icao": "KDCA", "tz": "America/New_York"},     # Reagan National
    "Atlanta":       {"icao": "KATL", "tz": "America/New_York"},     # Hartsfield
    "Philadelphia":  {"icao": "KPHL", "tz": "America/New_York"},     # PHL
    "Houston":       {"icao": "KHOU", "tz": "America/Chicago"},      # Hobby Airport (Kalshi CLIHOU)
    "Dallas":        {"icao": "KDFW", "tz": "America/Chicago"},      # DFW
    "Seattle":       {"icao": "KSEA", "tz": "America/Los_Angeles"},  # Sea-Tac
    "Boston":        {"icao": "KBOS", "tz": "America/New_York"},     # Logan
    "Phoenix":       {"icao": "KPHX", "tz": "America/Phoenix"},      # Sky Harbor
    "Oklahoma City": {"icao": "KOKC", "tz": "America/Chicago"},      # Will Rogers
    "Las Vegas":     {"icao": "KLAS", "tz": "America/Los_Angeles"},  # Harry Reid
    "San Francisco": {"icao": "KSFO", "tz": "America/Los_Angeles"}, # SFO
    "San Antonio":   {"icao": "KSAT", "tz": "America/Chicago"},      # SAT
    "New Orleans":   {"icao": "KMSY", "tz": "America/Chicago"},      # Louis Armstrong
}

# ── Timezone helpers (no pytz dependency) ──────────────────────────

def _is_dst(dt_utc, tz_name):
    """Determine if DST is active for a US timezone at a given UTC datetime.
    US DST: 2nd Sunday of March 2:00AM local → 1st Sunday of November 2:00AM local."""
    if tz_name == "America/Phoenix":
        return False  # Arizona never observes DST
    
    std_offsets = {
        "America/New_York": -5, "America/Chicago": -6,
        "America/Denver": -7, "America/Los_Angeles": -8,
    }
    std_offset = std_offsets.get(tz_name, -5)
    local_approx = dt_utc + timedelta(hours=std_offset)
    year = local_approx.year
    
    # 2nd Sunday of March
    mar1 = datetime(year, 3, 1)
    first_sun_mar = mar1 + timedelta(days=(6 - mar1.weekday()) % 7)
    dst_start = first_sun_mar + timedelta(days=7, hours=2)  # 2nd Sunday, 2AM local
    
    # 1st Sunday of November
    nov1 = datetime(year, 11, 1)
    first_sun_nov = nov1 + timedelta(days=(6 - nov1.weekday()) % 7)
    dst_end = first_sun_nov + timedelta(hours=2)  # 1st Sunday, 2AM local
    
    return dst_start <= local_approx < dst_end


def _utc_offset_hours(tz_name, dt_utc=None):
    """UTC offset for US timezones with proper DST support."""
    if dt_utc is None:
        dt_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    elif hasattr(dt_utc, 'tzinfo') and dt_utc.tzinfo:
        dt_utc = dt_utc.replace(tzinfo=None)
    
    std_offsets = {
        "America/New_York": -5, "America/Chicago": -6,
        "America/Denver": -7, "America/Los_Angeles": -8,
        "America/Phoenix": -7,
    }
    offset = std_offsets.get(tz_name, -5)
    if _is_dst(dt_utc, tz_name):
        offset += 1
    return offset


def _local_date_bounds_utc(target_date, tz_name):
    """Get UTC start/end of a local calendar day.
    Returns (start_utc, end_utc) as ISO strings."""
    base = datetime.strptime(target_date, "%Y-%m-%d")
    # Use midday of target date for DST check (avoids boundary issues)
    midday_utc = base + timedelta(hours=12)
    offset = _utc_offset_hours(tz_name, midday_utc)
    start_utc = base - timedelta(hours=offset)
    end_utc = start_utc + timedelta(hours=24)
    return start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"), end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def _local_hour_now(tz_name):
    """Current local hour (0-23) for a timezone."""
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    offset = _utc_offset_hours(tz_name, now_utc)
    local = now_utc + timedelta(hours=offset)
    return local.hour


# ── METAR Fetching ──────────────────────────────────────────────────

def _fetch_page(icao, start_utc, end_utc, limit=200):
    """Fetch a single page of observations from NWS API."""
    url = (f"https://api.weather.gov/stations/{icao}/observations"
           f"?start={start_utc}&end={end_utc}&limit={limit}")
    req = urllib.request.Request(url, headers={
        "User-Agent": "KingClaw-METAR/1.0",
        "Accept": "application/geo+json",
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"  ⚠ METAR fetch error for {icao}: {e}")
        return None


def fetch_observations(icao, start_utc, end_utc):
    """Fetch all observations from NWS API for a station within a time range.
    Follows pagination cursors if needed (NWS caps at 200 per request).
    Returns list of (timestamp_str, temp_f) tuples, sorted newest first."""
    results = []
    
    # First page
    data = _fetch_page(icao, start_utc, end_utc, limit=200)
    if not data:
        return results
    
    max_pages = 3  # Safety limit
    for page in range(max_pages):
        features = data.get("features", [])
        if not features:
            break
        
        out_of_range = False
        for f in features:
            props = f.get("properties", {})
            temp_c = props.get("temperature", {}).get("value")
            ts = props.get("timestamp")
            if temp_c is not None and ts:
                # Skip observations outside our requested range
                if ts < start_utc:
                    out_of_range = True
                    continue
                temp_f = temp_c * 9 / 5 + 32
                results.append((ts, round(temp_f, 1)))
        
        # Check for pagination cursor — stop if we've gone past our start time
        next_url = data.get("pagination", {}).get("next")
        if not next_url or len(features) < 200 or out_of_range:
            break
        
        # Fetch next page using cursor URL
        try:
            req = urllib.request.Request(next_url, headers={
                "User-Agent": "KingClaw-METAR/1.0",
                "Accept": "application/geo+json",
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
        except Exception as e:
            print(f"  ⚠ METAR pagination error for {icao}: {e}")
            break
    
    return results


# ── Intraday Status ─────────────────────────────────────────────────

_cache = {}
_cache_lock = Lock()
_cache_times = {}  # city|date -> last fetch timestamp

CACHE_TTL_SECONDS = 300  # Re-fetch every 5 minutes


def get_intraday_status(city, target_date):
    """Get real-time observed temperature status for a city on a given date.
    
    Returns dict:
        {
            "city": str,
            "date": str,
            "observed_high": float or None,  # Highest temp observed so far (°F)
            "current_temp": float or None,    # Most recent reading
            "obs_count": int,                 # Number of observations today
            "last_obs_time": str or None,     # Timestamp of most recent reading
            "local_hour": int,                # Current local hour (0-23)
            "is_settlement_day": bool,        # Is target_date today?
            "day_complete": bool,             # Is it past ~midnight local? (day is over)
            "confidence": str,                # "early", "midday", "afternoon", "final"
        }
    """
    cache_key = f"{city}|{target_date}"
    now = time.time()
    
    with _cache_lock:
        if cache_key in _cache and (now - _cache_times.get(cache_key, 0)) < CACHE_TTL_SECONDS:
            return _cache[cache_key]
    
    station = CITY_STATIONS.get(city)
    if not station:
        return None
    
    icao = station["icao"]
    tz_name = station["tz"]
    
    # Get UTC bounds for local calendar day
    start_utc, end_utc = _local_date_bounds_utc(target_date, tz_name)
    
    # Fetch observations
    obs = fetch_observations(icao, start_utc, end_utc)
    
    if not obs:
        result = {
            "city": city, "date": target_date,
            "observed_high": None, "current_temp": None,
            "obs_count": 0, "last_obs_time": None,
            "local_hour": _local_hour_now(tz_name),
            "is_settlement_day": target_date == datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "day_complete": False, "confidence": "no_data",
        }
    else:
        temps = [t for _, t in obs]
        observed_high = max(temps)
        current_temp = obs[0][1]  # Most recent
        local_hour = _local_hour_now(tz_name)
        
        # Determine how complete the day is
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        is_today = (target_date == today_str)
        
        # For past dates, the day is complete
        if not is_today:
            day_complete = True
            confidence = "final"
        else:
            day_complete = local_hour >= 23  # After 11PM local, high is basically set
            if local_hour < 10:
                confidence = "early"       # Morning, high hasn't peaked yet
            elif local_hour < 14:
                confidence = "midday"      # Approaching peak hours
            elif local_hour < 18:
                confidence = "afternoon"   # Peak hours, high is likely near final
            else:
                confidence = "evening"     # Past peak, high is probably set
        
        result = {
            "city": city, "date": target_date,
            "observed_high": observed_high, "current_temp": current_temp,
            "obs_count": len(obs), "last_obs_time": obs[0][0],
            "local_hour": local_hour,
            "is_settlement_day": is_today,
            "day_complete": day_complete,
            "confidence": confidence,
        }
    
    with _cache_lock:
        _cache[cache_key] = result
        _cache_times[cache_key] = now
    
    return result


def evaluate_position(city, target_date, direction, strike_str, strike_type=None):
    """Evaluate a position against observed data.
    
    Returns dict:
        {
            "verdict": str,        # "won", "lost", "likely_won", "likely_lost", "uncertain"
            "sell_pct": float,     # Recommended % of position to sell (0.0 - 1.0)
            "reason": str,         # Human-readable explanation
            "observed_high": float,
            "confidence": str,
        }
    """
    status = get_intraday_status(city, target_date)
    if not status or status["observed_high"] is None:
        return {"verdict": "uncertain", "sell_pct": 0.0, "reason": "No observation data",
                "observed_high": None, "confidence": "no_data"}
    
    observed_high = status["observed_high"]
    confidence = status["confidence"]
    local_hour = status["local_hour"]
    
    # Parse strike
    if strike_str.startswith("B"):
        strike = float(strike_str[1:])
        bracket_low = strike - 0.5
        bracket_high = strike + 0.5
        is_bracket = True
    elif strike_str.startswith("T"):
        strike = float(strike_str[1:])
        is_bracket = False
    else:
        return {"verdict": "uncertain", "sell_pct": 0.0, "reason": "Unknown strike format",
                "observed_high": None, "confidence": confidence}
    
    # ── Threshold (T) markets ──
    # Determine if this is "above" (greater) or "below" (less)
    is_below = strike_type in ("less", "below")
    
    if not is_bracket and is_below:
        # "Below" market: T18 "17° or below" → YES wins if daily high <= strike-1
        threshold = int(strike - 1)  # T18 → 17
        if direction == "YES":
            # YES on "below" = betting cold. Blown if temp already >= strike
            if observed_high >= strike:
                return {"verdict": "lost", "sell_pct": 1.0,
                        "reason": f"Observed high {observed_high}°F ≥ {strike}°F — below YES is impossible.",
                        "observed_high": observed_high, "confidence": confidence}
            else:
                gap = strike - observed_high
                if confidence in ("evening", "final") and gap >= 2:
                    return {"verdict": "likely_won", "sell_pct": 0.0,
                            "reason": f"Observed {observed_high}°F, need ≤{threshold}°F. Day ending, gap {gap:.1f}°F safe.",
                            "observed_high": observed_high, "confidence": confidence}
                return {"verdict": "uncertain", "sell_pct": 0.0,
                        "reason": f"Observed {observed_high}°F, need ≤{threshold}°F. Gap {gap:.1f}°F.",
                        "observed_high": observed_high, "confidence": confidence}
        else:  # NO on below = betting warm
            if observed_high >= strike:
                return {"verdict": "likely_won", "sell_pct": 0.0,
                        "reason": f"Observed {observed_high}°F ≥ {strike}°F — below YES blown, our NO wins.",
                        "observed_high": observed_high, "confidence": confidence}
            if confidence in ("evening", "final") and observed_high <= threshold:
                return {"verdict": "lost", "sell_pct": 0.90,
                        "reason": f"Observed {observed_high}°F ≤ {threshold}°F — below YES likely wins.",
                        "observed_high": observed_high, "confidence": confidence}
            return {"verdict": "uncertain", "sell_pct": 0.0,
                    "reason": f"Observed {observed_high}°F, below threshold {threshold}°F.",
                    "observed_high": observed_high, "confidence": confidence}
    
    elif not is_bracket:
        # "Above" market (default)
        if direction == "YES":
            # We bet YES on "above strike" — we win if observed high >= strike+1
            if observed_high >= strike + 1:
                # Already above! We've won (or very likely won if still early)
                if confidence in ("afternoon", "evening", "final"):
                    return {"verdict": "won", "sell_pct": 0.0,
                            "reason": f"Observed high {observed_high}°F already above {strike}°F. Hold to settlement.",
                            "observed_high": observed_high, "confidence": confidence}
                else:
                    return {"verdict": "likely_won", "sell_pct": 0.0,
                            "reason": f"Observed {observed_high}°F above {strike}°F and still climbing.",
                            "observed_high": observed_high, "confidence": confidence}
            else:
                gap = strike - observed_high
                # How likely to still reach strike?
                if confidence in ("evening", "final"):
                    # Day is basically over, we haven't hit strike
                    if gap <= 1.0:
                        return {"verdict": "likely_lost", "sell_pct": 0.70,
                                "reason": f"Observed high {observed_high}°F, need {strike}°F. Gap {gap:.1f}°F, day nearly over.",
                                "observed_high": observed_high, "confidence": confidence}
                    else:
                        return {"verdict": "lost", "sell_pct": 0.90,
                                "reason": f"Observed high {observed_high}°F, need {strike}°F. Gap {gap:.1f}°F, day is over.",
                                "observed_high": observed_high, "confidence": confidence}
                elif confidence == "afternoon":
                    if gap <= 2.0:
                        return {"verdict": "uncertain", "sell_pct": 0.30,
                                "reason": f"Observed {observed_high}°F, need {strike}°F. Gap {gap:.1f}°F, still afternoon.",
                                "observed_high": observed_high, "confidence": confidence}
                    elif gap <= 5.0:
                        return {"verdict": "likely_lost", "sell_pct": 0.60,
                                "reason": f"Observed {observed_high}°F, need {strike}°F. Gap {gap:.1f}°F, unlikely to catch up.",
                                "observed_high": observed_high, "confidence": confidence}
                    else:
                        return {"verdict": "lost", "sell_pct": 0.85,
                                "reason": f"Observed {observed_high}°F, need {strike}°F. Gap {gap:.1f}°F too large.",
                                "observed_high": observed_high, "confidence": confidence}
                else:
                    # Early/midday — too early to call
                    if gap > 10:
                        return {"verdict": "likely_lost", "sell_pct": 0.40,
                                "reason": f"Observed {observed_high}°F, need {strike}°F. Gap {gap:.1f}°F seems too large.",
                                "observed_high": observed_high, "confidence": confidence}
                    return {"verdict": "uncertain", "sell_pct": 0.0,
                            "reason": f"Observed {observed_high}°F, need {strike}°F. Gap {gap:.1f}°F but still early.",
                            "observed_high": observed_high, "confidence": confidence}
        
        else:  # direction == "NO"
            # We bet NO on "above strike" — we win if observed high <= strike (i.e. < strike+1)
            threshold = int(strike + 1)  # T68 → need temp < 69 for NO to win
            if observed_high >= threshold:
                # Already above threshold, we lost
                if confidence in ("afternoon", "evening", "final"):
                    return {"verdict": "lost", "sell_pct": 0.90,
                            "reason": f"Observed high {observed_high}°F ≥ {threshold}°F. Our NO bet lost.",
                            "observed_high": observed_high, "confidence": confidence}
                else:
                    return {"verdict": "likely_lost", "sell_pct": 0.70,
                            "reason": f"Observed {observed_high}°F ≥ {threshold}°F. Hard to undo.",
                            "observed_high": observed_high, "confidence": confidence}
            else:
                gap = strike - observed_high
                if confidence in ("evening", "final"):
                    # Day is over and we're still below — we won
                    return {"verdict": "won", "sell_pct": 0.0,
                            "reason": f"Observed high {observed_high}°F stayed below {strike}°F. Hold to settlement.",
                            "observed_high": observed_high, "confidence": confidence}
                elif confidence == "afternoon" and gap >= 3.0:
                    return {"verdict": "likely_won", "sell_pct": 0.0,
                            "reason": f"Observed {observed_high}°F, strike {strike}°F. Gap {gap:.1f}°F with peak passing.",
                            "observed_high": observed_high, "confidence": confidence}
                else:
                    return {"verdict": "uncertain", "sell_pct": 0.0,
                            "reason": f"Observed {observed_high}°F, strike {strike}°F. Gap {gap:.1f}°F, still evolving.",
                            "observed_high": observed_high, "confidence": confidence}
    
    # ── BRACKET (B) markets ──
    else:
        if direction == "YES":
            # We bet YES on "temp in bracket [bracket_low, bracket_high)"
            if observed_high >= bracket_high:
                # Already above bracket — we lost
                sell_pct = 0.90 if confidence in ("afternoon", "evening", "final") else 0.60
                return {"verdict": "lost" if confidence in ("afternoon", "evening", "final") else "likely_lost",
                        "sell_pct": sell_pct,
                        "reason": f"Observed high {observed_high}°F already above bracket ceiling {bracket_high}°F.",
                        "observed_high": observed_high, "confidence": confidence}
            elif observed_high >= bracket_low:
                # Currently in bracket — could win if it stays
                if confidence in ("evening", "final"):
                    return {"verdict": "won", "sell_pct": 0.0,
                            "reason": f"Observed high {observed_high}°F in bracket [{bracket_low}, {bracket_high}). Day ending.",
                            "observed_high": observed_high, "confidence": confidence}
                else:
                    return {"verdict": "uncertain", "sell_pct": 0.0,
                            "reason": f"Observed {observed_high}°F in bracket [{bracket_low}, {bracket_high}). Could still move.",
                            "observed_high": observed_high, "confidence": confidence}
            else:
                # Below bracket — need to reach it
                gap = bracket_low - observed_high
                if confidence in ("evening", "final"):
                    return {"verdict": "lost", "sell_pct": 0.85,
                            "reason": f"Observed high {observed_high}°F below bracket floor {bracket_low}°F. Day over.",
                            "observed_high": observed_high, "confidence": confidence}
                elif gap > 8:
                    return {"verdict": "likely_lost", "sell_pct": 0.50,
                            "reason": f"Observed {observed_high}°F, need {bracket_low}°F. Gap {gap:.1f}°F too large.",
                            "observed_high": observed_high, "confidence": confidence}
                else:
                    return {"verdict": "uncertain", "sell_pct": 0.0,
                            "reason": f"Observed {observed_high}°F, bracket [{bracket_low}, {bracket_high}). Gap {gap:.1f}°F, still early.",
                            "observed_high": observed_high, "confidence": confidence}
        
        else:  # direction == "NO" on bracket
            # We bet NO on bracket — we win if temp is NOT in bracket
            if observed_high >= bracket_high:
                # Above bracket — we win! (for NO)
                if confidence in ("afternoon", "evening", "final"):
                    return {"verdict": "won", "sell_pct": 0.0,
                            "reason": f"Observed high {observed_high}°F above bracket ceiling {bracket_high}°F. NO wins.",
                            "observed_high": observed_high, "confidence": confidence}
                else:
                    return {"verdict": "likely_won", "sell_pct": 0.0,
                            "reason": f"Observed {observed_high}°F above bracket. Likely stays above.",
                            "observed_high": observed_high, "confidence": confidence}
            elif observed_high >= bracket_low:
                # Currently in bracket — NO is currently losing
                if confidence in ("evening", "final"):
                    return {"verdict": "lost", "sell_pct": 0.85,
                            "reason": f"Observed high {observed_high}°F landed in bracket [{bracket_low}, {bracket_high}). NO loses.",
                            "observed_high": observed_high, "confidence": confidence}
                elif confidence == "afternoon":
                    # Could still break out above
                    margin_above = bracket_high - observed_high
                    if margin_above <= 1.5:
                        return {"verdict": "uncertain", "sell_pct": 0.20,
                                "reason": f"Observed {observed_high}°F in bracket, {margin_above:.1f}°F from ceiling. Could break out.",
                                "observed_high": observed_high, "confidence": confidence}
                    else:
                        return {"verdict": "likely_lost", "sell_pct": 0.50,
                                "reason": f"Observed {observed_high}°F sitting in bracket. {margin_above:.1f}°F to ceiling.",
                                "observed_high": observed_high, "confidence": confidence}
                else:
                    return {"verdict": "uncertain", "sell_pct": 0.0,
                            "reason": f"Observed {observed_high}°F in bracket but still early.",
                            "observed_high": observed_high, "confidence": confidence}
            else:
                # Below bracket — could go either way
                if confidence in ("evening", "final"):
                    return {"verdict": "won", "sell_pct": 0.0,
                            "reason": f"Observed high {observed_high}°F below bracket floor {bracket_low}°F. NO wins (below).",
                            "observed_high": observed_high, "confidence": confidence}
                else:
                    return {"verdict": "uncertain", "sell_pct": 0.0,
                            "reason": f"Observed {observed_high}°F below bracket. Still evolving.",
                            "observed_high": observed_high, "confidence": confidence}


# ── Standalone test ──────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    cities = sys.argv[1:] if len(sys.argv) > 1 else ["Phoenix", "Miami", "Seattle", "Las Vegas", "New Orleans"]
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    print(f"🌡️  METAR Intraday Tracker — {today}")
    print("=" * 60)
    
    for city in cities:
        status = get_intraday_status(city, today)
        if status:
            print(f"\n  {city} ({CITY_STATIONS[city]['icao']}):")
            print(f"    Observed high: {status['observed_high']}°F")
            print(f"    Current temp:  {status['current_temp']}°F")
            print(f"    Observations:  {status['obs_count']}")
            print(f"    Local hour:    {status['local_hour']}")
            print(f"    Confidence:    {status['confidence']}")
        else:
            print(f"\n  {city}: No data")
