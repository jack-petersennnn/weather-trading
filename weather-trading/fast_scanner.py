#!/usr/bin/env python3
"""
Fast Scanner — Lightweight pipeline using ONLY unlimited/high-limit sources.
Runs every 20 min (19 cities × 6 sources = 114 calls/scan). Skips Tomorrow.io and Visual Crossing.

Uses: NWS (unlimited), Open-Meteo ECMWF/GFS/BestMatch/Ensembles (10k/day)
That's 8 out of 10 sources — still a strong ensemble.

If it finds a new opportunity with strong edge, places the trade immediately.
"""

import json
import os
import sys
import time
import math
import fcntl
import statistics
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kalshi_client

# Import specific source functions from analyzer (skip rate-limited ones)
import analyzer
import trade_journal
import hypothetical_tracker
import training_logger
import city_logger
import forecast_logger
import circuit_breaker
import edge_calibration
import slippage_tracker

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRADES_FILE = os.path.join(BASE_DIR, "trades.json")
ACTIVE_MARKETS = os.path.join(BASE_DIR, "active-markets.json")
NOTIFY_FILE = os.path.join(BASE_DIR, "pm_notifications.json")

SERIES_META = {
    # Coordinates = NWS official measurement stations (what Kalshi settles on)
    "KXHIGHNY":  {"city": "New York",     "lat": 40.7789, "lon": -73.9692, "tz": "America/New_York"},      # Central Park
    "KXHIGHCHI": {"city": "Chicago",      "lat": 41.7868, "lon": -87.7522, "tz": "America/Chicago"},       # Midway (KMDW), NOT O'Hare!
    "KXHIGHMIA": {"city": "Miami",        "lat": 25.7959, "lon": -80.2870, "tz": "America/New_York"},      # Miami Intl (KMIA)
    "KXHIGHDEN": {"city": "Denver",       "lat": 39.8561, "lon": -104.6737, "tz": "America/Denver"},       # Denver Intl (KDEN)
    "KXHIGHAUS": {"city": "Austin",       "lat": 30.1945, "lon": -97.6699, "tz": "America/Chicago"},       # Bergstrom (KAUS)
    # LA excluded — backtest shows 6-8°F MAE across most sources
    # "KXHIGHLAX": {"city": "Los Angeles",  "lat": 33.9425, "lon": -118.4081, "tz": "America/Los_Angeles"}, # LAX (KLAX)

    # Extended cities — all using NWS official station coords
    "KXHIGHTMIN": {"city": "Minneapolis",    "lat": 44.8831, "lon": -93.2289, "tz": "America/Chicago"},     # MSP (KMSP)
    "KXHIGHTDC":  {"city": "Washington DC",  "lat": 38.8512, "lon": -77.0402, "tz": "America/New_York"},    # Reagan National (KDCA)
    "KXHIGHTATL": {"city": "Atlanta",        "lat": 33.6407, "lon": -84.4277, "tz": "America/New_York"},    # Hartsfield (KATL)
    "KXHIGHPHIL": {"city": "Philadelphia",   "lat": 39.8721, "lon": -75.2411, "tz": "America/New_York"},    # PHL (KPHL)
    "KXHIGHTHOU": {"city": "Houston",        "lat": 29.6454, "lon": -95.2789, "tz": "America/Chicago"},     # Hobby Airport (KHOU) — Kalshi uses CLIHOU
    "KXHIGHTDAL": {"city": "Dallas",         "lat": 32.8998, "lon": -97.0403, "tz": "America/Chicago"},     # DFW (KDFW)
    "KXHIGHTSEA": {"city": "Seattle",        "lat": 47.4502, "lon": -122.3088, "tz": "America/Los_Angeles"},# Sea-Tac (KSEA)
    "KXHIGHTBOS": {"city": "Boston",         "lat": 42.3656, "lon": -71.0096, "tz": "America/New_York"},    # Logan (KBOS)
    "KXHIGHTPHX": {"city": "Phoenix",        "lat": 33.4373, "lon": -112.0078, "tz": "America/Phoenix"},    # Sky Harbor (KPHX)
    "KXHIGHTOKC": {"city": "Oklahoma City",  "lat": 35.3931, "lon": -97.6007, "tz": "America/Chicago"},     # Will Rogers (KOKC)
    "KXHIGHTLV":  {"city": "Las Vegas",      "lat": 36.0840, "lon": -115.1537, "tz": "America/Los_Angeles"},# Harry Reid (KLAS)
    "KXHIGHTSFO": {"city": "San Francisco",  "lat": 37.6213, "lon": -122.3790, "tz": "America/Los_Angeles"},# SFO (KSFO)
    "KXHIGHTSATX":{"city": "San Antonio",    "lat": 29.5337, "lon": -98.4698, "tz": "America/Chicago"},     # SAT (KSAT)
    "KXHIGHTNOLA":{"city": "New Orleans",    "lat": 29.9934, "lon": -90.2580, "tz": "America/Chicago"},     # Louis Armstrong (KMSY)
}

# Only unlimited/high-limit sources
# "Best Match" removed — it's a GFS duplicate (Open-Meteo best_match defaults to GFS for US locations)
FAST_SOURCES = [
    ("NWS Forecast", analyzer.source_nws_forecast),
    ("NWS Hourly", analyzer.source_nws_hourly),
    ("ECMWF", analyzer.source_ecmwf),
    ("GFS", analyzer.source_gfs),
    ("ICON", analyzer.source_icon),
    ("Ensemble ICON", analyzer.source_ensemble_icon),
    ("Ensemble GFS", analyzer.source_ensemble_gfs),
    ("Ensemble ECMWF", analyzer.source_ensemble_ecmwf),
    ("Tomorrow.io", analyzer.source_tomorrow_io),
    ("Visual Crossing", analyzer.source_visual_crossing),
    # New training sources (weight=0, data collected but not used for trading yet)
    ("HRRR", analyzer.source_hrrr),
    ("Canadian GEM", analyzer.source_canadian_gem),
    ("JMA", analyzer.source_jma),
    ("UKMO", analyzer.source_ukmo),
    ("Meteo-France Arpege", analyzer.source_meteo_france_arpege),
    ("MET Norway", analyzer.source_met_norway),
]

# Independent model families — used to count truly independent data sources
MODEL_FAMILIES = {
    "ECMWF": "ecmwf",
    "Ensemble ECMWF": "ecmwf",
    "GFS": "gfs",
    "Ensemble GFS": "gfs",
    "HRRR": "hrrr",  # NOAA but different model from GFS
    "ICON": "icon",
    "Ensemble ICON": "icon",
    "Canadian GEM": "gem",
    "JMA": "jma",
    "UKMO": "ukmo",
    "Meteo-France Arpege": "arpege",
    "MET Norway": "metno",
    "NWS Forecast": "nws",
    "NWS Hourly": "nws",
    "Tomorrow.io": "tomorrow",
    "Visual Crossing": "visualcrossing",
}

# Load config from centralized file
def _load_trading_config():
    cfg_path = os.path.join(BASE_DIR, "trading_config.json")
    try:
        with open(cfg_path) as f:
            return json.load(f).get("scanner", {})
    except:
        return {}

_CFG = _load_trading_config()

# Risk parameters
def _load_risk_config():
    cfg_path = os.path.join(BASE_DIR, "trading_config.json")
    try:
        with open(cfg_path) as f:
            return json.load(f).get("risk", {})
    except:
        return {}

_RISK = _load_risk_config()
KELLY_MAX = _RISK.get("kelly_fraction_max", 0.25)

def _load_pm_config():
    cfg_path = os.path.join(BASE_DIR, "trading_config.json")
    try:
        with open(cfg_path) as f:
            return json.load(f).get("position_manager", {})
    except:
        return {}
_PM_CFG = _load_pm_config()

MIN_EDGE = _CFG.get("min_edge_threshold", 0.10)  # IMPROVED: Increased from 8% to 10%
MIN_EDGE_BRACKET = _CFG.get("min_edge_bracket", 0.15)  # IMPROVED: Increased from 12% to 15%

# NO BIAS: Historical data shows NO bets have 62% WR vs 8% YES WR.
# Lower the edge threshold for NO entries and boost NO edge in selection.
NO_EDGE_DISCOUNT = _CFG.get("no_edge_discount", 0.02)  # NO needs 2% less edge to qualify
NO_SELECTION_BONUS = _CFG.get("no_selection_bonus", 0.03)  # NO gets +3% virtual edge bonus in ranking
MIN_SOURCES = _CFG.get("min_sources", 3)
MIN_FAMILIES = _CFG.get("min_families", 4)
MAX_TRADE_COST = _CFG.get("max_trade_cost_cents", 2000)
MAX_TRADES_PER_CITY = _CFG.get("max_trades_per_city", 1)  # IMPROVED: Reduced from 2 to 1
RESERVE_CENTS = _CFG.get("reserve_cents", 5000)
MAX_SOURCE_SPREAD = _CFG.get("max_source_spread", 2.0)

# IMPROVED: Portfolio risk limits
MAX_PORTFOLIO_EXPOSURE_PCT = _CFG.get("max_portfolio_exposure_pct", 0.60)  # 60% of portfolio
MAX_PER_DAY_EXPOSURE_CENTS = _CFG.get("max_per_day_exposure_cents", 8000)  # $80 per day

# FIX 2: Re-entry lockout — don't re-enter tickers sold by PM within 2 hours
LOCKOUT_FILE = os.path.join(BASE_DIR, "reentry_lockouts.json")
LOCKOUT_HOURS = 2

def check_lockout(ticker, series, target_date):
    """Return True if this ticker/series/date is locked out from re-entry."""
    if not os.path.exists(LOCKOUT_FILE):
        return False
    try:
        with open(LOCKOUT_FILE) as f:
            lockouts = json.load(f)
    except:
        return False
    key = f"{ticker}|{series}|{target_date}"
    lockout_time = lockouts.get(key)
    if not lockout_time:
        return False
    lockout_dt = datetime.fromisoformat(lockout_time.replace("Z", "+00:00"))
    if lockout_dt.tzinfo is None:
        lockout_dt = lockout_dt.replace(tzinfo=timezone.utc)
    elapsed_hours = (datetime.now(timezone.utc) - lockout_dt).total_seconds() / 3600
    return elapsed_hours < LOCKOUT_HOURS


def add_notification(msg):
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


def collect_fast_forecasts(lat, lon, tz, target_date, active_weights=None, lightweight=False):
    """Collect forecasts from all sources using BATCHED Open-Meteo API calls.
    
    Uses analyzer.collect_forecasts_batched() to make 3-7 API calls per city
    instead of 13+ individual calls. Massively reduces rate limit usage.
    
    If lightweight=True, only uses Open-Meteo batches (3 calls/city).
    Full mode (default on top-of-hour runs) adds HRRR, MET Norway, 
    Tomorrow.io, Visual Crossing, NWS for training data collection.
    
    Returns (active_forecasts, all_forecasts):
      - active_forecasts: only sources with weight > 0, used for ensemble/trading
      - all_forecasts: everything including training sources, used for logging/accuracy
    """
    # Use batched collection
    raw_forecasts = analyzer.collect_forecasts_batched(lat, lon, tz, target_date, lightweight=lightweight)
    
    # Filter out metadata keys (prefixed with __) — these are ensemble member stats, not forecasts
    all_forecasts = {k: v for k, v in raw_forecasts.items() if not k.startswith("__")}
    
    # Split into active (weight > 0) and training (weight = 0)
    active_forecasts = {}
    for name, val in all_forecasts.items():
        w = active_weights.get(name, 1.0) if active_weights else 1.0
        if w > 0:
            active_forecasts[name] = val
    
    # Log independent family count for active sources
    active_families = set()
    for name in active_forecasts:
        fam = MODEL_FAMILIES.get(name)
        if fam:
            active_families.add(fam)
    training_count = len(all_forecasts) - len(active_forecasts)
    if training_count > 0:
        print(f"    📊 {len(active_forecasts)} active sources ({len(active_families)} families) + {training_count} training")
    
    return active_forecasts, all_forecasts


def norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


DECISION_LOG_FILE = os.path.join(BASE_DIR, "decision_log.jsonl")
EDGE_PERSISTENCE_FILE = os.path.join(BASE_DIR, "edge_persistence.json")

def _load_edge_persistence():
    """Load edge persistence tracker (consecutive scans with positive EV per ticker+side)."""
    try:
        with open(EDGE_PERSISTENCE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _save_edge_persistence(data):
    try:
        with open(EDGE_PERSISTENCE_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass

def check_edge_persistence(ticker, direction, has_edge):
    """Track and check if a contract has shown positive EV on 2+ consecutive scans.
    Returns True if edge is persistent (OK to trade), False if not yet."""
    persistence = _load_edge_persistence()
    key = f"{ticker}:{direction}"
    now_ts = datetime.now(timezone.utc).isoformat()
    
    if has_edge:
        entry = persistence.get(key, {"count": 0, "first_seen": now_ts})
        entry["count"] = entry.get("count", 0) + 1
        entry["last_seen"] = now_ts
        if "first_seen" not in entry:
            entry["first_seen"] = now_ts
        persistence[key] = entry
    else:
        persistence.pop(key, None)
    
    # Clean stale entries (older than 6 hours)
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
    persistence = {k: v for k, v in persistence.items() if v.get("last_seen", "") > cutoff}
    
    _save_edge_persistence(persistence)
    return persistence.get(key, {}).get("count", 0) >= 2

def log_decision(entry):
    """Append a structured decision record (pass or skip) to the JSONL decision log."""
    entry["log_ts"] = datetime.now(timezone.utc).isoformat()
    try:
        with open(DECISION_LOG_FILE, "a") as f:
            f.write(json.dumps(entry, default=str) + "\n")
    except Exception:
        pass


def filter_opportunity(opp, cfg, hold_to_settlement=False):
    """Apply all rescue-mode and general filters to a single opportunity.
    
    Returns (pass: bool, reason: str or None, details: dict).
    """
    ticker = opp["ticker"]
    direction = opp["direction"]
    entry_cents = opp["entry_price_cents"]
    our_prob = opp.get("our_prob", 0.5)
    
    # 1. Threshold-only: reject all bracket markets
    if cfg.get("threshold_only", False) and "-B" in ticker:
        return False, "bracket_disabled_rescue", {}
    
    # 2. Allowed sides: e.g. ["NO"] means YES is blocked
    allowed_sides = cfg.get("allowed_sides")
    if allowed_sides and direction not in allowed_sides:
        return False, "side_disabled_rescue", {}
    
    # 3. NO price band (only applies to NO entries)
    if direction == "NO":
        band_min = cfg.get("no_price_band_min_cents", 0)
        band_max = cfg.get("no_price_band_max_cents", 100)
        if entry_cents < band_min or entry_cents > band_max:
            return False, f"no_price_outside_band_{band_min}-{band_max}", {
                "entry_cents": entry_cents, "band": [band_min, band_max]
            }
    
    # 4. Abstain zone: skip if win probability is in the dead zone
    abstain_min = cfg.get("abstain_zone_min_prob", 0)
    abstain_max = cfg.get("abstain_zone_max_prob", 0)
    if abstain_min > 0 and abstain_max > 0:
        if abstain_min <= our_prob <= abstain_max:
            return False, f"abstain_zone_{abstain_min:.0%}-{abstain_max:.0%}", {
                "our_prob": our_prob
            }
    
    # 5. Net EV floor (side-specific, no double-counting spread)
    min_net_ev = cfg.get("min_net_ev_cents", 0)
    if min_net_ev > 0:
        # For hold-to-settlement: no exit fees, just entry cost
        # Net EV = (prob_win * payout) - entry_cost
        # Payout on win = 100 - entry_cents (profit per contract)
        # EV per contract = our_prob * (100 - entry_cents) - (1 - our_prob) * entry_cents
        #                  = our_prob * 100 - entry_cents
        net_ev_cents = our_prob * 100.0 - entry_cents
        if net_ev_cents < min_net_ev:
            return False, f"net_ev_below_{min_net_ev}c", {
                "net_ev_cents": round(net_ev_cents, 2),
                "our_prob": our_prob, "entry_cents": entry_cents
            }
    
    return True, None, {}


def check_market_microstructure(ticker, direction, required_contracts, cfg):
    """Check spread, depth, and quote age for a market.
    
    Returns (pass: bool, reason: str or None, details: dict).
    """
    max_spread = cfg.get("max_spread_cents", 99)
    min_depth_factor = cfg.get("min_depth_factor", 0)
    max_quote_age = cfg.get("max_quote_age_seconds", 9999)
    
    details = {}
    
    try:
        book = kalshi_client.get_orderbook(ticker)
    except Exception as e:
        # If we can't get orderbook, skip microstructure checks but warn
        return True, None, {"orderbook_error": str(e)}
    
    yes_bids = book.get("yes", [])
    no_bids = book.get("no", [])
    
    # Spread check: difference between best yes bid and best yes ask
    # In Kalshi, yes_ask = 100 - no_bid, so spread = 100 - best_no_bid - best_yes_bid
    best_yes_bid = yes_bids[0].get("price", 0) if yes_bids else 0
    best_no_bid = no_bids[0].get("price", 0) if no_bids else 0
    
    if best_yes_bid > 0 and best_no_bid > 0:
        spread = 100 - best_no_bid - best_yes_bid  # e.g., yes_bid=70, no_bid=25 → spread=5
        details["spread_cents"] = spread
        if spread > max_spread:
            return False, f"spread_{spread}c_exceeds_{max_spread}c", details
    
    # Depth check: contracts available at best price must cover our order
    if min_depth_factor > 0 and required_contracts > 0:
        if direction == "YES":
            # We buy YES at ask. Depth on no side = YES ask depth
            depth = no_bids[0].get("count", 0) if no_bids else 0  
        else:
            # We buy NO at ask. Depth on yes side = NO ask depth  
            depth = yes_bids[0].get("count", 0) if yes_bids else 0
        details["top_depth_contracts"] = depth
        min_depth = int(required_contracts * min_depth_factor)
        if depth < min_depth:
            return False, f"depth_{depth}_below_{min_depth}_contracts", details
    
    # Quote age: check last_price timestamp if available from market data
    # Kalshi orderbook doesn't include timestamps, so we check the market endpoint
    try:
        mkt = kalshi_client.get_market(ticker)
        last_trade_ts = mkt.get("last_price_time") or mkt.get("latest_price_ts")
        if last_trade_ts and max_quote_age < 9999:
            # Parse ISO timestamp
            if isinstance(last_trade_ts, str):
                last_dt = datetime.fromisoformat(last_trade_ts.replace("Z", "+00:00"))
                age_sec = (datetime.now(timezone.utc) - last_dt).total_seconds()
                details["quote_age_seconds"] = int(age_sec)
                if age_sec > max_quote_age:
                    return False, f"quote_age_{int(age_sec)}s_exceeds_{max_quote_age}s", details
    except Exception:
        pass  # Don't block on quote age check failure
    
    return True, None, details


def find_opportunities(series, markets, forecast_mean, forecast_std, sigma_skipped=None):
    """Find trading opportunities from market list.
    FIX 6: Applies 2σ buffer for bracket bets to avoid marginal entries.
    If sigma_skipped list is passed, appends opportunities skipped by 2σ filter."""
    opportunities = []
    
    for mkt in markets:
        ticker = mkt.get("ticker", "")
        subtitle = mkt.get("subtitle", "")
        yes_bid = mkt.get("yes_bid")
        yes_ask = mkt.get("yes_ask")
        
        if not yes_bid or not yes_ask:
            continue
        
        # Parse strike from ticker
        parts = ticker.split("-")
        if len(parts) != 3:
            continue
        strike_str = parts[2]
        
        if strike_str.startswith("B"):
            strike = float(strike_str[1:])
            # Bracket market: P(temp in bracket)
            # Kalshi floor/cap are integers (e.g. B67.5 → floor=67, cap=68)
            # Apply ±0.5 continuity correction for integer settlement
            floor_val = strike - 1.0   # e.g. 67.5-1.0 = 66.5
            cap_val = strike + 1.0     # e.g. 67.5+1.0 = 68.5
            our_yes = norm_cdf((cap_val - forecast_mean) / max(forecast_std, 1.0)) - \
                      norm_cdf((floor_val - forecast_mean) / max(forecast_std, 1.0))
        elif strike_str.startswith("T"):
            strike = float(strike_str[1:])
            # T-markets can be "greater" (YES = above) or "less" (YES = below)
            # Must check strike_type from Kalshi API — DO NOT assume direction from ticker
            # Kalshi floor_strike/cap_strike are integer boundaries, apply +0.5 offset
            # e.g. T68 "69° or above": floor_strike=68 → boundary at 68.5
            # e.g. T61 "60° or below": cap_strike=61 → boundary at 60.5
            strike_type = mkt.get("strike_type", "greater")
            if strike_type == "less":
                # YES = temp <= (strike-1) in integers → P(temp < strike-0.5)
                our_yes = norm_cdf(((strike - 0.5) - forecast_mean) / max(forecast_std, 1.0))
            else:
                # YES = temp >= (strike+1) in integers → P(temp > strike+0.5)
                our_yes = 1 - norm_cdf(((strike + 0.5) - forecast_mean) / max(forecast_std, 1.0))
        else:
            continue
        
        # Cap probabilities at 0.95 to prevent fake "100% probability" edge inflation
        our_yes = min(our_yes, 0.95)
        our_no = 1.0 - our_yes
        market_yes = yes_bid / 100.0
        market_no = 1.0 - (yes_ask / 100.0)  # Use ask for NO side
        
        # Check YES edge (buy yes at ask)
        yes_cost = yes_ask / 100.0
        yes_edge = our_yes - yes_cost
        effective_min_edge = MIN_EDGE_BRACKET if strike_str.startswith("B") else MIN_EDGE
        if yes_edge > effective_min_edge:
            # FIX 6: 2σ buffer for bracket YES bets
            # Don't enter YES bracket if ensemble_mean - 2*std <= bracket_threshold (too close to edge)
            skip_sigma = False
            if strike_str.startswith("B"):
                bracket_floor = strike - 0.5
                if forecast_mean - 2 * forecast_std <= bracket_floor:
                    skip_sigma = True
            if skip_sigma and sigma_skipped is not None:
                sigma_skipped.append({
                    "ticker": ticker, "direction": "YES",
                    "edge": yes_edge, "entry_price_cents": yes_ask,
                })
            if not skip_sigma:
                opportunities.append({
                    "ticker": ticker,
                    "direction": "YES",
                    "edge": yes_edge,
                    "our_prob": our_yes,
                    "market_price": yes_cost,
                    "entry_price_cents": yes_ask,
                    "strike": strike,
                    "strike_type": mkt.get("strike_type", "greater") if strike_str.startswith("T") else "bracket",
                })
        
        # Check NO edge (buy no at ask = sell yes at bid equivalent)
        # Cap our_no at 0.95 independently for NO edge calculation
        capped_our_no = min(our_no, 0.95)
        no_cost = (100 - yes_bid) / 100.0
        no_edge = capped_our_no - no_cost
        effective_min_edge_no = (MIN_EDGE_BRACKET if strike_str.startswith("B") else MIN_EDGE) - NO_EDGE_DISCOUNT
        if no_edge > effective_min_edge_no:
            # FIX 6: 2σ buffer for bracket NO bets
            # Don't enter NO bracket if ensemble_mean + 2*std >= bracket_threshold (too close)
            skip_sigma = False
            if strike_str.startswith("B"):
                bracket_cap = strike + 0.5
                if forecast_mean + 2 * forecast_std >= bracket_cap:
                    skip_sigma = True
            # Hard 2σ buffer for threshold NO bets (same as brackets)
            # Will be revisited after sigma validation at late March checkpoint
            elif strike_str.startswith("T"):
                strike_type = mkt.get("strike_type", "greater")
                if strike_type == "less":
                    # NO on "less" market = betting temp >= strike. Skip if mean - 2σ < strike
                    if forecast_mean - 2 * forecast_std <= strike:
                        skip_sigma = True
                else:
                    # NO on "greater" market = betting temp < strike. Skip if mean + 2σ >= strike
                    if forecast_mean + 2 * forecast_std >= strike:
                        skip_sigma = True
            if skip_sigma and sigma_skipped is not None:
                sigma_skipped.append({
                    "ticker": ticker, "direction": "NO",
                    "edge": no_edge, "entry_price_cents": 100 - yes_bid,
                })
            if not skip_sigma:
                opportunities.append({
                    "ticker": ticker,
                    "direction": "NO",
                    "edge": no_edge,
                    "our_prob": capped_our_no,
                    "market_price": no_cost,
                    "entry_price_cents": 100 - yes_bid,
                    "strike": strike,
                    "strike_type": mkt.get("strike_type", "greater") if strike_str.startswith("T") else "bracket",
                })
    
    # NO BIAS: Add virtual bonus to NO opportunities when ranking (not stored in edge field)
    return sorted(opportunities, key=lambda x: x["edge"] + (NO_SELECTION_BONUS if x["direction"] == "NO" else 0), reverse=True)


def run():
    start = time.time()
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    
    print(f"⚡ Fast Scanner — {now.strftime('%H:%M:%S UTC')}")
    
    # === RESCUE MODE ENFORCEMENT ===
    _rescue = _CFG.get("rescue_mode", False)
    _allowed_cities = _CFG.get("allowed_cities", None)  # None = all cities
    _fixed_size = _CFG.get("fixed_size_cents", None)  # None = use Kelly
    _yes_ban_below = _CFG.get("yes_banned_below_cents", 0)
    _disable_adds = _CFG.get("disable_adds", False)
    _allowed_sides = _CFG.get("allowed_sides", None)  # e.g. ["NO"] — None = all sides
    _threshold_only = _CFG.get("threshold_only", False)
    _no_price_band_min = _CFG.get("no_price_band_min_cents", 0)
    _no_price_band_max = _CFG.get("no_price_band_max_cents", 100)
    _abstain_min = _CFG.get("abstain_zone_min_prob", 0)
    _abstain_max = _CFG.get("abstain_zone_max_prob", 0)
    _min_net_ev = _CFG.get("min_net_ev_cents", 0)
    _max_spread_cents = _CFG.get("max_spread_cents", 99)
    _min_depth_factor = _CFG.get("min_depth_factor", 0)
    _max_quote_age_sec = _CFG.get("max_quote_age_seconds", 9999)
    # Risk config
    _risk_cfg = _load_risk_config()
    _kill_switch = _risk_cfg.get("kill_switch_balance_cents", 0)
    _rolling_dd_pct = _risk_cfg.get("rolling_24h_drawdown_pct", 1.0)
    if _rescue:
        print(f"  🛟 RESCUE MODE — fixed ${(_fixed_size or 0)/100:.0f}/trade, "
              f"cities={_allowed_cities or 'all'}, sides={_allowed_sides or 'all'}, "
              f"threshold_only={_threshold_only}, NO band={_no_price_band_min}-{_no_price_band_max}¢, "
              f"abstain={_abstain_min:.0%}-{_abstain_max:.0%}, min_ev={_min_net_ev}¢, "
              f"adds={'OFF' if _disable_adds else 'ON'}")
    
    # Circuit breaker check
    cb_tripped, cb_reason = circuit_breaker.is_tripped()
    if cb_tripped:
        print(f"  🚨 CIRCUIT BREAKER: {cb_reason} — no new trades this scan")
        # Still collect forecasts for training but don't trade
        # Fall through to forecast collection only
    
    # Load existing trades to avoid duplicates
    trades_data = {"trades": []}
    if os.path.exists(TRADES_FILE):
        with open(TRADES_FILE) as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            trades_data = json.load(f)
            fcntl.flock(f, fcntl.LOCK_UN)
    # Track tickers we already hold OPEN positions in (for add-to logic)
    # This uses trades.json — will be supplemented by Kalshi positions below
    open_tickers = {t["ticker"]: t for t in trades_data.get("trades", []) if t.get("status") == "open"}
    # Track BLOWN tickers — temp already passed the strike, physics says no re-entry
    blown_tickers = {t["ticker"] for t in trades_data.get("trades", []) 
                     if t.get("result") == "blown"}
    # Track tickers we exited at a loss — re-entry allowed if data supports it + timing is right
    lost_tickers = {t["ticker"] for t in trades_data.get("trades", [])
                    if t.get("status") in ("exited", "settled") and t.get("result") == "loss"
                    and t.get("ticker") not in open_tickers}
    
    # Get balance
    try:
        bal = kalshi_client.get_balance()
        available = bal.get("balance", 0)
    except:
        available = 0
    
    # === KILL SWITCH ===
    if _kill_switch > 0 and available < _kill_switch:
        msg = f"🚨 KILL SWITCH: Balance ${available/100:.2f} < ${_kill_switch/100:.2f} — ALL TRADING HALTED"
        print(f"  {msg}")
        add_notification(msg)
        # Still collect forecasts below but skip all trading
        # We set cb_tripped to block all trades
        cb_tripped = True
        cb_reason = msg
    
    # === ROLLING 24h DRAWDOWN CIRCUIT BREAKER ===
    if _rolling_dd_pct < 1.0 and not cb_tripped:
        try:
            _24h_ago = (now - timedelta(hours=24)).isoformat()
            _all_trades = trades_data.get("trades", [])
            # Compute realized losses in last 24h from settled/exited trades
            _recent_loss = 0
            _recent_starting_capital = available  # approximate
            for _t in _all_trades:
                _t_ts = _t.get("settled_at") or _t.get("exit_timestamp") or ""
                if _t_ts >= _24h_ago and _t.get("status") in ("settled", "exited", "exited_blown", "spike_sold"):
                    _pnl = _t.get("pnl_cents", 0) or 0
                    if _pnl < 0:
                        _recent_loss += abs(_pnl)
                    _recent_starting_capital += abs(_pnl)  # reconstruct approximate starting point
            if _recent_starting_capital > 0:
                _dd_ratio = _recent_loss / _recent_starting_capital
                if _dd_ratio >= _rolling_dd_pct:
                    msg = f"🚨 24h DRAWDOWN: {_dd_ratio:.1%} >= {_rolling_dd_pct:.0%} limit (lost ${_recent_loss/100:.2f}) — TRADING HALTED"
                    print(f"  {msg}")
                    add_notification(msg)
                    cb_tripped = True
                    cb_reason = msg
        except Exception as _e:
            print(f"  ⚠ Rolling drawdown check failed: {_e}")
    
    new_trades = 0
    
    # Count open positions per city per date — FROM KALSHI (source of truth)
    from collections import Counter
    SERIES_TO_CITY = {s: m["city"] for s, m in SERIES_META.items()}
    
    city_date_open = Counter()
    kalshi_open_tickers = set()
    try:
        positions = kalshi_client.get_positions()
        for p in positions.get("market_positions", []):
            pos_count = p.get("position", 0)
            if pos_count == 0:
                continue
            tk = p.get("ticker", "")
            kalshi_open_tickers.add(tk)
            parts = tk.split("-")
            if len(parts) == 3:
                series_key = parts[0]
                date_key = parts[1]
                city_name = SERIES_TO_CITY.get(series_key, series_key)
                city_date_open[(city_name, date_key)] += 1
    except Exception as e:
        print(f"  ⚠ Kalshi positions fetch failed ({e}), falling back to trades.json")
        def ticker_date(ticker):
            parts = ticker.split("-")
            return parts[1] if len(parts) >= 3 else ""
        def normalize_city(c):
            return SERIES_TO_CITY.get(c, c)
        for t in trades_data.get("trades", []):
            if t.get("status") == "open":
                tk = t.get("ticker", "")
                kalshi_open_tickers.add(tk)
                city_date_open[(normalize_city(t.get("city", "")), ticker_date(tk))] += 1
    
    # Portfolio-level correlation check
    # Count how many cities are betting "warm" (YES on above-threshold) vs "cold" (NO)
    warm_cities = set()
    cold_cities = set()
    for t in trades_data.get("trades", []):
        if t.get("status") != "open":
            continue
        c = t.get("city", "")
        d = t.get("direction", "")
        tk = t.get("ticker", "")
        if "-T" in tk:  # threshold market
            st = t.get("strike_type", "greater")
            if st == "less":
                # "below" market: YES=cold, NO=warm
                if d == "YES":
                    cold_cities.add(c)
                else:
                    warm_cities.add(c)
            else:
                # "above" market: YES=warm, NO=cold
                if d == "YES":
                    warm_cities.add(c)
                else:
                    cold_cities.add(c)
        elif "-B" in tk:  # bracket market
            # Determine direction from forecast vs bracket center
            try:
                strike = float(tk.split("-")[2][1:])
                entry_forecast = t.get("entry_forecast_high")
                if entry_forecast is not None:
                    if d == "NO" and entry_forecast > strike:
                        warm_cities.add(c)  # NO bracket betting temp goes above
                    elif d == "NO" and entry_forecast < strike:
                        cold_cities.add(c)  # NO bracket betting temp stays below
                    elif d == "YES":
                        pass  # YES bracket is non-directional
            except:
                pass
    # If 4+ cities betting same direction, flag for reduced sizing
    portfolio_correlated = len(warm_cities) >= 4 or len(cold_cities) >= 4
    if portfolio_correlated:
        dominant = "warm" if len(warm_cities) >= 4 else "cold"
        print(f"  ⚠ Portfolio correlation: {len(warm_cities)} warm / {len(cold_cities)} cold — will reduce new {dominant} sizing by 50%")
    
    # Load source weights once (used for ensemble + source exclusion)
    weights_data = analyzer.load_source_weights()
    active_weights = weights_data[0] if isinstance(weights_data, tuple) else weights_data
    excluded_sources = [s for s, w in active_weights.items() if w == 0]
    if excluded_sources:
        print(f"  🚫 Excluded sources (MAE > cutoff): {', '.join(excluded_sources)}")
    
    for series, meta in SERIES_META.items():
        city = meta["city"]
        lat, lon, tz = meta["lat"], meta["lon"], meta["tz"]
        
        # Skip disabled cities (reversible via config)
        _disabled_cities = _CFG.get("disabled_cities", {})
        if city in _disabled_cities:
            print(f"  {city}: skipped — city_disabled_{city}")
            continue
        
        # RESCUE MODE: Track whether city is allowed for trading (but still collect data)
        _city_allowed_for_trading = (not _allowed_cities) or (city in _allowed_cities)
        
        # Get open markets for this series
        try:
            mkts = kalshi_client.get_markets(series_ticker=series, status="open")
            market_list = mkts.get("markets", [])
        except Exception as e:
            print(f"  {city}: market fetch failed — {e}")
            continue
        
        if not market_list:
            continue
        
        # Group markets by date — process each date with its own forecast
        from collections import defaultdict
        markets_by_date = defaultdict(list)
        for mkt in market_list:
            ticker = mkt.get("ticker", "")
            parts = ticker.split("-")
            if len(parts) == 3:
                ds = parts[1]
                months = {"JAN":"01","FEB":"02","MAR":"03","APR":"04","MAY":"05","JUN":"06",
                          "JUL":"07","AUG":"08","SEP":"09","OCT":"10","NOV":"11","DEC":"12"}
                try:
                    yr = "20" + ds[:2]
                    mo = months.get(ds[2:5], "01")
                    dy = ds[5:]
                    mkt_date = f"{yr}-{mo}-{dy}"
                    if mkt_date >= today:
                        markets_by_date[mkt_date].append(mkt)
                except:
                    markets_by_date[today].append(mkt)
        
        if not markets_by_date:
            print(f"  {city}: no active future markets")
            continue
        
        # Process the LATEST date only (tomorrow if available, else today)
        # We don't enter new positions for today — PM handles those
        target_date = max(markets_by_date.keys())
        market_list = markets_by_date[target_date]
        
        # ── Up-front market universe filter (rescue mode) ──
        # Don't waste API calls evaluating markets we'll never trade.
        # Brackets and wrong-side markets are filtered HERE, not in filter_opportunity.
        if _threshold_only:
            pre_count = len(market_list)
            market_list = [m for m in market_list if "-B" not in m.get("ticker", "")]
            if pre_count != len(market_list):
                print(f"    [rescue] Filtered {pre_count - len(market_list)} bracket markets up front")
        if not market_list:
            continue
        
        # Get fast forecasts for the target date
        # Lightweight mode on :22/:42 runs saves API quota (skip Tomorrow.io/VC/HRRR/MET Norway/NWS)
        # Full mode on :02 runs collects all training data
        _minute = now.minute
        _lightweight = not (_minute < 10)  # :02 run = full, :22/:42 = lightweight
        forecasts, all_forecasts = collect_fast_forecasts(lat, lon, tz, target_date, active_weights=active_weights, lightweight=_lightweight)
        # Extract training-only sources for journal logging
        training_forecasts = {k: v for k, v in all_forecasts.items() if k not in forecasts}
        if len(forecasts) < MIN_SOURCES:
            print(f"  {city}: only {len(forecasts)} sources, need {MIN_SOURCES} — skip")
            # Log hypothetical: we don't know the best opp but log what we can
            if forecasts and len(forecasts) > 0:
                _vals = [v for v in forecasts.values() if v is not None]
                if _vals:
                    _mean = sum(_vals) / len(_vals)
                    _std = statistics.stdev(_vals) if len(_vals) > 1 else 3.0
                    hypothetical_tracker.log_hypothetical(
                        city=city, ticker=f"{series}-?-?", direction="?",
                        edge=None, entry_price_cents=None,
                        reason_skipped=f"MIN_SOURCES: {len(forecasts)}/{MIN_SOURCES}",
                        ensemble_mean=_mean, ensemble_std=_std,
                        source_spread=max(_vals) - min(_vals) if len(_vals) > 1 else 0,
                    )
            continue
        
        # Apply per-city model disabling before family check
        city_weights = analyzer.get_city_weights(city)
        if city_weights:
            forecasts = {k: v for k, v in forecasts.items() if city_weights.get(k, 1.0) > 0}
        
        # Check minimum independent model families
        active_families = set()
        for name in forecasts:
            fam = MODEL_FAMILIES.get(name)
            if fam:
                active_families.add(fam)
        if len(active_families) < MIN_FAMILIES:
            print(f"  {city}: only {len(active_families)} model families ({', '.join(active_families)}), need {MIN_FAMILIES} — skip")
            _vals = [v for v in forecasts.values() if v is not None]
            if _vals:
                _mean = sum(_vals) / len(_vals)
                _std = statistics.stdev(_vals) if len(_vals) > 1 else 3.0
                hypothetical_tracker.log_hypothetical(
                    city=city, ticker=f"{series}-?-?", direction="?",
                    edge=None, entry_price_cents=None,
                    reason_skipped=f"MIN_FAMILIES: {len(active_families)}/{MIN_FAMILIES} ({', '.join(active_families)})",
                    ensemble_mean=_mean, ensemble_std=_std,
                    source_spread=max(_vals) - min(_vals) if len(_vals) > 1 else 0,
                )
            continue
        
        # Weighted ensemble with sigma multiplier
        sigma_multiplier = analyzer.get_sigma_multiplier(city)
        stats = analyzer.weighted_ensemble_stats(forecasts, active_weights, sigma_multiplier, city)
        
        mean = stats.get("ensemble_mean")
        std = stats.get("calibrated_std", 3.0)
        
        # Log ALL forecasts (active + training) for accuracy tracking
        # This runs every scan for every city regardless of whether we trade
        try:
            training_logger.log_forecasts(
                city=city, target_date=target_date,
                active_forecasts=forecasts, all_forecasts=all_forecasts,
                ensemble_mean=mean, ensemble_std=std,
            )
        except Exception:
            pass  # Don't let logging errors break trading

        # Per-city daily logging for learning
        try:
            city_logger.log_forecast(
                city=city,
                sources={k: round(v, 2) for k, v in forecasts.items()},
                mean=mean, std=std,
                target_date=target_date_str,
            )
        except Exception:
            pass
        
        # Forecast history (tracks how forecasts evolve over time)
        try:
            forecast_logger.log_snapshot(
                city=city, series=series, target_date=target_date_str,
                source_forecasts={k: round(v, 2) for k, v in forecasts.items()},
                ensemble_mean=mean, calibrated_std=std,
            )
        except Exception:
            pass
        
        if not mean:
            continue
        
        # RESCUE MODE: If city not in allowed list, we collected data but skip trading
        if not _city_allowed_for_trading:
            print(f"  {city}: {mean:.1f}°F ± {std:.1f}°F — data logged, trading skipped (not in allowed_cities)")
            log_decision({"action": "idle_proof", "city": city, "ensemble_mean": mean, "ensemble_std": std, "reason": "not_in_allowed_cities"})
            continue
        
        # ── Weather alert check ──
        try:
            alert_info = analyzer.check_weather_alerts(lat, lon)
            if alert_info.get("skip_trading"):
                print(f"  ⛈️  {city}: SEVERE WEATHER ALERT — {', '.join(alert_info['alerts'])}. Skipping trades.")
                continue
            elif alert_info.get("confidence_penalty", 0) > 0:
                penalty = alert_info["confidence_penalty"]
                std = std * (1 + penalty)  # Widen std to reflect increased uncertainty
                print(f"  ⚠️  {city}: Weather advisory ({', '.join(alert_info['alerts'])}). "
                      f"Widening std by {penalty:.0%} → {std:.1f}°F")
        except Exception:
            pass  # Don't let alert check failure block trading
        
        # Source spread → sizing modifier (not hard gate)
        # Models always disagree somewhat. Use spread to modulate confidence, not block trades.
        # Dynamic threshold: base 8°F, but scale with calibrated_std
        # Cities with naturally high uncertainty (high std) get a wider spread tolerance
        source_spread_val = stats.get("source_spread_f", 0) or 0
        spread_size_factor = 1.0
        spread_skip_threshold = max(8.0, std * 4)  # At least 8°F, or 4x the city's calibrated std
        spread_half_threshold = max(5.0, std * 2.5)
        spread_reduce_threshold = max(3.0, std * 1.5)
        if source_spread_val > spread_skip_threshold:
            print(f"  {city}: {mean:.1f}°F ± {std:.1f}°F — source spread {source_spread_val:.1f}°F > {spread_skip_threshold:.0f}°F, skip")
            _hypo_opps = find_opportunities(series, market_list, mean, std)
            if _hypo_opps:
                _best = _hypo_opps[0]
                hypothetical_tracker.log_hypothetical(
                    city=city, ticker=_best["ticker"], direction=_best["direction"],
                    edge=_best["edge"], entry_price_cents=_best["entry_price_cents"],
                    reason_skipped=f"source_spread: {source_spread_val:.1f}F > {spread_skip_threshold:.0f}F",
                    ensemble_mean=mean, ensemble_std=std, source_spread=source_spread_val,
                    our_probability=_best.get("our_prob"),
                    market_probability=_best.get("market_price"),
                    strike=_best.get("strike"),
                    bracket=_best.get("strike_type"),
                    target_date=str(target_date),
                    sigma_multiplier=sigma_multiplier,
                    sources_used=list(forecasts.keys()),
                    source_forecasts={k: round(v, 1) for k, v in forecasts.items() if v is not None},
                )
            continue
        elif source_spread_val > spread_half_threshold:
            spread_size_factor = 0.5
            print(f"    ⚠ High spread {source_spread_val:.1f}°F — sizing at 50%")
        elif source_spread_val > spread_reduce_threshold:
            spread_size_factor = 0.75
        # ≤3°F = full size (spread_size_factor stays 1.0)
        
        # Get today's hourly data — used for blown-trade filter AND peak detection
        # Open-Meteo blends observed actuals (past hours) with forecast (future hours)
        current_max = None
        hourly_temps = []
        if target_date == today:
            try:
                url = (f"https://api.open-meteo.com/v1/forecast?"
                       f"latitude={lat}&longitude={lon}"
                       f"&hourly=temperature_2m&temperature_unit=fahrenheit"
                       f"&start_date={today}&end_date={today}")
                req = urllib.request.Request(url, headers={"User-Agent": "KingClaw-FS/1.0"})
                with urllib.request.urlopen(req, timeout=10) as resp:
                    hdata = json.loads(resp.read())
                hourly_temps = hdata.get("hourly", {}).get("temperature_2m", [])
                # ONLY use OBSERVED hours (up to current UTC hour) for current max
                # Future hours are forecast, not reality — don't use them for blown detection
                observed_temps = [hourly_temps[i] for i in range(min(now.hour + 1, len(hourly_temps)))
                                  if i < len(hourly_temps) and hourly_temps[i] is not None]
                current_max = max(observed_temps) if observed_temps else None
            except:
                pass
        
        # === DATA-DRIVEN PEAK DETECTION ===
        import peak_detector
        from zoneinfo import ZoneInfo
        local_now = now.astimezone(ZoneInfo(tz))
        utc_offset_hrs = local_now.utcoffset().total_seconds() / 3600
        peak_info = peak_detector.detect_peak(hourly_temps, now.hour, utc_offset_hours=utc_offset_hrs, forecast_std=std)
        past_peak = peak_info["past_peak"]
        peak_hour = peak_info["peak_hour"]
        
        if past_peak:
            print(f"    📉 Peak CONFIRMED ({peak_info['confidence']}): "
                  f"max {peak_info['observed_max']:.1f}°F at UTC hour {peak_hour}, "
                  f"rate={peak_info['rate_of_change']:+.1f}°F/hr, "
                  f"{peak_info['consecutive_declines']} consecutive declines")
        
        # For future dates, peak detection doesn't apply
        if target_date != today:
            past_peak = False
        
        # === TIME-OF-DAY ENTRY RULES FOR NEXT-DAY MARKETS ===
        # Evening/night model runs are stale — require very strong edge for tomorrow's markets.
        # UTC 22-07 = evening/night in US time zones. UTC 08+ = 3AM+ EST, fresh models arriving.
        # Daytime UTC 11-21 = fresh model data, normal rules apply.
        next_day_evening = False
        next_day_max_spend = None
        if target_date > today:
            current_utc_hour = now.hour
            if current_utc_hour >= 22 or current_utc_hour <= 7:
                next_day_evening = True  # Will filter below after finding opportunities
        
        # Find opportunities
        _sigma_skipped = []
        opps = find_opportunities(series, market_list, mean, std, sigma_skipped=_sigma_skipped)
        # Log hypothetical trades that were skipped by 2σ buffer
        if _sigma_skipped and not opps:
            _best_sigma = max(_sigma_skipped, key=lambda x: x["edge"])
            hypothetical_tracker.log_hypothetical(
                city=city, ticker=_best_sigma["ticker"], direction=_best_sigma["direction"],
                edge=_best_sigma["edge"], entry_price_cents=_best_sigma["entry_price_cents"],
                reason_skipped="2sigma_buffer",
                ensemble_mean=mean, ensemble_std=std, source_spread=source_spread_val,
                our_probability=_best_sigma.get("our_prob"),
                market_probability=_best_sigma.get("market_price"),
                strike=_best_sigma.get("strike"),
                bracket=_best_sigma.get("strike_type"),
                target_date=str(target_date),
                sigma_multiplier=sigma_multiplier,
                sources_used=list(forecasts.keys()),
                source_forecasts={k: round(v, 1) for k, v in forecasts.items() if v is not None},
            )
        
        # === NEXT-DAY EVENING FILTER ===
        # If targeting tomorrow during evening/night hours, require 20%+ edge or skip.
        # Strong edges (≥20%) allowed but capped at $5 max spend.
        if next_day_evening and opps:
            evening_filtered = []
            for o in opps:
                if o["edge"] >= 0.20:
                    evening_filtered.append(o)
                else:
                    print(f"    ⏰ Next-day evening entry — edge {o['edge']:.1%} not strong enough (need 20%+)")
                    hypothetical_tracker.log_hypothetical(
                        city=city, ticker=o["ticker"], direction=o["direction"],
                        edge=o["edge"], entry_price_cents=o["entry_price_cents"],
                        reason_skipped=f"evening_filter: edge {o['edge']:.1%} < 20%",
                        ensemble_mean=mean, ensemble_std=std, source_spread=source_spread_val,
                        our_probability=o.get("our_prob"),
                        market_probability=o.get("market_price"),
                        strike=o.get("strike"),
                        bracket=o.get("strike_type"),
                        target_date=str(target_date),
                        sigma_multiplier=sigma_multiplier,
                        sources_used=list(forecasts.keys()),
                    )
            opps = evening_filtered
            next_day_max_spend = 500  # $5 cap for next-day evening entries
        
        # Filter out BLOWN tickers permanently (temp already passed strike — physics)
        opps = [o for o in opps if o["ticker"] not in blown_tickers]
        
        # FIX 2: Filter out tickers under re-entry lockout (PM sold within last 2 hours)
        pre_lockout = len(opps)
        opps = [o for o in opps if not check_lockout(o["ticker"], series, target_date)]
        if len(opps) < pre_lockout:
            print(f"    🔒 Lockout filtered {pre_lockout - len(opps)} ticker(s) (PM sold < {LOCKOUT_HOURS}h ago)")
        
        # Lost tickers: allow re-entry only pre-peak with stronger edge requirement
        # Filter lost tickers: only allow re-entry pre-peak with higher edge bar
        RE_ENTRY_MIN_EDGE = 0.10  # 10% edge minimum for re-entries (vs 8% normal)
        if past_peak:
            # After peak: no re-entering lost tickers at all (high is locked in)
            opps = [o for o in opps if o["ticker"] not in lost_tickers]
        else:
            # Before peak: allow re-entry but demand stronger edge
            filtered = []
            for o in opps:
                if o["ticker"] in lost_tickers:
                    if o["edge"] >= RE_ENTRY_MIN_EDGE:
                        print(f"    ♻ Re-entry candidate: {o['ticker']} {o['direction']} edge={o['edge']:.1%} (was lost, data now supports)")
                        filtered.append(o)
                    # else: skip, edge not strong enough to justify re-entry
                else:
                    filtered.append(o)
            opps = filtered
        
        # Filter out trades that are ALREADY blown by current temps
        if current_max is not None:
            safe_opps = []
            for o in opps:
                strike = o["strike"]
                direction = o["direction"]
                ticker = o["ticker"]
                # Use actual strike_type from the opportunity (sourced from Kalshi API)
                opp_strike_type = o.get("strike_type", "between" if "-B" in ticker else "greater")
                is_less = opp_strike_type in ("less", "below")
                is_bracket = opp_strike_type in ("between", "bracket")
                
                blown = False
                if direction == "NO" and not is_bracket and not is_less:
                    # NO on "above" (e.g. T68 "69° or above") — blown if temp already >= strike+1
                    if current_max >= strike + 1:
                        blown = True
                elif direction == "NO" and is_less:
                    # NO on "below" (e.g. T61 "60° or below") = betting warm
                    # Blown if past peak and max is still ≤ strike-1 (YES wins)
                    if past_peak and current_max <= strike - 1:
                        blown = True
                elif direction == "NO" and is_bracket:
                    # NO on bracket — only blown if PAST PEAK and daily max is in bracket
                    bracket_low = int(strike - 0.5)
                    bracket_high = int(strike + 0.5)
                    if past_peak and bracket_low <= current_max <= bracket_high:
                        blown = True
                elif direction == "YES" and not is_bracket and not is_less:
                    # YES on "above" — can't be blown early, only at end of day
                    pass
                elif direction == "YES" and is_less:
                    # YES on "below" (e.g. T18 "17° or below") = betting cold
                    # BLOWN if temp already >= strike (daily high guaranteed above threshold)
                    if current_max >= strike:
                        blown = True
                elif direction == "YES" and is_bracket:
                    # YES on bracket — can't be blown early
                    pass
                
                if blown:
                    print(f"    ✗ Skip {ticker} {direction} — temp already {current_max}°F, strike blown")
                else:
                    safe_opps.append(o)
            opps = safe_opps
        
        if not opps:
            # ── Idle proof: log top 5 closest-to-pass threshold NO candidates ──
            idle_candidates = []
            for mkt in market_list:
                ticker = mkt.get("ticker", "")
                parts = ticker.split("-")
                if len(parts) != 3 or not parts[2].startswith("T"):
                    continue
                yes_bid = mkt.get("yes_bid")
                yes_ask = mkt.get("yes_ask")
                if not yes_bid or not yes_ask:
                    continue
                strike = float(parts[2][1:])
                strike_type = mkt.get("strike_type", "greater")
                if strike_type == "less":
                    our_yes = norm_cdf(((strike - 0.5) - mean) / max(std, 1.0))
                else:
                    our_yes = 1 - norm_cdf(((strike + 0.5) - mean) / max(std, 1.0))
                our_yes = min(our_yes, 0.95)
                our_no = min(1 - our_yes, 0.95)
                no_ask_cents = 100 - yes_bid
                no_cost = no_ask_cents / 100.0
                no_edge = our_no - no_cost
                fee_per_contract = 0.02  # 2¢ Kalshi fee estimate
                net_ev_cents = round((no_edge * 100) - fee_per_contract, 2)
                idle_candidates.append({
                    "ticker": ticker, "direction": "NO",
                    "yes_bid": yes_bid, "yes_ask": yes_ask,
                    "no_ask_cents": no_ask_cents, "model_p_no": round(our_no, 4),
                    "no_edge": round(no_edge, 4), "net_ev_cents": net_ev_cents,
                    "strike": strike, "strike_type": strike_type,
                    "z_score": round(((strike - mean) / max(std, 1.0)) if strike_type != "less" else ((mean - strike) / max(std, 1.0)), 2),
                    "fail_reason": "negative_ev" if net_ev_cents <= 0 else
                                   f"no_price_outside_band" if no_ask_cents < _CFG.get("no_price_band_min_cents", 0) or no_ask_cents > _CFG.get("no_price_band_max_cents", 100) else
                                   f"blocked_by_2sigma (z={((strike - mean) / max(std, 1.0)) if strike_type != 'less' else ((mean - strike) / max(std, 1.0)):.1f}, EV={net_ev_cents:+.1f}¢)" if ((strike_type != "less" and (strike - mean) / max(std, 1.0) < 2.0) or (strike_type == "less" and (mean - strike) / max(std, 1.0) < 2.0)) else
                                   "passed_but_filtered",
                })
            idle_candidates.sort(key=lambda x: x["net_ev_cents"], reverse=True)
            top5 = idle_candidates[:5]
            if top5:
                print(f"  {city}: {mean:.1f}°F ± {std:.1f}°F — no new edge. Top candidates:")
                for c in top5:
                    print(f"    {c['ticker']} NO @ {c['no_ask_cents']}¢ | p={c['model_p_no']:.2f} | EV={c['net_ev_cents']:+.1f}¢ | {c['fail_reason']}")
                log_decision({
                    "action": "idle_proof", "city": city,
                    "ensemble_mean": round(mean, 1), "ensemble_std": round(std, 1),
                    "top_candidates": top5,
                })
            else:
                print(f"  {city}: {mean:.1f}°F ± {std:.1f}°F — no new edge (no threshold markets)")
            continue
        
        # IMPROVED: Position conflict detection and overlap filtering
        # Uses KALSHI positions as source of truth for what we actually hold
        # 1. Check for contradictory positions (e.g., YES ">=57" and NO "57-58")
        # 2. Minimum 3°F strike separation for same direction + same market type
        MIN_STRIKE_SEP = 3.0
        existing_city_positions = []
        for tk in kalshi_open_tickers:
            tk_parts = tk.split("-")
            if len(tk_parts) != 3 or tk_parts[0] != series:
                continue
            strike_str = tk_parts[2]
            try:
                strike_val = float(strike_str[1:])
            except:
                continue
            # Infer direction from Kalshi position count (positive=YES, negative=NO)
            pos_dir = "YES"  # default
            for t in trades_data.get("trades", []):
                if t.get("ticker") == tk and t.get("status") == "open":
                    pos_dir = t.get("direction", "YES")
                    break
            else:
                # Not in trades.json — check Kalshi position sign
                try:
                    for p in positions.get("market_positions", []):
                        if p.get("ticker") == tk:
                            pos_dir = "YES" if p.get("position", 0) > 0 else "NO"
                            break
                except:
                    pass
            market_type = "T" if "-T" in tk else "B"
            existing_city_positions.append((pos_dir, tk, strike_val, market_type))
        
        # IMPROVED: Detect position conflicts before checking overlaps
        conflict_filtered = []
        for o in opps:
            o_type = "T" if "-T" in o["ticker"] else "B"
            has_conflict = False
            
            for ex_dir, ex_ticker, ex_strike, ex_type in existing_city_positions:
                # Check for contradictory positions
                if o["direction"] == "YES" and o_type == "T" and ex_dir == "NO" and ex_type == "B":
                    # YES on ">=X" conflicts with NO on bracket near X
                    if abs(o["strike"] - ex_strike) <= 1.5:  # Adjacent or overlapping
                        print(f"    CONFLICT: {o['ticker']} {o['direction']} conflicts with existing {ex_ticker} {ex_dir}")
                        has_conflict = True
                        break
                elif o["direction"] == "NO" and o_type == "B" and ex_dir == "YES" and ex_type == "T":
                    # NO on bracket conflicts with YES on ">=X" near same strike
                    if abs(o["strike"] - ex_strike) <= 1.5:
                        print(f"    CONFLICT: {o['ticker']} {o['direction']} conflicts with existing {ex_ticker} {ex_dir}")
                        has_conflict = True
                        break
                # Additional conflict: opposite sides of same strike
                elif o_type == ex_type and abs(o["strike"] - ex_strike) < 0.5:
                    if o["direction"] != ex_dir:
                        print(f"    CONFLICT: {o['ticker']} {o['direction']} conflicts with existing {ex_ticker} {ex_dir} (same strike)")
                        has_conflict = True
                        break
            
            if not has_conflict:
                conflict_filtered.append(o)
        
        conflicts_removed = len(opps) - len(conflict_filtered)
        if conflicts_removed > 0:
            print(f"    Filtered {conflicts_removed} conflicting opportunities")
        opps = conflict_filtered
        
        # Apply overlap filtering after conflict detection
        deduped = []
        for o in opps:
            dominated = False
            o_type = "T" if "-T" in o["ticker"] else "B"
            for ex_dir, ex_ticker, ex_strike, ex_type in existing_city_positions:
                if o["direction"] == ex_dir and o_type == ex_type:
                    if abs(o["strike"] - ex_strike) < MIN_STRIKE_SEP:
                        dominated = True
                        break
            if not dominated:
                deduped.append(o)
        
        overlaps_removed = len(opps) - len(deduped)
        if overlaps_removed > 0:
            print(f"    Filtered {overlaps_removed} overlapping opportunities (< {MIN_STRIKE_SEP}°F from existing)")
        opps = deduped
        
        if not opps:
            print(f"  {city}: {mean:.1f}°F ± {std:.1f}°F — all opportunities overlap existing positions")
            continue
        
        # Parse target date from first opp ticker for city-date counting
        first_ticker = opps[0]["ticker"]
        t_parts = first_ticker.split("-")
        target_date_str = t_parts[1] if len(t_parts) >= 3 else ""
        
        # Check per-city-per-date limit (for NEW distinct tickers only)
        city_date_count = city_date_open.get((city, target_date_str), 0)
        
        # IMPROVED: Single best opportunity selection per city
        # Instead of spraying across multiple brackets, select the SINGLE best opportunity
        # This reduces correlation risk and improves capital efficiency
        
        # Compute hours to settlement: NWS CLI typically available next morning ~9AM EST = 14:00 UTC
        try:
            _settle_date = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            _settle_time = _settle_date + timedelta(days=1, hours=14)  # day after target, 14:00 UTC
            _hours_to_settlement = max(0, (_settle_time - now).total_seconds() / 3600)
        except:
            _hours_to_settlement = None
        
        # === COMPREHENSIVE FILTER CHAIN ===
        # Each filter logs skipped opportunities to decision_log.jsonl
        pre_filter = len(opps)
        filtered_opps = []
        for _opp in opps:
            _pass, _reason, _fdetails = filter_opportunity(_opp, _CFG,
                hold_to_settlement=_PM_CFG.get("hold_to_settlement", False))
            if _pass:
                filtered_opps.append(_opp)
            else:
                print(f"    🚫 Skip {_opp['ticker']} {_opp['direction']} @ {_opp['entry_price_cents']}¢ — {_reason}")
                log_decision({
                    "action": "skip",
                    "ticker": _opp["ticker"],
                    "direction": _opp["direction"],
                    "entry_price_cents": _opp["entry_price_cents"],
                    "our_prob": _opp.get("our_prob"),
                    "edge": _opp.get("edge"),
                    "reason": _reason,
                    "city": city,
                    "ensemble_mean": mean,
                    "ensemble_std": std,
                    "raw_std": stats.get("ensemble_std", 3.0),
                    "sigma_multiplier": sigma_multiplier,
                    "sigma_range_low": mean - std,
                    "sigma_range_high": mean + std,
                    "source_spread": source_spread_val,
                    "family_count": len(active_families),
                    "hours_to_settlement": _hours_to_settlement,
                    **_fdetails,
                })
        opps = filtered_opps
        if pre_filter > len(opps):
            print(f"    Filtered {pre_filter - len(opps)}/{pre_filter} opportunities by rescue rules")
        
        # Legacy YES ban (still active as fallback if allowed_sides not set)
        if _yes_ban_below > 0 and not _allowed_sides:
            pre_ban = len(opps)
            opps = [o for o in opps if not (o["direction"] == "YES" and o["entry_price_cents"] < _yes_ban_below)]
            if len(opps) < pre_ban:
                print(f"    🚫 Banned {pre_ban - len(opps)} YES entries below {_yes_ban_below}¢")
        
        # Separate opps into "add to existing" vs "new position"
        add_opps = [o for o in opps if o["ticker"] in kalshi_open_tickers]
        new_opps = [o for o in opps if o["ticker"] not in kalshi_open_tickers]
        
        # IMPROVED: Select only the single best opportunity for new positions
        # For existing positions, only add if edge is very strong (12%+)
        best = None
        is_add = False
        
        if add_opps and add_opps[0]["edge"] >= 0.12 and not _disable_adds:
            best = add_opps[0]
            is_add = True
            print(f"    💪 Strong edge {add_opps[0]['edge']:.1%} - adding to existing position")
        elif new_opps:
            if city_date_count >= MAX_TRADES_PER_CITY:
                print(f"  {city}: {mean:.1f}°F ± {std:.1f}°F — already {MAX_TRADES_PER_CITY} position(s) for {target_date_str}, skip")
                continue
            else:
                # IMPROVED: Take only the single best new opportunity (already sorted by edge)
                best = new_opps[0]
                if len(new_opps) > 1:
                    second_best_edge = new_opps[1]["edge"]
                    print(f"    🎯 Single best: {best['edge']:.1%} edge (vs {second_best_edge:.1%} for 2nd best)")
        elif add_opps and add_opps[0]["edge"] >= 0.10 and not _disable_adds:
            best = add_opps[0]
            is_add = True
        
        if not best:
            print(f"  {city}: {mean:.1f}°F ± {std:.1f}°F — no actionable edge")
            continue
        
        # ── Edge persistence: require positive EV on 2+ consecutive scans ──
        persistent = check_edge_persistence(best["ticker"], best["direction"], True)
        if not persistent:
            print(f"  {city}: {mean:.1f}°F ± {std:.1f}°F — edge on {best['ticker']} {best['direction']} ({best['edge']:.1%}) — waiting for persistence (scan 1/2)")
            log_decision({
                "action": "skip", "ticker": best["ticker"], "direction": best["direction"],
                "entry_price_cents": best["entry_price_cents"], "our_prob": best.get("our_prob"),
                "edge": best.get("edge"), "reason": "edge_not_persistent",
                "city": city, "ensemble_mean": mean, "ensemble_std": std,
            })
            continue
        
        action_label = "ADD" if is_add else "NEW"
        print(f"  {city}: {mean:.1f}°F ± {std:.1f}°F — [{action_label}] {best['ticker']} {best['direction']} edge={best['edge']:.1%} (persistent ✓)")
        
        # CIRCUIT BREAKER: Skip new trades if tripped (but we still collected forecasts above)
        if cb_tripped:
            print(f"    ⛔ Circuit breaker active — skipping trade")
            continue
        
        # IMPROVED: Portfolio exposure limits
        # 1. Hard stop: Never trade below reserve
        if available <= RESERVE_CENTS:
            print(f"  ⛔ RESERVE LIMIT: Cash ${available/100:.2f} <= ${RESERVE_CENTS/100:.2f} reserve. Stopping.")
            break
            
        # 2. IMPROVED: Total portfolio exposure cap
        try:
            bal = kalshi_client.get_balance()
            portfolio_value = bal.get("portfolio_value", available)
            current_exposure = portfolio_value - available  # Deployed capital
            max_portfolio_exposure = int(portfolio_value * MAX_PORTFOLIO_EXPOSURE_PCT)
            
            if current_exposure >= max_portfolio_exposure:
                print(f"  ⛔ PORTFOLIO EXPOSURE LIMIT: ${current_exposure/100:.2f}/${max_portfolio_exposure/100:.2f} ({MAX_PORTFOLIO_EXPOSURE_PCT:.0%})")
                continue
                
            # 3. IMPROVED: Per-day exposure limit (sum of all positions for today)
            today_exposure = 0
            for t in trades_data.get("trades", []):
                if t.get("status") == "open":
                    t_parsed_parts = t.get("ticker", "").split("-")
                    if len(t_parsed_parts) >= 2:
                        # Parse date from ticker to check if it's today
                        try:
                            months = {"JAN":"01","FEB":"02","MAR":"03","APR":"04","MAY":"05","JUN":"06",
                                      "JUL":"07","AUG":"08","SEP":"09","OCT":"10","NOV":"11","DEC":"12"}
                            yr = "20" + t_parsed_parts[1][:2]
                            mo = months.get(t_parsed_parts[1][2:5], "01")
                            dy = t_parsed_parts[1][5:]
                            trade_date = f"{yr}-{mo}-{dy}"
                            if trade_date == today:
                                today_exposure += t.get("market_exposure") or t.get("cost_cents", 0)
                        except:
                            pass
            
            if today_exposure >= MAX_PER_DAY_EXPOSURE_CENTS:
                print(f"  ⛔ DAILY EXPOSURE LIMIT: ${today_exposure/100:.2f}/${MAX_PER_DAY_EXPOSURE_CENTS/100:.2f} for {today}")
                continue
        except Exception as e:
            print(f"  ⚠ Exposure check failed: {e}")
            # Continue with trade if check fails
        
        # Size from tradeable capital only (above reserve)
        tradeable = available - RESERVE_CENTS
        entry_cents = best["entry_price_cents"]
        
        # RESCUE MODE: Fixed sizing overrides Kelly
        if _fixed_size:
            size = _fixed_size
            kelly_f = 0
            kelly_raw = 0
        else:
            # Kelly sizing: calculate optimal fraction, cap it, use as size guide
            prob = best.get("our_prob", 0.5)
            b = (100 - entry_cents) / entry_cents  # Same for YES and NO: pay entry, win 100
            q = 1 - prob
            kelly_raw = (prob * b - q) / b if b > 0 else 0
            kelly_f = max(0, min(kelly_raw, KELLY_MAX))
            kelly_size = int(tradeable * kelly_f)
            
            # Final size = min of Kelly-sized amount and original caps
            size = min(MAX_TRADE_COST, kelly_size, int(tradeable * 0.03))
        # Apply next-day evening spend cap ($5 max)
        if next_day_max_spend is not None:
            size = min(size, next_day_max_spend)
        
        # Apply spread sizing factor
        size = int(size * spread_size_factor)
        
        # Source agreement sizing: when our sources AGREE (low std), we're more confident
        # When they DISAGREE (high std), size down — we're less sure
        source_spread = stats.get("source_spread_f", 0) or 0
        if std <= 1.5:
            # Tight agreement — boost sizing 50%
            size = int(size * 1.5)
            print(f"    📊 High conviction (std={std:.1f}°F) — sizing +50%")
        elif std >= 4.0:
            # Wide disagreement — reduce sizing 40%
            size = int(size * 0.6)
            print(f"    📊 Low conviction (std={std:.1f}°F) — sizing -40%")
        
        # Reduce sizing if portfolio is correlated and this trade adds to the correlation
        if portfolio_correlated:
            is_warm = best["direction"] == "YES" and "-T" in best["ticker"]
            is_cold = best["direction"] == "NO" and "-T" in best["ticker"]
            if (is_warm and len(warm_cities) >= 4) or (is_cold and len(cold_cities) >= 4):
                size = size // 2
                print(f"    📉 Correlation reduction: sizing halved")
        contracts = max(1, size // entry_cents)
        
        # FIX 4: Cap total exposure per market (series+date) — max $20 (2000 cents)
        # One "market" = all brackets for a city+date (e.g., all Chicago Feb 22 high temp trades)
        MAX_MARKET_EXPOSURE = 1500  # $15 per market
        existing_exposure = 0
        for t in trades_data.get("trades", []):
            if t.get("status") == "open" and t.get("city") == city:
                t_parsed_parts = t.get("ticker", "").split("-")
                if len(t_parsed_parts) >= 2 and t_parsed_parts[1] == target_date_str:
                    # Use market_exposure if available (accurate for synced positions),
                    # fall back to cost_cents, fall back to contracts * entry_price
                    t_exposure = t.get("market_exposure") or t.get("cost_cents") or (
                        t.get("contracts", 0) * t.get("entry_price_cents", 0))
                    existing_exposure += t_exposure
        remaining_market_budget = max(0, MAX_MARKET_EXPOSURE - existing_exposure)
        if contracts * entry_cents > remaining_market_budget:
            contracts = max(1, remaining_market_budget // entry_cents)
            if remaining_market_budget < entry_cents:
                print(f"    Skip — market exposure cap reached (${existing_exposure/100:.2f}/${MAX_MARKET_EXPOSURE/100:.2f})")
                try:
                    city_logger.log_trade_decision(city=city, action="skip", ticker=best["ticker"],
                        direction=best["direction"], edge=best["edge"], entry_price=entry_cents/100,
                        contracts=0, cost_cents=0, target_date=target_date_str,
                        reason="market_exposure_cap")
                except Exception:
                    pass
                continue
        
        cost = contracts * entry_cents
        
        if cost > tradeable:
            print(f"    Skip — trade cost ${cost/100:.2f} exceeds tradeable ${tradeable/100:.2f}")
            continue
        
        # === MICROSTRUCTURE CHECK (spread, depth, quote age) ===
        _ms_pass, _ms_reason, _ms_details = check_market_microstructure(
            best["ticker"], best["direction"], contracts, _CFG)
        if not _ms_pass:
            print(f"    🚫 Microstructure reject: {_ms_reason} {_ms_details}")
            log_decision({
                "action": "skip",
                "ticker": best["ticker"],
                "direction": best["direction"],
                "entry_price_cents": entry_cents,
                "contracts": contracts,
                "reason": _ms_reason,
                "city": city,
                "ensemble_mean": mean,
                "ensemble_std": std,
                "raw_std": stats.get("ensemble_std", 3.0),
                "sigma_multiplier": sigma_multiplier,
                "sigma_range_low": mean - std,
                "sigma_range_high": mean + std,
                **_ms_details,
            })
            continue
        
        # Log as paper trade (full context journal entry)
        hypothetical_tracker.log_hypothetical(
            city=city, ticker=best["ticker"], direction=best["direction"],
            edge=best["edge"], entry_price_cents=entry_cents,
            reason_skipped=None,  # None = WOULD HAVE TRADED
            ensemble_mean=mean, ensemble_std=std,
            source_spread=stats.get("source_spread_f", 0) or 0,
            our_probability=best.get("our_prob"),
            market_probability=best.get("market_price"),
            kelly_fraction=kelly_f,
            contracts=contracts,
            expected_profit_cents=round(best["edge"] * contracts * 100, 1) if best.get("edge") else None,
            strike=best.get("strike"),
            bracket=best.get("strike_type"),
            target_date=str(target_date),
            sigma_multiplier=sigma_multiplier,
            sources_used=list(forecasts.keys()),
            source_forecasts={k: round(v, 1) for k, v in forecasts.items() if v is not None},
            notes=f"WOULD TRADE: {contracts}x {best['direction']} @ {entry_cents}¢ = ${contracts*entry_cents/100:.2f}",
        )
        
        # Place it
        try:
            result = kalshi_client.place_order(
                ticker=best["ticker"],
                side=best["direction"].lower(),
                contracts=contracts,
                price_cents=entry_cents,
            )
            
            order_data = result.get("order", {})
            order_id = order_data.get("order_id", "unknown")
            order_status = order_data.get("status", "unknown")
            filled = order_data.get("fill_count", 0)
            
            # Check if order actually filled
            if order_status == "resting" or filled == 0:
                # Order didn't fill — cancel it, don't record as a trade
                print(f"    ⏳ Order resting (not filled) — canceling {order_id}")
                try:
                    kalshi_client.cancel_order(order_id)
                except:
                    pass
                continue
            
            # Partial fill — adjust contracts to what actually filled
            if filled < contracts:
                print(f"    ⚠ Partial fill: {filled}/{contracts} — adjusting")
                contracts = filled
                cost = contracts * entry_cents
            
            # Track slippage (intended vs actual fill)
            try:
                actual_avg = order_data.get("avg_fill_price") or entry_cents
                slippage_tracker.record(
                    ticker=best["ticker"], side=best["direction"],
                    intended_price_cents=entry_cents,
                    actual_fill_price_cents=actual_avg,
                    contracts=contracts, order_type="entry",
                )
            except:
                pass
            
            if is_add and best["ticker"] in open_tickers:
                # Add to existing position
                existing = open_tickers[best["ticker"]]
                existing["contracts"] = existing.get("contracts", 0) + contracts
                existing["cost_cents"] = existing.get("cost_cents", 0) + cost
                existing["position_count"] = existing.get("position_count", 0) + contracts
                existing["market_exposure"] = existing.get("market_exposure", 0) + cost
                existing.setdefault("order_ids", []).append(order_id)
                # Update pm_original_contracts so PM knows the new total
                existing["pm_original_contracts"] = existing["contracts"]
                # Weighted average entry price
                total_contracts = existing["contracts"]
                existing["entry_price_cents"] = existing["cost_cents"] // total_contracts
                print(f"    ✓ Added {best['direction']} x{contracts} @ {entry_cents}¢ (${cost/100:.2f}) — now {total_contracts} total")
                
                # Journal: ADD
                conviction_note = "high (std≤1.5, +50%)" if std <= 1.5 else "low (std≥4.0, -40%)" if std >= 4.0 else "normal"
                trade_journal.log_action(
                    action="ADD", ticker=best["ticker"], direction=best["direction"],
                    contracts=contracts, price_cents=entry_cents,
                    city=city, series=series,
                    reasoning=f"Adding to existing position — edge {best['edge']:.1%} still strong, {len(forecasts)} sources agree on {mean:.1f}°F ± {std:.1f}°F",
                    forecast_snapshot=forecasts, ensemble_mean=mean, ensemble_std=std,
                    source_spread=stats.get("source_spread_f"),
                    edge=best["edge"], our_prob=best["our_prob"], market_price=best["market_price"],
                    conviction=conviction_note,
                    position_context={"total_contracts_after": total_contracts, "city_date_count": city_date_count},
                    extra={"training_forecasts": training_forecasts} if training_forecasts else None,
                )
            else:
                # New position
                trades_data["trades"].append({
                    "ticker": best["ticker"],
                    "series": series,
                    "city": city,
                    "direction": best["direction"],
                    "entry_price_cents": entry_cents,
                    "contracts": contracts,
                    "cost_cents": cost,
                    "fees_cents": 0,
                    "timestamp": now.isoformat(),
                    "status": "open",
                    "mode": "LIVE",
                    "order_ids": [order_id],
                    "position_count": contracts,
                    "market_exposure": cost,
                    "realized_pnl_cents": 0,
                    "pnl_cents": None,
                    "result": None,
                    "our_prob": best["our_prob"],
                    "edge": best["edge"],
                    "market_price_at_entry": best["market_price"],
                    "entry_forecast_high": mean,
                    "entry_forecast_std": std,
                    "entry_raw_std": stats.get("ensemble_std", 3.0),
                    "entry_sigma_multiplier": sigma_multiplier,
                    "entry_sigma_range_low": mean - std,
                    "entry_sigma_range_high": mean + std,
                    "entry_source_spread": stats.get("source_spread_f"),
                    "strike_type": best.get("strike_type", "bracket"),
                    "source": "fast_scanner",
                })
                # Track the new ticker as open
                new_trade = trades_data["trades"][-1]
                open_tickers[best["ticker"]] = new_trade
                city_date_open[(city, target_date_str)] = city_date_open.get((city, target_date_str), 0) + 1
                print(f"    ✓ Placed {best['direction']} x{contracts} @ {entry_cents}¢ (${cost/100:.2f})")
                
                # Per-city trade log
                try:
                    city_logger.log_trade_decision(
                        city=city, action="trade", ticker=best["ticker"],
                        direction=best["direction"], edge=best["edge"],
                        entry_price=entry_cents/100, contracts=contracts,
                        cost_cents=cost, target_date=target_date_str,
                        reason=f"kelly_f={kelly_f:.3f} kelly_raw={kelly_raw:.3f} std={std:.1f}",
                    )
                except Exception:
                    pass
                
                # Edge calibration: record prediction for later outcome tracking
                try:
                    edge_calibration.record_prediction(
                        ticker=best["ticker"], direction=best["direction"],
                        our_prob=best["our_prob"], market_price=best["market_price"],
                        edge=best["edge"], city=city,
                    )
                except:
                    pass
                
                # Journal: ENTRY or RE_ENTRY
                is_reentry = best["ticker"] in lost_tickers
                conviction_note = "high (std≤1.5, +50%)" if std <= 1.5 else "low (std≥4.0, -40%)" if std >= 4.0 else "normal"
                corr_note = " | correlation-reduced 50%" if portfolio_correlated else ""
                peak_note = f" | peak={'confirmed' if past_peak else 'not yet'}" if target_date == today else ""
                trade_journal.log_action(
                    action="RE_ENTRY" if is_reentry else "ENTRY",
                    ticker=best["ticker"], direction=best["direction"],
                    contracts=contracts, price_cents=entry_cents,
                    city=city, series=series,
                    reasoning=(
                        f"{'Re-entering lost ticker — ' if is_reentry else ''}"
                        f"{len(forecasts)} sources → ensemble {mean:.1f}°F ± {std:.1f}°F, "
                        f"edge {best['edge']:.1%} on {best['direction']} @ {entry_cents}¢ "
                        f"(our prob {best['our_prob']:.0%} vs market {best['market_price']:.0%})"
                        f"{corr_note}{peak_note}"
                    ),
                    forecast_snapshot=forecasts, ensemble_mean=mean, ensemble_std=std,
                    source_spread=stats.get("source_spread_f"),
                    edge=best["edge"], our_prob=best["our_prob"], market_price=best["market_price"],
                    current_temp=current_max,
                    conviction=conviction_note,
                    peak_info={"past_peak": past_peak, "peak_hour": peak_hour} if target_date == today else None,
                    position_context={
                        "city_date_count": city_date_count + 1,
                        "portfolio_correlated": portfolio_correlated,
                        "available_cash_cents": available,
                    },
                    extra={"training_forecasts": training_forecasts} if training_forecasts else None,
                )
            
            available -= cost
            new_trades += 1
            
        except Exception as e:
            print(f"    ✗ Order failed: {e}")
    
    # Save trades with file lock
    with open(TRADES_FILE, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        json.dump(trades_data, f, indent=2)
        f.flush()
        fcntl.flock(f, fcntl.LOCK_UN)
    
    elapsed = time.time() - start
    print(f"  Done in {elapsed:.1f}s — {new_trades} new trades")
    
    if new_trades > 0:
        add_notification(f"⚡ Fast Scanner placed {new_trades} trade(s) at {now.strftime('%H:%M UTC')}")


if __name__ == "__main__":
    run()
