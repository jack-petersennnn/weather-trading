#!/usr/bin/env python3
"""Weather Analyzer v3.1 — Weighted ensemble with city calibration, per-city strategies, dynamic thresholds."""

import json
import os
import urllib.request
import urllib.error
import math
import re
import statistics
import time
from datetime import datetime, timezone, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MARKETS_FILE = os.path.join(BASE_DIR, "active-markets.json")
OUTPUT = os.path.join(BASE_DIR, "analysis.json")
WEIGHTS_FILE = os.path.join(BASE_DIR, "source_weights.json")
CALIBRATION_FILE = os.path.join(BASE_DIR, "city_calibration.json")
STRATEGIES_FILE = os.path.join(BASE_DIR, "city_strategies.json")
CITY_MODEL_CONFIG_FILE = os.path.join(BASE_DIR, "city_model_config.json")

_city_model_config_cache = None
_city_model_config_mtime = 0

def load_city_model_config():
    """Load per-city model weights, biases, and disabled models."""
    global _city_model_config_cache, _city_model_config_mtime
    try:
        mtime = os.path.getmtime(CITY_MODEL_CONFIG_FILE)
        if _city_model_config_cache and mtime == _city_model_config_mtime:
            return _city_model_config_cache
        with open(CITY_MODEL_CONFIG_FILE) as f:
            _city_model_config_cache = json.load(f)
            _city_model_config_mtime = mtime
            return _city_model_config_cache
    except Exception:
        return None


def get_city_weights(city):
    """Get per-city weights dict. Falls back to global weights if no city config."""
    config = load_city_model_config()
    if config and city in config.get("cities", {}):
        return config["cities"][city]["weights"]
    return None


def get_city_biases(city):
    """Get per-city bias corrections dict."""
    config = load_city_model_config()
    if config and city in config.get("cities", {}):
        return config["cities"][city].get("biases", {})
    return {}


def get_sigma_multiplier(city):
    """Get sigma multiplier for a city with hard cap enforcement."""
    config = load_city_model_config()
    if not config:
        return 1.0
    
    # Get city-specific multiplier
    multiplier = 1.0
    if city in config.get("cities", {}):
        multiplier = config["cities"][city].get("sigma_multiplier", 1.0)
    
    # Apply hard cap (0.85 ≤ k ≤ 1.50)
    hard_cap_range = config.get("sigma_multipliers", {}).get("hard_cap_range", [0.85, 1.50])
    min_cap, max_cap = hard_cap_range
    
    capped_multiplier = max(min_cap, min(max_cap, multiplier))
    
    if capped_multiplier != multiplier:
        print(f"⚠️  Sigma multiplier for {city} capped: {multiplier:.2f} → {capped_multiplier:.2f}")
    
    return capped_multiplier


def get_sigma_multipliers_config():
    """Get the full sigma multipliers configuration."""
    config = load_city_model_config()
    if config and "sigma_multipliers" in config:
        return config["sigma_multipliers"]
    return {}

CITIES = {
    "KXHIGHNY":  {"city": "New York",    "lat": 40.7831, "lon": -73.9712, "nws_station": "OKX", "tz": "America/New_York"},
    "KXHIGHCHI": {"city": "Chicago",     "lat": 41.8781, "lon": -87.6298, "nws_station": "LOT", "tz": "America/Chicago"},
    "KXHIGHMIA": {"city": "Miami",       "lat": 25.7617, "lon": -80.1918, "nws_station": "MFL", "tz": "America/New_York"},
    "KXHIGHDEN": {"city": "Denver",      "lat": 39.7392, "lon": -104.9903,"nws_station": "BOU", "tz": "America/Denver"},
    "KXHIGHLAX": {"city": "Los Angeles", "lat": 34.0522, "lon": -118.2437,"nws_station": "LOX", "tz": "America/Los_Angeles"},
    "KXHIGHAUS": {"city": "Austin",      "lat": 30.2672, "lon": -97.7431, "nws_station": "EWX", "tz": "America/Chicago"},
}


class RateLimitError(Exception):
    pass


def fetch_json(url, timeout=15, retries=2):
    req = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "User-Agent": "KingClaw-Weather/3.1"
    })
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                if attempt < retries:
                    wait = 2 ** (attempt + 1)  # 2s, 4s
                    print(f"      ⏳ 429 Rate Limited, retry in {wait}s...", end=" ")
                    time.sleep(wait)
                    continue
                print(f"      ✗ HTTP 429 Rate Limited (after {retries} retries)")
                raise RateLimitError(f"429 from {url}")
            print(f"      ✗ {e}")
            return None
        except Exception as e:
            print(f"      ✗ {e}")
            return None


# ── Configuration Loading ─────────────────────────────────────────

def load_source_weights():
    defaults = {
        "NWS Hourly": 1.5, "ECMWF": 1.4, "NWS Forecast": 1.2,
        "Best Match": 1.1, "GFS": 1.0, "Ensemble ICON": 0.9,
        "Ensemble GFS": 0.9, "Ensemble ECMWF": 0.9,
        "Tomorrow.io": 1.0, "Visual Crossing": 1.0,
    }
    try:
        if os.path.exists(WEIGHTS_FILE):
            with open(WEIGHTS_FILE) as f:
                data = json.load(f)
            # Support both formats:
            # 1. Nested: {"weights": {...}, "city_weights": {...}}
            # 2. Flat: {"GFS": 1.537, "ICON": 0.979, ...}
            if "weights" in data:
                return data["weights"], data.get("city_weights", {})
            else:
                # Flat dict = global weights directly, no city overrides
                return data, {}
    except Exception:
        pass
    return defaults, {}


def load_city_calibration():
    defaults = {
        "New York": 1.3, "Chicago": 1.2, "Miami": 0.85,
        "Denver": 1.4, "Los Angeles": 0.8, "Austin": 1.1,
    }
    try:
        if os.path.exists(CALIBRATION_FILE):
            with open(CALIBRATION_FILE) as f:
                data = json.load(f)
            cals = data.get("calibrations", {})
            return {city: c.get("adjusted_std_multiplier", defaults.get(city, 1.0))
                    for city, c in cals.items()}
    except Exception:
        pass
    return defaults


def load_city_strategies():
    """Load per-city strategy configuration."""
    try:
        if os.path.exists(STRATEGIES_FILE):
            with open(STRATEGIES_FILE) as f:
                return json.load(f)
    except Exception as e:
        print(f"  Warning: Could not load city_strategies.json: {e}")
    return None


def get_city_strategy(strategies, city):
    """Get strategy config for a city, falling back to defaults."""
    if not strategies:
        return {
            "style": "balanced", "edge_threshold_base": 0.12,
            "max_trades_per_day": 3, "kelly_multiplier": 0.25,
            "capital_allocation_pct": 8.0, "min_sources_required": 7,
            "max_source_spread_f": 6.0, "skip_if_bimodal": False,
        }
    cities = strategies.get("cities", {})
    defaults = strategies.get("defaults", {})
    return cities.get(city, defaults)


# ── NWS Gridpoint Cache (precomputed, saves 1 API call per source per city) ──

NWS_GRIDPOINTS = {
    "New York": "OKX/34,38",
    "Chicago": "LOT/72,69",
    "Miami": "MFL/106,51",
    "Denver": "BOU/74,66",
    "Austin": "EWX/159,88",
    "Minneapolis": "MPX/110,68",
    "Washington DC": "LWX/97,69",
    "Atlanta": "FFC/50,82",
    "Philadelphia": "PHI/48,72",
    "Houston": "HGX/64,104",
    "Dallas": "FWD/80,109",
    "Seattle": "SEW/124,61",
    "Boston": "BOX/73,91",
    "Phoenix": "PSR/161,57",
    "Oklahoma City": "OUN/94,90",
    "Las Vegas": "VEF/122,94",
    "San Francisco": "MTR/85,98",
    "San Antonio": "EWX/127,59",
    "New Orleans": "LIX/60,90",
}

# Reverse lookup: lat,lon → city name (for gridpoint resolution)
_LATLON_TO_CITY = {}
for _series, _cfg in CITIES.items():
    _LATLON_TO_CITY[(round(_cfg["lat"], 4), round(_cfg["lon"], 4))] = _cfg["city"]


def _get_gridpoint(lat, lon):
    """Get NWS gridpoint string for a lat/lon, using cache first."""
    city = _LATLON_TO_CITY.get((round(lat, 4), round(lon, 4)))
    if city and city in NWS_GRIDPOINTS:
        return NWS_GRIDPOINTS[city]
    # Fallback: API lookup
    points = fetch_json(f"https://api.weather.gov/points/{lat},{lon}")
    if not points:
        return None
    props = points.get("properties", {})
    office = props.get("gridId")
    gx = props.get("gridX")
    gy = props.get("gridY")
    if all([office, gx, gy]):
        return f"{office}/{gx},{gy}"
    return None


def source_nws_hourly_fast(lat, lon, target_date):
    """NWS Hourly forecast using cached gridpoints (1 API call instead of 2)."""
    print("    [NWS-Hourly] NWS Hourly (cached grid)...", end=" ")
    gridpoint = _get_gridpoint(lat, lon)
    if not gridpoint:
        print("FAIL (no gridpoint)")
        return None
    url = f"https://api.weather.gov/gridpoints/{gridpoint}/forecast/hourly"
    hdata = fetch_json(url)
    if not hdata:
        print("FAIL")
        return None
    temps = []
    for p in hdata.get("properties", {}).get("periods", []):
        start = p.get("startTime", "")[:10]
        if start == str(target_date):
            temps.append(p.get("temperature"))
    if temps:
        mx = max(temps)
        print(f"OK → {mx}°F (from {len(temps)} hours)")
        return mx
    print("FAIL (no matching hours)")
    return None


# ── Weather Alerts ────────────────────────────────────────────────

# Alert types that significantly impact temperature predictability
SEVERE_ALERT_TYPES = {
    "Blizzard Warning", "Ice Storm Warning", "Winter Storm Warning",
    "Extreme Cold Warning", "Extreme Heat Warning",
    "Tornado Warning", "Severe Thunderstorm Warning",
}
# Alert types that moderately increase forecast uncertainty
CAUTION_ALERT_TYPES = {
    "Winter Storm Watch", "Winter Weather Advisory",
    "Heat Advisory", "Cold Weather Advisory",
    "Freeze Warning", "Frost Advisory",
    "Wind Advisory", "High Wind Warning",
    "Severe Thunderstorm Watch", "Tornado Watch",
    "Extreme Cold Watch", "Extreme Heat Watch",
}

_alert_cache = {}  # (lat,lon) -> (timestamp, alerts)
_ALERT_CACHE_TTL = 900  # 15 minutes


def check_weather_alerts(lat, lon):
    """Check active NWS weather alerts for a location.
    Returns dict:
        {
            "has_severe": bool,      # Active severe warnings
            "has_caution": bool,     # Active watches/advisories
            "alerts": list,          # List of alert event names
            "skip_trading": bool,    # Recommendation to skip
            "confidence_penalty": float,  # 0.0 to 0.5 — reduce ensemble confidence by this
        }
    """
    import time as _time
    cache_key = (round(lat, 2), round(lon, 2))
    now = _time.time()
    
    if cache_key in _alert_cache:
        cached_time, cached_result = _alert_cache[cache_key]
        if now - cached_time < _ALERT_CACHE_TTL:
            return cached_result
    
    result = {"has_severe": False, "has_caution": False, "alerts": [],
              "skip_trading": False, "confidence_penalty": 0.0}
    
    try:
        url = f"https://api.weather.gov/alerts/active?point={lat},{lon}&status=actual&urgency=Immediate,Expected"
        data = fetch_json(url)
        if data:
            for f in data.get("features", []):
                event = f.get("properties", {}).get("event", "")
                result["alerts"].append(event)
                if event in SEVERE_ALERT_TYPES:
                    result["has_severe"] = True
                elif event in CAUTION_ALERT_TYPES:
                    result["has_caution"] = True
        
        if result["has_severe"]:
            result["skip_trading"] = True
            result["confidence_penalty"] = 0.5
        elif result["has_caution"]:
            result["confidence_penalty"] = 0.2
    except Exception:
        pass  # Don't let alert check failure block trading
    
    _alert_cache[cache_key] = (now, result)
    return result


# ── Forecast Sources ──────────────────────────────────────────────

def source_nws_forecast(lat, lon, target_date):
    print("    [1/10] NWS Forecast (cached grid)...", end=" ")
    gridpoint = _get_gridpoint(lat, lon)
    if not gridpoint:
        print("FAIL (no gridpoint)")
        return None
    forecast_url = f"https://api.weather.gov/gridpoints/{gridpoint}/forecast"
    fdata = fetch_json(forecast_url)
    if not fdata:
        print("FAIL")
        return None
    for p in fdata.get("properties", {}).get("periods", []):
        if not p.get("isDaytime"):
            continue
        start = p.get("startTime", "")[:10]
        if start == target_date:
            temp = p.get("temperature")
            print(f"OK → {temp}°F")
            return temp
    for p in fdata.get("properties", {}).get("periods", []):
        if p.get("isDaytime"):
            temp = p.get("temperature")
            print(f"OK → {temp}°F (first daytime)")
            return temp
    print("FAIL (no daytime period)")
    return None


def source_nws_hourly(lat, lon, target_date):
    print("    [2/10] NWS Hourly...", end=" ")
    points = fetch_json(f"https://api.weather.gov/points/{lat},{lon}")
    if not points:
        print("FAIL")
        return None
    props = points.get("properties", {})
    office = props.get("gridId")
    gx = props.get("gridX")
    gy = props.get("gridY")
    if not all([office, gx, gy]):
        print("FAIL (no grid)")
        return None
    url = f"https://api.weather.gov/gridpoints/{office}/{gx},{gy}/forecast/hourly"
    hdata = fetch_json(url)
    if not hdata:
        print("FAIL")
        return None
    temps = []
    for p in hdata.get("properties", {}).get("periods", []):
        start = p.get("startTime", "")[:10]
        if start == target_date:
            temps.append(p.get("temperature"))
    if temps:
        mx = max(temps)
        print(f"OK → {mx}°F (from {len(temps)} hours)")
        return mx
    print("FAIL (no matching hours)")
    return None


def _open_meteo(label, num, url_path, lat, lon, tz, target_date, extra_params=""):
    print(f"    [{num}/10] {label}...", end=" ")
    tz_encoded = tz.replace("/", "%2F")
    base = "ensemble-api.open-meteo.com" if url_path == "ensemble" else "api.open-meteo.com"
    url = (f"https://{base}/v1/{url_path}?"
           f"latitude={lat}&longitude={lon}"
           f"&daily=temperature_2m_max&temperature_unit=fahrenheit"
           f"&timezone={tz_encoded}&forecast_days=3{extra_params}")
    data = fetch_json(url)
    if not data or "daily" not in data:
        print("FAIL")
        return None
    dates = data["daily"].get("time", [])
    maxes = data["daily"].get("temperature_2m_max", [])
    target_str = str(target_date)  # Handle both date objects and strings
    for d, t in zip(dates, maxes):
        if d == target_str and t is not None:
            print(f"OK → {t}°F")
            return t
    print(f"FAIL (date {target_str} not in {dates})")
    return None


def source_ecmwf(lat, lon, tz, target_date):
    return _open_meteo("Open-Meteo ECMWF", 3, "ecmwf", lat, lon, tz, target_date)

def source_gfs(lat, lon, tz, target_date):
    return _open_meteo("Open-Meteo GFS", 4, "gfs", lat, lon, tz, target_date)

def source_icon(lat, lon, tz, target_date):
    return _open_meteo("ICON", 4.5, "forecast", lat, lon, tz, target_date,
                       "&models=icon_seamless")

def source_best_match(lat, lon, tz, target_date):
    return _open_meteo("Open-Meteo Best Match", 5, "forecast", lat, lon, tz, target_date)

def source_ensemble_icon(lat, lon, tz, target_date):
    return _open_meteo("Ensemble ICON", 6, "ensemble", lat, lon, tz, target_date,
                       "&models=icon_seamless")

def source_ensemble_gfs(lat, lon, tz, target_date):
    return _open_meteo("Ensemble GFS", 7, "ensemble", lat, lon, tz, target_date,
                       "&models=gfs_seamless")

def source_ensemble_ecmwf(lat, lon, tz, target_date):
    return _open_meteo("Ensemble ECMWF", 8, "ensemble", lat, lon, tz, target_date,
                       "&models=ecmwf_ifs025")


def source_tomorrow_io(lat, lon, tz, target_date):
    print("    [9/10] Tomorrow.io...", end=" ")
    key = os.environ.get("TOMORROW_IO_API_KEY")
    if not key:
        print("SKIP (no API key)")
        return None
    url = (f"https://api.tomorrow.io/v4/weather/forecast"
           f"?location={lat},{lon}&apikey={key}&units=imperial")
    data = fetch_json(url)
    if not data:
        print("FAIL")
        return None
    try:
        timelines = data.get("timelines", {})
        daily = timelines.get("daily", [])
        for day in daily:
            day_date = day.get("time", "")[:10]
            if day_date == target_date:
                temp = day.get("values", {}).get("temperatureMax")
                if temp is not None:
                    print(f"OK → {temp}°F")
                    return float(temp)
        if daily:
            temp = daily[0].get("values", {}).get("temperatureMax")
            if temp is not None:
                print(f"OK → {temp}°F (first day)")
                return float(temp)
    except Exception as e:
        print(f"FAIL ({e})")
        return None
    print("FAIL (no data)")
    return None


def _open_meteo_hourly_max(label, model_name, lat, lon, tz, target_date):
    """Fetch hourly temps from an Open-Meteo model-specific endpoint and return the daily max."""
    print(f"    [--] {label}...", end=" ")
    url = (f"https://api.open-meteo.com/v1/forecast?"
           f"latitude={lat}&longitude={lon}&models={model_name}"
           f"&hourly=temperature_2m&temperature_unit=fahrenheit"
           f"&start_date={target_date}&end_date={target_date}")
    data = fetch_json(url)
    if not data or "hourly" not in data:
        print("FAIL")
        return None
    temps = data["hourly"].get("temperature_2m", [])
    valid = [t for t in temps if t is not None]
    if not valid:
        print("FAIL (no valid temps)")
        return None
    mx = max(valid)
    print(f"OK → {mx}°F (max of {len(valid)} hourly)")
    return mx


def source_hrrr(lat, lon, tz, target_date):
    return _open_meteo_hourly_max("HRRR (3km)", "ncep_hrrr_conus", lat, lon, tz, target_date)

def source_canadian_gem(lat, lon, tz, target_date):
    return _open_meteo_hourly_max("Canadian GEM", "gem_global", lat, lon, tz, target_date)

def source_jma(lat, lon, tz, target_date):
    return _open_meteo_hourly_max("JMA", "jma_gsm", lat, lon, tz, target_date)

def source_ukmo(lat, lon, tz, target_date):
    return _open_meteo_hourly_max("UKMO (10km)", "ukmo_global_deterministic_10km", lat, lon, tz, target_date)

def source_meteo_france_arpege(lat, lon, tz, target_date):
    return _open_meteo_hourly_max("Meteo-France Arpege", "arpege_world", lat, lon, tz, target_date)

def source_met_norway(lat, lon, tz, target_date):
    return _open_meteo_hourly_max("MET Norway", "metno_seamless", lat, lon, tz, target_date)


def source_visual_crossing(lat, lon, tz, target_date):
    print(f"    [10/10] Visual Crossing...", end=" ")
    key = os.environ.get("VISUAL_CROSSING_API_KEY")
    if not key:
        print("SKIP (no API key)")
        return None
    url = (f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
           f"/{lat},{lon}/{target_date}?unitGroup=us&key={key}&contentType=json&include=days")
    data = fetch_json(url)
    if not data:
        print("FAIL")
        return None
    try:
        days = data.get("days", [])
        if days:
            temp = days[0].get("tempmax")
            if temp is not None:
                print(f"OK → {temp}°F")
                return float(temp)
    except Exception as e:
        print(f"FAIL ({e})")
        return None
    print("FAIL (no data)")
    return None


ALL_SOURCES = [
    ("NWS Forecast",      source_nws_forecast),
    ("NWS Hourly",        source_nws_hourly),
    ("ECMWF",             source_ecmwf),
    ("GFS",               source_gfs),
    ("ICON",              source_icon),
    # "Best Match" removed — it's a GFS duplicate (Open-Meteo best_match defaults to GFS for US locations)
    ("Ensemble ICON",     source_ensemble_icon),
    ("Ensemble GFS",      source_ensemble_gfs),
    ("Ensemble ECMWF",    source_ensemble_ecmwf),
    ("Tomorrow.io",       source_tomorrow_io),
    ("Visual Crossing",   source_visual_crossing),
    ("HRRR",              source_hrrr),
    ("Canadian GEM",      source_canadian_gem),
    ("JMA",               source_jma),
    ("UKMO",              source_ukmo),
    ("Meteo-France Arpege", source_meteo_france_arpege),
    ("MET Norway",        source_met_norway),
]

_rate_limited_sources = set()


def collect_forecasts_batched(lat, lon, tz, target_date, lightweight=False):
    """Collect forecasts from all sources using batched Open-Meteo API calls.
    
    Instead of 13+ individual API calls per city, this makes 3-6:
      1. Batched forecast models (GFS, ICON, GEM, JMA, UKMO, Arpege)
      2. ECMWF (separate endpoint)
      3. Batched ensemble models (ECMWF, ICON, GFS ensembles)
      4. NWS (separate API entirely)
      5. HRRR + MET Norway (individual Open-Meteo calls)
      6. Tomorrow.io + Visual Crossing (separate APIs)
    
    If lightweight=True, skips batches 4b-6 (HRRR, MET Norway, Tomorrow.io, 
    Visual Crossing, NWS). Used by Position Manager to save API calls.
    Lightweight = 3 calls/city vs full = 7-8 calls/city.
    
    Returns dict of {source_name: forecast_high_temp_F}
    """
    results = {}
    tz_encoded = tz.replace("/", "%2F")
    target_str = str(target_date)
    
    # === Batch 1: Forecast models (single call) ===
    # Note: hrrr_conus can't batch with other models (API rejects it).
    # metno_nordic is regional (Nordic only), won't return data for US cities.
    forecast_models = "gfs_seamless,icon_seamless,gem_global,jma_gsm,ukmo_global_deterministic_10km,arpege_world"
    # Model name → our source name mapping
    forecast_model_map = {
        "temperature_2m_max_gfs_seamless": "GFS",
        "temperature_2m_max_icon_seamless": "ICON",
        "temperature_2m_max_gem_global": "Canadian GEM",
        "temperature_2m_max_jma_gsm": "JMA",
        "temperature_2m_max_ukmo_global_deterministic_10km": "UKMO",
        "temperature_2m_max_arpege_world": "Meteo-France Arpege",
    }
    try:
        print(f"    [Batch 1] Forecast models (6 models)...", end=" ")
        url = (f"https://api.open-meteo.com/v1/forecast?"
               f"latitude={lat}&longitude={lon}&models={forecast_models}"
               f"&daily=temperature_2m_max&temperature_unit=fahrenheit"
               f"&timezone={tz_encoded}&forecast_days=3")
        data = fetch_json(url)
        if data and "daily" in data:
            dates = data["daily"].get("time", [])
            count = 0
            for key, source_name in forecast_model_map.items():
                vals = data["daily"].get(key, [])
                for d, v in zip(dates, vals):
                    if d == target_str and v is not None:
                        results[source_name] = float(v)
                        count += 1
                        break
            print(f"OK → {count}/7 models returned data")
        else:
            print("FAIL")
    except RateLimitError:
        print("RATE LIMITED")
    except Exception as e:
        print(f"FAIL ({e})")
    
    time.sleep(2)
    
    # === Batch 2: ECMWF (separate endpoint) ===
    try:
        print(f"    [Batch 2] ECMWF...", end=" ")
        url = (f"https://api.open-meteo.com/v1/ecmwf?"
               f"latitude={lat}&longitude={lon}"
               f"&daily=temperature_2m_max&temperature_unit=fahrenheit"
               f"&timezone={tz_encoded}&forecast_days=3")
        data = fetch_json(url)
        if data and "daily" in data:
            dates = data["daily"].get("time", [])
            maxes = data["daily"].get("temperature_2m_max", [])
            for d, t in zip(dates, maxes):
                if d == target_str and t is not None:
                    results["ECMWF"] = float(t)
                    print(f"OK → {t}°F")
                    break
            else:
                print(f"FAIL (no data for {target_str})")
        else:
            print("FAIL")
    except RateLimitError:
        print("RATE LIMITED")
    except Exception as e:
        print(f"FAIL ({e})")
    
    time.sleep(2)
    
    # === Batch 3: Ensemble models (single call, parse members per model) ===
    # Each ensemble model has N perturbed members. We compute the MEAN of all members.
    # ECMWF: 50 members (suffix: _ecmwf_ifs025_ensemble)
    # GFS: 30 members (suffix: _ncep_gefs_seamless)
    # ICON: ~39 members (suffix: _icon_seamless_eps)
    # The API also returns a "control" run — we use member mean instead.
    ensemble_member_suffixes = {
        "_ecmwf_ifs025_ensemble": "Ensemble ECMWF",
        "_ncep_gefs_seamless": "Ensemble GFS",
        "_icon_seamless_eps": "Ensemble ICON",
    }
    try:
        print(f"    [Batch 3] Ensemble models (3 models)...", end=" ")
        url = (f"https://ensemble-api.open-meteo.com/v1/ensemble?"
               f"latitude={lat}&longitude={lon}&models=ecmwf_ifs025,icon_seamless,gfs_seamless"
               f"&daily=temperature_2m_max&temperature_unit=fahrenheit"
               f"&timezone={tz_encoded}&forecast_days=3")
        data = fetch_json(url)
        if data and "daily" in data:
            dates = data["daily"].get("time", [])
            date_idx = None
            for i, d in enumerate(dates):
                if d == target_str:
                    date_idx = i
                    break
            
            ens_count = 0
            if date_idx is not None:
                # Group member keys by model suffix
                for suffix, source_name in ensemble_member_suffixes.items():
                    member_vals = []
                    for key, vals in data["daily"].items():
                        if "member" in key and key.endswith(suffix):
                            if date_idx < len(vals) and vals[date_idx] is not None:
                                member_vals.append(float(vals[date_idx]))
                    if member_vals:
                        ens_mean = sum(member_vals) / len(member_vals)
                        ens_std = (sum((v - ens_mean)**2 for v in member_vals) / len(member_vals)) ** 0.5
                        results[source_name] = round(ens_mean, 1)
                        results[f"__{source_name}_std"] = round(ens_std, 1)
                        results[f"__{source_name}_members"] = len(member_vals)
                        ens_count += 1
            print(f"OK → {ens_count}/3 computed from members")
        else:
            print("FAIL")
    except RateLimitError:
        print("RATE LIMITED")
    except Exception as e:
        print(f"FAIL ({e})")
    
    time.sleep(2)
    
    # === Remaining sources (skipped in lightweight mode for PM) ===
    if not lightweight:
        # === Batch 4: NWS (separate API, unlimited, cached gridpoints) ===
        # Use the fast cached-gridpoint version for hourly (1 call instead of 2)
        for name, fn in [("NWS Forecast", source_nws_forecast), ("NWS Hourly", source_nws_hourly_fast)]:
            try:
                val = fn(lat, lon, target_date)
                if val is not None:
                    results[name] = float(val)
            except Exception:
                pass
        
        # === Batch 4b: HRRR (can't batch with other models) ===
        try:
            val = source_hrrr(lat, lon, tz, target_date)
            if val is not None:
                results["HRRR"] = float(val)
        except Exception:
            pass
        
        # === Batch 4c: MET Norway ===
        try:
            val = source_met_norway(lat, lon, tz, target_date)
            if val is not None:
                results["MET Norway"] = float(val)
        except Exception:
            pass
        
        # === Batch 5: Tomorrow.io (separate API, own key) ===
        try:
            val = source_tomorrow_io(lat, lon, tz, target_date)
            if val is not None:
                results["Tomorrow.io"] = float(val)
        except Exception:
            pass
        
        # === Batch 6: Visual Crossing (separate API, own key) ===
        try:
            val = source_visual_crossing(lat, lon, tz, target_date)
            if val is not None:
                results["Visual Crossing"] = float(val)
        except Exception:
            pass
    
    return results


def collect_forecasts(lat, lon, tz, target_date):
    results = {}
    for name, fn in ALL_SOURCES:
        if name in _rate_limited_sources:
            print(f"    [?/10] {name}... SKIP (rate-limited this run)")
            continue
        try:
            if name.startswith("NWS"):
                val = fn(lat, lon, target_date)
            else:
                val = fn(lat, lon, tz, target_date)
            if val is not None:
                results[name] = float(val)
        except RateLimitError:
            _rate_limited_sources.add(name)
            print(f"      → {name} disabled for remaining cities this run")
        except Exception as e:
            print(f"      ✗ {name} exception: {e}")
    return results


# ── Weighted Ensemble Stats ───────────────────────────────────────

def weighted_ensemble_stats(forecasts, source_weights, city_multiplier=1.0, city=None):
    """Compute ensemble stats using family-first averaging + bias correction.
    
    Step 1: Apply per-city bias correction to each forecast (if city config exists)
    Step 2: Use per-city weights (disabling bad models for specific cities)
    Step 3: Average forecasts within each model family
    Step 4: Weight families by the average weight of their members
    
    This prevents any single model family from dominating the ensemble
    just because it has multiple variants (base + ensemble).
    """
    if not forecasts:
        return {}

    # Load per-city config if available
    city_weights = get_city_weights(city) if city else None
    city_biases = get_city_biases(city) if city else {}
    
    # Use per-city weights if available, otherwise fall back to global
    effective_weights = city_weights if city_weights else source_weights

    # Apply bias correction: subtract known bias from each forecast
    # (bias is negative = model reads low, so subtracting negative = adding)
    corrected_forecasts = {}
    for name, temp in forecasts.items():
        bias = city_biases.get(name, 0.0)
        corrected_forecasts[name] = temp - bias  # Subtract bias to correct
    
    # Model family mapping (same as fast_scanner.py MODEL_FAMILIES)
    SOURCE_FAMILIES = {
        "ECMWF": "ecmwf", "Ensemble ECMWF": "ecmwf",
        "GFS": "gfs", "Ensemble GFS": "gfs",
        "HRRR": "hrrr",
        "ICON": "icon", "Ensemble ICON": "icon",
        "Canadian GEM": "gem", "JMA": "jma", "UKMO": "ukmo",
        "Meteo-France Arpege": "arpege", "MET Norway": "metno",
        "NWS Forecast": "nws", "NWS Hourly": "nws",
        "Tomorrow.io": "tomorrow", "Visual Crossing": "visualcrossing",
    }
    
    # Step 1: Group sources by family, compute weighted average per family
    family_data = {}  # family -> [(temp, weight), ...]
    for name, temp in corrected_forecasts.items():
        w = effective_weights.get(name, source_weights.get(name, 1.0))
        if w <= 0:
            continue
        family = SOURCE_FAMILIES.get(name, name)
        if family not in family_data:
            family_data[family] = []
        family_data[family].append((temp, w))
    
    # Step 2: Compute one value per family (weighted average within family)
    family_temps = []  # [(family_avg_temp, family_avg_weight), ...]
    for family, members in family_data.items():
        total_w = sum(w for _, w in members)
        avg_temp = sum(t * w for t, w in members) / total_w
        avg_weight = total_w / len(members)  # Average weight, not sum — prevents family size advantage
        family_temps.append((avg_temp, avg_weight, family))
    
    if not family_temps:
        return {}
    
    # Step 3: Compute ensemble mean from family-level values
    total_family_weight = sum(w for _, w, _ in family_temps)
    w_mean = sum(t * w for t, w, _ in family_temps) / total_family_weight
    
    # Also keep raw source-level data for spread/std calculations
    # Use corrected forecasts for spread calc (spread after bias correction)
    temps = [corrected_forecasts[n] for n in corrected_forecasts if effective_weights.get(n, source_weights.get(n, 1.0)) > 0]
    names = [n for n in corrected_forecasts if effective_weights.get(n, source_weights.get(n, 1.0)) > 0]
    weights = [effective_weights.get(n, source_weights.get(n, 1.0)) for n in names]
    
    # Std computed from family-level values (not inflated by duplicate sources)
    if len(family_temps) > 1:
        variance = sum(w * (t - w_mean) ** 2 for t, w, _ in family_temps) / total_family_weight
        w_std = math.sqrt(variance)
    else:
        w_std = 3.0

    if w_std < 1.0:
        w_std = 1.5

    calibrated_std = w_std * city_multiplier

    sorted_temps = sorted(temps)
    bimodal = False
    cluster_info = None
    if len(sorted_temps) >= 4:
        gaps = [(sorted_temps[i+1] - sorted_temps[i], i) for i in range(len(sorted_temps)-1)]
        max_gap, gap_idx = max(gaps, key=lambda x: x[0])
        if max_gap > w_std * 1.5 and gap_idx >= 1 and gap_idx < len(sorted_temps) - 2:
            bimodal = True
            low_cluster = sorted_temps[:gap_idx+1]
            high_cluster = sorted_temps[gap_idx+1:]
            cluster_info = {
                "low_mean": statistics.mean(low_cluster),
                "low_std": max(statistics.pstdev(low_cluster), 1.0) if len(low_cluster) > 1 else 1.5,
                "low_weight": len(low_cluster) / len(sorted_temps),
                "high_mean": statistics.mean(high_cluster),
                "high_std": max(statistics.pstdev(high_cluster), 1.0) if len(high_cluster) > 1 else 1.5,
                "high_weight": len(high_cluster) / len(sorted_temps),
            }

    skewness = 0.0
    if len(temps) >= 3:
        try:
            m3 = sum((t - w_mean) ** 3 for t in temps) / len(temps)
            skewness = m3 / (w_std ** 3) if w_std > 0 else 0.0
        except Exception:
            pass

    sources_agreeing = sum(1 for t in temps if abs(t - w_mean) <= 3.0)

    # Source spread (max - min)
    source_spread = max(temps) - min(temps) if len(temps) > 1 else 0.0

    return {
        "ensemble_mean": round(w_mean, 1),
        "ensemble_median": round(statistics.median(temps), 1),
        "ensemble_std": round(w_std, 2),
        "calibrated_std": round(calibrated_std, 2),
        "ensemble_min": round(min(temps), 1),
        "ensemble_max": round(max(temps), 1),
        "confidence_interval_low": round(w_mean - calibrated_std, 1),
        "confidence_interval_high": round(w_mean + calibrated_std, 1),
        "sources_agreeing": sources_agreeing,
        "source_spread_f": round(source_spread, 1),
        "bimodal": bimodal,
        "cluster_info": cluster_info,
        "skewness": round(skewness, 3),
        "city_std_multiplier": round(city_multiplier, 3),
    }


# ── Probability / Market Logic ───────────────────────────────────

def norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def skew_norm_cdf(x, skew):
    delta = skew * 0.3
    adjusted_x = x - delta
    return norm_cdf(adjusted_x)


def _continuity_correct(floor, cap):
    """Apply continuity correction for integer-settled temperature markets.
    
    NWS reports whole-integer °F. Kalshi floor_strike/cap_strike are integers.
    
    Bracket (both floor AND cap present): '67° to 68°' floor=67, cap=68
      → P(66.5 < temp < 68.5) — both endpoints inclusive, so expand outward by 0.5
    
    Threshold greater (floor only): '69° or above' floor=68, cap=None  
      → P(temp > 68.5) — floor is EXCLUSIVE boundary, so shift inward by +0.5
    
    Threshold less (cap only): '60° or below' floor=None, cap=61
      → P(temp < 60.5) — cap is EXCLUSIVE boundary, so shift inward by -0.5
    """
    is_bracket = (floor is not None and cap is not None)
    if is_bracket:
        # Bracket: expand outward to include both endpoint integers
        cc_floor = floor - 0.5
        cc_cap = cap + 0.5
    else:
        # Threshold: boundary is exclusive, shift by 0.5 toward the "yes" side
        cc_floor = (floor + 0.5) if floor is not None else None
        cc_cap = (cap - 0.5) if cap is not None else None
    return cc_floor, cc_cap


def compute_probability(mean, std, floor, cap, skewness=0.0, bimodal=False, cluster_info=None, is_tail=False):
    if std <= 0:
        std = 3.0

    # Apply continuity correction for integer settlement
    cc_floor, cc_cap = _continuity_correct(floor, cap)

    if bimodal and cluster_info:
        prob = _mixture_probability(cluster_info, cc_floor, cc_cap)
    elif abs(skewness) > 0.5:
        prob = _skewed_probability(mean, std, cc_floor, cc_cap, skewness)
    else:
        prob = _normal_probability(mean, std, cc_floor, cc_cap)

    if is_tail:
        prob = prob * 0.85

    return max(0.001, min(0.999, prob))


def _normal_probability(mean, std, floor, cap):
    if floor is not None and cap is not None:
        return norm_cdf((cap - mean) / std) - norm_cdf((floor - mean) / std)
    elif floor is not None:
        return 1 - norm_cdf((floor - mean) / std)
    elif cap is not None:
        return norm_cdf((cap - mean) / std)
    return 0.5


def _skewed_probability(mean, std, floor, cap, skewness):
    if floor is not None and cap is not None:
        return skew_norm_cdf((cap - mean) / std, skewness) - skew_norm_cdf((floor - mean) / std, skewness)
    elif floor is not None:
        return 1 - skew_norm_cdf((floor - mean) / std, skewness)
    elif cap is not None:
        return skew_norm_cdf((cap - mean) / std, skewness)
    return 0.5


def _mixture_probability(cluster_info, floor, cap):
    prob = 0.0
    for prefix in ["low", "high"]:
        cm = cluster_info[f"{prefix}_mean"]
        cs = cluster_info[f"{prefix}_std"]
        cw = cluster_info[f"{prefix}_weight"]
        if floor is not None and cap is not None:
            p = norm_cdf((cap - cm) / cs) - norm_cdf((floor - cm) / cs)
        elif floor is not None:
            p = 1 - norm_cdf((floor - cm) / cs)
        elif cap is not None:
            p = norm_cdf((cap - cm) / cs)
        else:
            p = 0.5
        prob += cw * p
    return prob


def is_tail_bracket(floor, cap, mean, std):
    if floor is not None and cap is None:
        return floor > mean + 2 * std
    if cap is not None and floor is None:
        return cap < mean - 2 * std
    if floor is not None and cap is not None:
        mid = (floor + cap) / 2
        return abs(mid - mean) > 2.5 * std
    return False


# ── Dynamic Edge Threshold (now per-city strategy aware) ──────────

def compute_edge_threshold(city, std, sources_agreeing, days_out, strategy=None):
    """Compute dynamic edge threshold using per-city strategy base."""
    if strategy:
        base = strategy.get("edge_threshold_base", 0.12)
    else:
        base = 0.12
        city_adj = {
            "Denver": 0.06, "New York": 0.03, "Chicago": 0.03,
            "Austin": 0.01, "Miami": -0.02, "Los Angeles": -0.02,
        }
        base += city_adj.get(city, 0.0)

    threshold = base

    # Dynamic adjustments on top of strategy base
    if sources_agreeing >= 8:
        threshold = max(threshold - 0.04, 0.06)
    elif sources_agreeing >= 6:
        threshold = max(threshold - 0.02, 0.06)

    if days_out == 0:
        threshold = max(threshold - 0.03, 0.04)
    elif days_out >= 2:
        threshold += 0.03

    if std < 2.0:
        threshold = max(threshold - 0.02, 0.04)

    return round(max(0.04, min(0.30, threshold)), 4)


def extract_target_date(event):
    title = event.get("title", "")
    end = event.get("end_date", "")
    if end:
        return end[:10]
    ticker = event.get("event_ticker", "")
    m = re.search(r'(\d{2})([A-Z]{3})(\d{2})$', ticker)
    if m:
        yr, mon, day = m.groups()
        months = {"JAN":"01","FEB":"02","MAR":"03","APR":"04","MAY":"05","JUN":"06",
                  "JUL":"07","AUG":"08","SEP":"09","OCT":"10","NOV":"11","DEC":"12"}
        if mon in months:
            return f"20{yr}-{months[mon]}-{day}"
    return None


def extract_strike(market):
    floor = market.get("floor_strike")
    cap = market.get("cap_strike")
    if floor is not None or cap is not None:
        return (float(floor) if floor is not None else None,
                float(cap) if cap is not None else None)
    subtitle = market.get("subtitle", "")
    nums = re.findall(r'(\d+)', subtitle)
    if len(nums) >= 2:
        return int(nums[0]), int(nums[1])
    elif len(nums) == 1:
        return int(nums[0]), None
    return None, None


def compute_days_out(target_date):
    try:
        target = datetime.strptime(target_date, "%Y-%m-%d").date()
        today = datetime.now(timezone.utc).date()
        return (target - today).days
    except Exception:
        return 1


# ── Main Analysis ─────────────────────────────────────────────────

def analyze():
    _rate_limited_sources.clear()
    print("╔══════════════════════════════════════════════╗")
    print("║  KingClaw Weather Analyzer v3.1 (Per-City)   ║")
    print("╚══════════════════════════════════════════════╝")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")

    # Load configurations
    source_weights, city_source_weights = load_source_weights()
    city_calibrations = load_city_calibration()
    strategies = load_city_strategies()
    print(f"  Source weights loaded: {len(source_weights)} sources, {len(city_source_weights)} city overrides")
    print(f"  City calibrations: {city_calibrations}")
    if strategies:
        print(f"  City strategies loaded: v{strategies.get('version', '?')} — {len(strategies.get('cities', {}))} cities")
    else:
        print(f"  City strategies: not found, using defaults")
    print()

    try:
        with open(MARKETS_FILE) as f:
            markets_data = json.load(f)
    except FileNotFoundError:
        print("No active-markets.json found. Run scanner first.")
        return None

    analysis = {
        "analysis_time": datetime.now(timezone.utc).isoformat(),
        "analyzer_version": "3.1-per-city",
        "source_weights": source_weights,
        "cities": {}
    }

    for series, sdata in markets_data.get("series", {}).items():
        cfg = CITIES.get(series)
        if not cfg:
            print(f"  ⚠ Unknown series {series}, skipping")
            continue
        city = cfg["city"]
        lat, lon, tz = cfg["lat"], cfg["lon"], cfg["tz"]
        city_multiplier = city_calibrations.get(city, 1.0)
        strategy = get_city_strategy(strategies, city)

        print(f"\n  📋 {city} strategy: {strategy.get('style', 'balanced')} "
              f"(base_threshold={strategy.get('edge_threshold_base', 0.12):.0%}, "
              f"kelly_mult={strategy.get('kelly_multiplier', 0.25)}, "
              f"min_sources={strategy.get('min_sources_required', 7)})")

        city_analysis = {
            "city": city,
            "series": series,
            "city_std_multiplier": city_multiplier,
            "strategy": {
                "style": strategy.get("style", "balanced"),
                "edge_threshold_base": strategy.get("edge_threshold_base", 0.12),
                "kelly_multiplier": strategy.get("kelly_multiplier", 0.25),
                "capital_allocation_pct": strategy.get("capital_allocation_pct", 8.0),
                "max_trades_per_day": strategy.get("max_trades_per_day", 3),
                "min_sources_required": strategy.get("min_sources_required", 7),
                "max_source_spread_f": strategy.get("max_source_spread_f", 6.0),
                "skip_if_bimodal": strategy.get("skip_if_bimodal", False),
            },
            "events": []
        }

        for event in sdata.get("events", []):
            target_date = extract_target_date(event)
            print(f"\n{'─'*50}")
            print(f"  {city} — {event.get('title', series)} (date: {target_date})")
            print(f"{'─'*50}")

            if not target_date:
                print("    ⚠ Could not determine target date, skipping")
                continue

            days_out = compute_days_out(target_date)

            forecasts = collect_forecasts(lat, lon, tz, target_date)
            sources_used = list(forecasts.keys())
            sources_failed = [n for n, _ in ALL_SOURCES if n not in forecasts]

            print(f"\n    Sources: {len(sources_used)}/{len(ALL_SOURCES)} succeeded")
            if sources_failed:
                print(f"    Failed: {', '.join(sources_failed)}")

            # Use per-city weights if available, fall back to global
            effective_weights = city_source_weights.get(city, source_weights)
            stats = weighted_ensemble_stats(forecasts, effective_weights, city_multiplier, city=city)
            if stats:
                print(f"    Ensemble: mean={stats['ensemble_mean']}  median={stats['ensemble_median']}  "
                      f"std={stats['ensemble_std']}  calibrated_std={stats['calibrated_std']}")
                print(f"    CI: [{stats['confidence_interval_low']}, {stats['confidence_interval_high']}]")
                print(f"    Agreement: {stats['sources_agreeing']}/{len(sources_used)} within 3°F  "
                      f"Bimodal: {stats['bimodal']}  Skew: {stats['skewness']}  "
                      f"Spread: {stats.get('source_spread_f', 0)}°F")

            mean = stats.get("ensemble_mean")
            calibrated_std = stats.get("calibrated_std", 3.0)
            raw_std = stats.get("ensemble_std", 3.0)
            skewness = stats.get("skewness", 0.0)
            bimodal = stats.get("bimodal", False)
            cluster_info = stats.get("cluster_info")
            sources_agreeing = stats.get("sources_agreeing", 0)
            source_spread = stats.get("source_spread_f", 0)

            # Per-city confidence checks
            min_sources = strategy.get("min_sources_required", 7)
            max_spread = strategy.get("max_source_spread_f", 6.0)
            skip_bimodal = strategy.get("skip_if_bimodal", False)

            low_confidence = False
            low_confidence_reasons = []

            if len(sources_used) < min_sources:
                low_confidence = True
                low_confidence_reasons.append(f"only {len(sources_used)} sources (need {min_sources})")

            if source_spread > max_spread:
                low_confidence = True
                low_confidence_reasons.append(f"spread {source_spread:.1f}°F > max {max_spread}°F")

            if skip_bimodal and bimodal:
                low_confidence = True
                low_confidence_reasons.append("bimodal distribution detected")

            if low_confidence:
                print(f"    ⚠ LOW CONFIDENCE: {'; '.join(low_confidence_reasons)}")
                print(f"    → Skipping opportunity detection for this event")

            # Dynamic edge threshold using strategy base
            edge_threshold = compute_edge_threshold(city, calibrated_std, sources_agreeing, days_out, strategy)
            print(f"    Edge threshold: {edge_threshold:.1%} (base={strategy.get('edge_threshold_base', 0.12):.0%}, days_out={days_out})")

            event_analysis = {
                "event_ticker": event["event_ticker"],
                "title": event.get("title", ""),
                "target_date": target_date,
                "days_out": days_out,
                "sources_used": sources_used,
                "sources_failed": sources_failed,
                "source_forecasts": {k: round(v, 1) for k, v in forecasts.items()},
                "edge_threshold": edge_threshold,
                "low_confidence": low_confidence,
                "low_confidence_reasons": low_confidence_reasons,
                **stats,
                "markets": [],
                "opportunities": []
            }

            print(f"\n    {'Market':<30} {'Strike':>10} {'Mkt':>6} {'Ours':>6} {'Gap':>6} {'Signal':>8}")
            print(f"    {'─'*72}")

            for mkt in event.get("markets", []):
                floor, cap = extract_strike(mkt)
                last = mkt.get("last_price", 0)
                market_price = last / 100.0 if last > 1 else last

                our_prob = None
                gap = None
                tail = False
                if mean is not None and (floor is not None or cap is not None):
                    tail = is_tail_bracket(floor, cap, mean, calibrated_std)
                    our_prob = compute_probability(
                        mean, calibrated_std, floor, cap,
                        skewness=skewness, bimodal=bimodal,
                        cluster_info=cluster_info, is_tail=tail
                    )
                    gap = abs(our_prob - market_price)

                strike_str = f"{int(floor) if floor else '?'}-{int(cap) if cap else '?'}"
                signal = ""
                # Only flag opportunities if NOT low_confidence
                is_opp = (not low_confidence) and gap is not None and gap > edge_threshold
                if is_opp:
                    direction = "BUY YES" if our_prob > market_price else "BUY NO"
                    signal = f"🔥 {direction}"

                print(f"    {mkt.get('subtitle',''):<30} {strike_str:>10} {market_price:>5.0%} "
                      f"{our_prob:>5.0%} {gap:>5.0%} {signal}" if our_prob is not None else
                      f"    {mkt.get('subtitle',''):<30} {strike_str:>10} {market_price:>5.0%}   N/A")

                mkt_analysis = {
                    "ticker": mkt["ticker"],
                    "subtitle": mkt.get("subtitle", ""),
                    "floor_strike": floor,
                    "cap_strike": cap,
                    "market_yes_price": round(market_price, 3),
                    "our_probability": round(our_prob, 4) if our_prob is not None else None,
                    "gap": round(gap, 4) if gap is not None else None,
                    "is_opportunity": is_opp,
                    "is_tail": tail,
                    "edge_threshold": edge_threshold,
                    "yes_bid": mkt.get("yes_bid", 0),
                    "yes_ask": mkt.get("yes_ask", 0),
                    "volume": mkt.get("volume", 0),
                    "open_interest": mkt.get("open_interest", 0),
                }
                event_analysis["markets"].append(mkt_analysis)

                if is_opp:
                    direction = "YES" if our_prob > market_price else "NO"
                    event_analysis["opportunities"].append({
                        "ticker": mkt["ticker"],
                        "subtitle": mkt.get("subtitle", ""),
                        "direction": direction,
                        "our_prob": round(our_prob, 4),
                        "market_price": round(market_price, 3),
                        "edge": round(gap, 4),
                        "is_tail": tail,
                        "edge_threshold": edge_threshold,
                        "volume": mkt.get("volume", 0),
                        "open_interest": mkt.get("open_interest", 0),
                        "yes_bid": mkt.get("yes_bid", 0),
                        "yes_ask": mkt.get("yes_ask", 0),
                    })

            city_analysis["events"].append(event_analysis)
        analysis["cities"][series] = city_analysis

    # Summary
    total_opps = sum(
        len(e["opportunities"])
        for c in analysis["cities"].values()
        for e in c["events"]
    )
    analysis["total_opportunities"] = total_opps

    print(f"\n{'═'*50}")
    print(f"  TOTAL OPPORTUNITIES: {total_opps}")
    if total_opps > 0:
        print(f"\n  Top opportunities:")
        for c in analysis["cities"].values():
            for e in c["events"]:
                for opp in e["opportunities"]:
                    tail_flag = " (TAIL)" if opp.get("is_tail") else ""
                    print(f"    {opp['direction']:>3} {opp['ticker']:<35} edge={opp['edge']:.1%}  "
                          f"(ours={opp['our_prob']:.1%} vs mkt={opp['market_price']:.1%}){tail_flag}")
    print(f"{'═'*50}")

    with open(OUTPUT, "w") as f:
        json.dump(analysis, f, indent=2)
    print(f"\nSaved to {OUTPUT}")
    return analysis


def run():
    return analyze()


if __name__ == "__main__":
    run()
