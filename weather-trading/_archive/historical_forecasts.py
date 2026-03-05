#!/usr/bin/env python3
"""
Historical Forecast Collector
Collects historical model outputs from Open-Meteo archive API for comparison against NWS CLI actuals.
Also extends actuals using Open-Meteo archive for dates where NWS CLI is not available.
"""
import json, os, time
from datetime import datetime, timedelta
from urllib.request import urlopen, Request

ACTUALS_FILE = os.path.join(os.path.dirname(__file__), "nws_cli_actuals.json")
FORECASTS_FILE = os.path.join(os.path.dirname(__file__), "historical_forecasts.json")

CITIES_COORDS = {
    "New York":    {"lat": 40.7831, "lon": -73.9712, "tz": "America/New_York"},
    "Chicago":     {"lat": 41.97,   "lon": -87.91,   "tz": "America/Chicago"},
    "Miami":       {"lat": 25.79,   "lon": -80.29,   "tz": "America/New_York"},
    "Denver":      {"lat": 39.86,   "lon": -104.67,  "tz": "America/Denver"},
    "Los Angeles": {"lat": 33.94,   "lon": -118.41,  "tz": "America/Los_Angeles"},
    "Austin":      {"lat": 30.19,   "lon": -97.67,   "tz": "America/Chicago"},
}

DELAY = 0.4

def fetch_json(url):
    req = Request(url, headers={"User-Agent": "KingClaw-Weather/3.1"})
    try:
        with urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"    ⚠ {e}")
        return None

def fetch_archive(lat, lon, tz, start_date, end_date, model=None):
    """Fetch from Open-Meteo archive. Returns {date: temp_f}."""
    tz_enc = tz.replace("/", "%2F")
    url = (f"https://archive-api.open-meteo.com/v1/archive?"
           f"latitude={lat}&longitude={lon}"
           f"&start_date={start_date}&end_date={end_date}"
           f"&daily=temperature_2m_max&temperature_unit=fahrenheit"
           f"&timezone={tz_enc}")
    if model:
        url += f"&models={model}"
    
    data = fetch_json(url)
    time.sleep(DELAY)
    
    if not data or "daily" not in data:
        return {}
    
    dates = data["daily"].get("time", [])
    temps = data["daily"].get("temperature_2m_max", [])
    return {d: round(t, 1) for d, t in zip(dates, temps) if t is not None}

def collect_all():
    print("📊 Historical Forecast Collector (Extended)")
    print("=" * 60)
    
    with open(ACTUALS_FILE) as f:
        actuals = json.load(f)
    
    # Load or init forecasts
    existing = {}
    if os.path.exists(FORECASTS_FILE):
        with open(FORECASTS_FILE) as f:
            existing = json.load(f)
    if "cities" not in existing:
        existing = {"cities": {}, "metadata": {}}
    
    # Define date range: 12 months back from the latest NWS CLI date, up to 5 days ago
    # (archive data has ~5 day lag)
    end_date = (datetime.utcnow() - timedelta(days=6)).strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=365)).strftime("%Y-%m-%d")
    
    print(f"  Archive range: {start_date} to {end_date}")
    
    # Sources: model name for Open-Meteo archive API
    sources = [
        ("Best Match",      None),           # default archive = best match
        ("ECMWF",           "ecmwf_ifs025"),
        ("GFS",             "gfs_seamless"),
        ("Ensemble ICON",   "icon_seamless"),
        ("Ensemble GFS",    "gfs_seamless"),  # same model, separate tracking
        ("Ensemble ECMWF",  "ecmwf_ifs025"), # same model, separate tracking
    ]
    
    # Also fetch the "actual" best-match archive to extend NWS CLI actuals
    # We'll store this separately and note it's Open-Meteo-based
    
    for city, coords in CITIES_COORDS.items():
        print(f"\n  🌡️ {city}")
        
        if city not in existing["cities"]:
            existing["cities"][city] = {}
        
        # First: fetch extended actuals from Open-Meteo archive (best match = closest to reality)
        # This extends our NWS CLI data for longer backtest
        print(f"    [Extended Actuals]...", end=" ", flush=True)
        extended = fetch_archive(coords["lat"], coords["lon"], coords["tz"],
                                  start_date, end_date)
        if extended:
            # Store in actuals file as supplementary data
            if city in actuals.get("cities", {}):
                nws_dates = set(actuals["cities"][city].get("data", {}).keys())
                # Add Open-Meteo dates that NWS doesn't have
                if "extended_data" not in actuals["cities"][city]:
                    actuals["cities"][city]["extended_data"] = {}
                for d, t in extended.items():
                    if d not in nws_dates:
                        actuals["cities"][city]["extended_data"][d] = t
                print(f"✅ {len(extended)} dates ({len(nws_dates)} NWS + {len(extended) - len(nws_dates & set(extended.keys()))} extended)")
            else:
                actuals.setdefault("cities", {})[city] = {
                    "cli_code": city[:3].upper(),
                    "data": {},
                    "extended_data": extended
                }
                print(f"✅ {len(extended)} dates (all extended)")
        else:
            print("⚠ Failed")
        
        # Now fetch each model's archive
        for source_name, model in sources:
            # Skip if GFS/ECMWF ensemble is same as regular (they are the same archive model)
            # But we keep them as separate entries for the weight system
            if source_name.startswith("Ensemble") and source_name.replace("Ensemble ", "") in [s[0] for s in sources]:
                # Use existing data from the base model
                base_name = source_name.split()[-1]
                base_model = model
                # Actually fetch separately — the ensemble models might differ slightly
                pass
            
            print(f"    [{source_name}]...", end=" ", flush=True)
            data = fetch_archive(coords["lat"], coords["lon"], coords["tz"],
                                  start_date, end_date, model)
            if data:
                existing["cities"][city][source_name] = data
                print(f"✅ {len(data)} dates")
            else:
                print("⚠ No data")
    
    # Save extended actuals
    actuals["metadata"]["last_updated"] = datetime.utcnow().isoformat() + "Z"
    actuals["metadata"]["notes"] = ("NWS CLI actuals in 'data', Open-Meteo archive in 'extended_data'. "
                                     "NWS CLI is authoritative for settlement; extended data for backtesting only.")
    with open(ACTUALS_FILE, "w") as f:
        json.dump(actuals, f, indent=2)
    print(f"\n💾 Updated {ACTUALS_FILE} with extended actuals")
    
    # Save forecasts
    existing["metadata"] = {
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "date_range": f"{start_date} to {end_date}",
        "sources_available": sorted(set(s[0] for s in sources)),
        "sources_unavailable": ["NWS Forecast", "NWS Hourly", "Tomorrow.io", "Visual Crossing"],
        "notes": ("All archive data from Open-Meteo. NWS forecasts not available retroactively. "
                  "Ensemble sources use same model archive as their base models.")
    }
    
    with open(FORECASTS_FILE, "w") as f:
        json.dump(existing, f, indent=2)
    print(f"💾 Saved {FORECASTS_FILE}")
    
    # Summary
    print("\n📊 Summary:")
    for city in CITIES_COORDS:
        city_sources = existing.get("cities", {}).get(city, {})
        counts = {s: len(d) for s, d in city_sources.items()}
        nws_count = len(actuals.get("cities", {}).get(city, {}).get("data", {}))
        ext_count = len(actuals.get("cities", {}).get(city, {}).get("extended_data", {}))
        print(f"  {city}: NWS CLI={nws_count}, Extended={ext_count}, Sources={counts}")

if __name__ == "__main__":
    collect_all()
