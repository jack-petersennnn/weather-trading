#!/usr/bin/env python3
"""
NWS Gridpoint Forecast Tracker

Pulls NWS gridpoint maxTemperature forecasts for all Kalshi settlement stations
every run, logs them with timestamps. Compares against ACIS actuals when available.

Run via cron every 4 hours. Data accumulates in nws_gridpoint_log.json.

Usage:
    python3 nws_gridpoint_tracker.py          # Pull and log forecasts
    python3 nws_gridpoint_tracker.py --report  # Print accuracy report
"""
import json, os, sys, urllib.request
from datetime import datetime, timezone, timedelta

LOG_FILE = os.path.join(os.path.dirname(__file__), "nws_gridpoint_log.json")
ACIS_ACTUALS_FILE = os.path.join(os.path.dirname(__file__), "acis_actuals_daily.json")

# Exact stations per Kalshi settlement rules
# Grid coords looked up via api.weather.gov/points/{lat},{lon}
STATIONS = {
    # Specific-station markets (station named in rules)
    "New York":      {"grid": "OKX/34,38",  "acis": "KNYC", "desc": "Central Park",    "market": "NHIGH"},
    "Chicago":       {"grid": "LOT/72,69",  "acis": "KMDW", "desc": "Midway",          "market": "CHIHIGH"},
    "Miami":         {"grid": "MFL/106,51", "acis": "KMIA", "desc": "Miami Intl",       "market": "MIAHIGH"},
    "Denver":        {"grid": "BOU/74,66",  "acis": "KDEN", "desc": "Denver Intl",      "market": "DENHIGH"},
    "Los Angeles":   {"grid": "LOX/149,41", "acis": "KLAX", "desc": "LAX",              "market": "LAHIGH"},
    "Austin":        {"grid": "EWX/158,87", "acis": "KAUS", "desc": "Austin Bergstrom", "market": "AUSHIGH"},
    "Philadelphia":  {"grid": "PHI/48,72",   "acis": "KPHL", "desc": "Philly Intl",      "market": "PHILHIGH"},
    # GLOBALTEMPERATURE markets (primary NWS station)
    "Phoenix":       {"grid": "PSR/161,57",  "acis": "KPHX", "desc": "Phoenix Airport",        "market": "GLOBAL"},
    "Las Vegas":     {"grid": "VEF/122,94",  "acis": "KLAS", "desc": "Harry Reid Intl",         "market": "GLOBAL"},
    "Atlanta":       {"grid": "FFC/50,82",   "acis": "KATL", "desc": "Hartsfield-Jackson",      "market": "GLOBAL"},
    "Boston":        {"grid": "BOX/73,90",   "acis": "KBOS", "desc": "Logan Intl",              "market": "GLOBAL"},
    "Seattle":       {"grid": "SEW/124,61",  "acis": "KSEA", "desc": "Sea-Tac",                 "market": "GLOBAL"},
    "San Francisco": {"grid": "MTR/85,98",   "acis": "KSFO", "desc": "SFO Intl",                "market": "GLOBAL"},
    "Houston":       {"grid": "HGX/66,89",   "acis": "KHOU", "desc": "Hobby Airport",           "market": "GLOBAL"},
    "San Antonio":   {"grid": "EWX/127,59",  "acis": "KSAT", "desc": "SA Intl",                 "market": "GLOBAL"},
    "New Orleans":   {"grid": "LIX/60,90",   "acis": "KMSY", "desc": "NO Intl",                 "market": "GLOBAL"},
    "Oklahoma City": {"grid": "OUN/94,90",   "acis": "KOKC", "desc": "Will Rogers World",       "market": "GLOBAL"},
    "Dallas":        {"grid": "FWD/87,107",  "acis": "KDFW", "desc": "DFW Airport (Kalshi CLIDFW)", "market": "GLOBAL"},
    "Minneapolis":   {"grid": "MPX/110,68",  "acis": "KMSP", "desc": "MSP Intl",                "market": "GLOBAL"},
    "Washington DC": {"grid": "LWX/97,69",   "acis": "KDCA", "desc": "Reagan National",         "market": "GLOBAL"},
}


def fetch_json(url, timeout=15):
    req = urllib.request.Request(url, headers={
        "User-Agent": "KingClaw-Weather/3.1",
        "Accept": "application/json"
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def load_log():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE) as f:
            return json.load(f)
    return {"forecasts": [], "actuals": {}}


def save_log(log):
    with open(LOG_FILE, "w") as f:
        json.dump(log, f, indent=2)


def pull_forecasts():
    """Pull current NWS gridpoint maxTemperature for all stations."""
    now = datetime.now(timezone.utc)
    timestamp = now.isoformat()
    
    print(f"\n🌡️  NWS Gridpoint Tracker — {timestamp}\n")
    
    entry = {
        "timestamp": timestamp,
        "cities": {}
    }
    
    for city, info in STATIONS.items():
        try:
            data = fetch_json(f"https://api.weather.gov/gridpoints/{info['grid']}")
            max_temps = data["properties"].get("maxTemperature", {}).get("values", [])
            
            forecasts = {}
            for v in max_temps[:7]:  # Next 7 days
                temp_c = v["value"]
                temp_f = round(temp_c * 9/5 + 32)
                date = v["validTime"][:10]
                forecasts[date] = temp_f
            
            entry["cities"][city] = forecasts
            dates_str = ", ".join(f"{d}: {t}°F" for d, t in list(forecasts.items())[:3])
            print(f"  ✅ {city}: {dates_str}...")
            
        except Exception as e:
            print(f"  ❌ {city}: {e}")
            entry["cities"][city] = {"error": str(e)}
    
    return entry


def pull_actuals():
    """Pull latest ACIS actuals for comparison."""
    print("\n📊 Pulling ACIS actuals...")
    
    now = datetime.now(timezone.utc)
    end_date = now.strftime("%Y-%m-%d")
    start_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    
    actuals = {}
    for city, info in STATIONS.items():
        try:
            body = json.dumps({
                "sid": info["acis"],
                "sdate": start_date,
                "edate": end_date,
                "elems": [{"name": "maxt"}]
            }).encode()
            req = urllib.request.Request(
                "https://data.rcc-acis.org/StnData",
                data=body,
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read().decode())
            
            for row in data["data"]:
                date_str = row[0]
                val = row[1]
                if val not in ("M", "T", "S", ""):
                    key = f"{city}_{date_str}"
                    actuals[key] = int(val)
                    
        except Exception as e:
            print(f"  ❌ {city} ACIS: {e}")
    
    print(f"  Got {len(actuals)} actual values")
    return actuals


def accuracy_report(log):
    """Compare logged forecasts against actuals."""
    actuals = log.get("actuals", {})
    
    if not actuals:
        print("No actuals available yet. Keep running the tracker!")
        return
    
    print("\n" + "=" * 70)
    print("  NWS GRIDPOINT FORECAST ACCURACY REPORT")
    print("=" * 70)
    
    # For each forecast entry, check how close it was
    # Group by: city, days_ahead (how many days before the actual date was the forecast made)
    from collections import defaultdict
    errors_by_city = defaultdict(list)
    errors_by_city_days = defaultdict(lambda: defaultdict(list))
    
    for entry in log["forecasts"]:
        ts = datetime.fromisoformat(entry["timestamp"])
        forecast_date = ts.date()
        
        for city, forecasts in entry["cities"].items():
            if isinstance(forecasts, dict) and "error" not in forecasts:
                for date_str, predicted in forecasts.items():
                    key = f"{city}_{date_str}"
                    actual = actuals.get(key)
                    if actual is not None:
                        error = predicted - actual
                        abs_error = abs(error)
                        
                        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                        days_ahead = (target_date - forecast_date).days
                        
                        errors_by_city[city].append(abs_error)
                        errors_by_city_days[city][days_ahead].append(abs_error)
    
    if not errors_by_city:
        print("  No forecast/actual pairs available yet.")
        return
    
    print(f"\n{'City':<15} {'N':>5} {'MAE':>6} {'Within 1°F':>12} {'Within 2°F':>12}")
    print("-" * 55)
    
    for city in STATIONS:
        errors = errors_by_city.get(city, [])
        if errors:
            mae = sum(errors) / len(errors)
            within_1 = sum(1 for e in errors if e <= 1) / len(errors) * 100
            within_2 = sum(1 for e in errors if e <= 2) / len(errors) * 100
            print(f"{city:<15} {len(errors):>5} {mae:>5.1f}°F {within_1:>10.0f}% {within_2:>10.0f}%")
    
    # Breakdown by days ahead
    print(f"\n{'City':<15} {'Days Out':>9} {'N':>5} {'MAE':>6}")
    print("-" * 40)
    for city in STATIONS:
        for days in sorted(errors_by_city_days.get(city, {}).keys()):
            errors = errors_by_city_days[city][days]
            if errors:
                mae = sum(errors) / len(errors)
                print(f"{city:<15} {days:>7}d {len(errors):>5} {mae:>5.1f}°F")


def main():
    log = load_log()
    
    if "--report" in sys.argv:
        accuracy_report(log)
        return
    
    # Pull forecasts
    entry = pull_forecasts()
    log["forecasts"].append(entry)
    
    # Pull actuals
    new_actuals = pull_actuals()
    log["actuals"].update(new_actuals)
    
    # Quick comparison for any dates we can check
    print("\n📋 Quick check (today's forecasts vs available actuals):")
    for city, forecasts in entry["cities"].items():
        if isinstance(forecasts, dict) and "error" not in forecasts:
            for date_str, predicted in forecasts.items():
                key = f"{city}_{date_str}"
                actual = log["actuals"].get(key)
                if actual is not None:
                    diff = predicted - actual
                    match = "✅" if abs(diff) <= 1 else "⚠️" if abs(diff) <= 2 else "❌"
                    print(f"  {city} {date_str}: NWS={predicted}°F  Actual={actual}°F  diff={diff:+d}  {match}")
    
    save_log(log)
    print(f"\n💾 Logged to {LOG_FILE}")
    print(f"   Total forecast entries: {len(log['forecasts'])}")
    print(f"   Total actuals: {len(log['actuals'])}")


if __name__ == "__main__":
    main()
