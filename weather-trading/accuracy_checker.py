#!/usr/bin/env python3
"""
Accuracy Checker — Compares saved forecasts against actual temps.

Runs daily. For each past date in forecast_history.json:
1. Fetches the actual high temp from Open-Meteo archive
2. Compares every source's forecast against actual
3. Stores per-source error data
4. Feeds into weight calibration

This tracks ALL sources (NWS, ECMWF, GFS, ICON, ensembles, etc.)
not just the 4 that the old backtester had data for.
"""

import json
import os
import math
import fcntl
import urllib.request
from datetime import datetime, timezone, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FORECAST_HISTORY = os.path.join(BASE_DIR, "forecast_history.json")
ACCURACY_DATA = os.path.join(BASE_DIR, "source_accuracy_live.json")
WEIGHTS_FILE = os.path.join(BASE_DIR, "source_weights.json")

CITIES_COORDS = {
    "New York":    {"lat": 40.7128, "lon": -73.9352, "tz": "America/New_York"},
    "Chicago":     {"lat": 41.8781, "lon": -87.6298, "tz": "America/Chicago"},
    "Miami":       {"lat": 25.7617, "lon": -80.1918, "tz": "America/New_York"},
    "Denver":      {"lat": 39.7392, "lon": -104.9903, "tz": "America/Denver"},
    "Los Angeles": {"lat": 34.0522, "lon": -118.2437, "tz": "America/Los_Angeles"},
    "Austin":      {"lat": 30.2672, "lon": -97.7431, "tz": "America/Chicago"},
}


def get_actual_temp(lat, lon, tz, date_str):
    tz_encoded = tz.replace("/", "%2F")
    url = (f"https://archive-api.open-meteo.com/v1/archive?"
           f"latitude={lat}&longitude={lon}"
           f"&start_date={date_str}&end_date={date_str}"
           f"&daily=temperature_2m_max&temperature_unit=fahrenheit"
           f"&timezone={tz_encoded}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "KingClaw-Acc/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        temps = data.get("daily", {}).get("temperature_2m_max", [])
        return temps[0] if temps and temps[0] is not None else None
    except:
        return None


def run():
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    
    print(f"📊 Accuracy Checker — {now.strftime('%H:%M UTC')}")
    
    if not os.path.exists(FORECAST_HISTORY):
        print("  No forecast history yet. Run forecast_logger first.")
        return
    
    with open(FORECAST_HISTORY) as f:
        history = json.load(f)
    
    # Load existing accuracy data
    accuracy = {}
    if os.path.exists(ACCURACY_DATA):
        try:
            with open(ACCURACY_DATA) as f:
                accuracy = json.load(f)
        except:
            accuracy = {}
    
    checked = accuracy.get("checked_dates", {})
    comparisons = accuracy.get("comparisons", [])
    source_errors = accuracy.get("source_errors", {})  # {city: {source: [errors]}}
    
    new_checks = 0
    
    for key, entry in history.items():
        city = entry.get("city")
        target_date = entry.get("target_date")
        
        if not city or not target_date:
            continue
        
        # Only check past dates (at least 1 day old for archive availability)
        if target_date >= today:
            continue
        
        # Skip if already checked
        check_key = f"{city}|{target_date}"
        if check_key in checked:
            continue
        
        coords = CITIES_COORDS.get(city)
        if not coords:
            continue
        
        # Get actual temp
        actual = get_actual_temp(coords["lat"], coords["lon"], coords["tz"], target_date)
        if actual is None:
            print(f"  {city} {target_date}: no actual data yet")
            continue
        
        # Get the final forecast snapshot (closest to the actual day)
        final_forecasts = entry.get("final_source_forecasts", {})
        ensemble_mean = entry.get("final_ensemble_mean")
        
        if not final_forecasts:
            continue
        
        print(f"  {city} {target_date}: actual={actual}°F, ensemble={ensemble_mean}°F (err={abs(actual-ensemble_mean):.1f}°F)")
        
        # Calculate error for EACH source
        if city not in source_errors:
            source_errors[city] = {}
        
        source_results = {}
        for source_name, predicted in final_forecasts.items():
            error = predicted - actual  # signed error (positive = over-predicted)
            abs_error = abs(error)
            
            if source_name not in source_errors[city]:
                source_errors[city][source_name] = []
            source_errors[city][source_name].append({
                "date": target_date,
                "predicted": predicted,
                "actual": actual,
                "error": round(error, 2),
                "abs_error": round(abs_error, 2),
            })
            
            source_results[source_name] = {
                "predicted": predicted,
                "error": round(error, 2),
                "abs_error": round(abs_error, 2),
            }
            
            print(f"    {source_name:20s} predicted={predicted:6.1f}°F  error={error:+.1f}°F")
        
        comparisons.append({
            "city": city,
            "target_date": target_date,
            "actual": actual,
            "ensemble_mean": ensemble_mean,
            "ensemble_error": round(abs(ensemble_mean - actual), 2),
            "sources": source_results,
            "checked_at": now.isoformat(),
        })
        
        checked[check_key] = now.isoformat()
        new_checks += 1
    
    # Calculate summary stats per source per city
    summary = {}
    for city, sources in source_errors.items():
        summary[city] = {}
        for source, errors in sources.items():
            abs_errors = [e["abs_error"] for e in errors]
            summary[city][source] = {
                "mae": round(sum(abs_errors) / len(abs_errors), 2),
                "n": len(abs_errors),
                "max_error": round(max(abs_errors), 2),
                "bias": round(sum(e["error"] for e in errors) / len(errors), 2),
            }
    
    # Save
    accuracy["checked_dates"] = checked
    accuracy["comparisons"] = comparisons
    accuracy["source_errors"] = source_errors
    accuracy["summary"] = summary
    accuracy["last_run"] = now.isoformat()
    
    with open(ACCURACY_DATA, "w") as f:
        json.dump(accuracy, f, indent=2)
    
    print(f"\n  ✓ {new_checks} new checks, {len(comparisons)} total comparisons")
    
    # If we have enough data, update weights
    if new_checks > 0 and len(comparisons) >= 5:
        update_weights(summary)


def update_weights(summary):
    """Update source weights based on live accuracy data."""
    print("\n  🔄 Updating source weights from live data...")
    
    # Load existing weights
    if os.path.exists(WEIGHTS_FILE):
        with open(WEIGHTS_FILE) as f:
            weights_data = json.load(f)
    else:
        weights_data = {"weights": {}, "city_weights": {}}
    
    # For each city, calculate relative weight based on inverse MAE
    for city, sources in summary.items():
        if len(sources) < 3:
            continue
        
        # Get MAE for each source
        maes = {src: stats["mae"] for src, stats in sources.items() if stats["n"] >= 3}
        if not maes:
            continue
        
        # Inverse MAE weighting: lower MAE = higher weight
        # Normalize so average weight = 1.0
        inv_maes = {src: 1.0 / max(mae, 0.1) for src, mae in maes.items()}
        avg_inv = sum(inv_maes.values()) / len(inv_maes)
        
        city_weights = {src: round(inv / avg_inv, 3) for src, inv in inv_maes.items()}
        
        if city not in weights_data.get("city_weights", {}):
            weights_data["city_weights"][city] = {}
        
        # Blend: 70% backtest weights + 30% live weights (live grows as we get more data)
        for src, live_weight in city_weights.items():
            existing = weights_data["city_weights"][city].get(src, 1.0)
            n = summary[city][src]["n"]
            # Live weight influence grows with more data points
            live_influence = min(0.5, n * 0.05)  # 5% per data point, max 50%
            blended = existing * (1 - live_influence) + live_weight * live_influence
            weights_data["city_weights"][city][src] = round(blended, 3)
        
        print(f"    {city}: updated {len(city_weights)} source weights (live influence: {live_influence:.0%})")
    
    # Update global weights (average across cities)
    all_sources = set()
    for city_w in weights_data.get("city_weights", {}).values():
        all_sources.update(city_w.keys())
    
    for src in all_sources:
        vals = [weights_data["city_weights"][c].get(src, 1.0) 
                for c in weights_data.get("city_weights", {})]
        weights_data["weights"][src] = round(sum(vals) / len(vals), 3)
    
    weights_data["last_calibrated"] = datetime.now(timezone.utc).isoformat()
    weights_data["method"] = "blended_backtest_live"
    
    with open(WEIGHTS_FILE, "w") as f:
        json.dump(weights_data, f, indent=2)
    
    print("    ✓ Weights saved")


if __name__ == "__main__":
    run()
