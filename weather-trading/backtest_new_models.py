#!/usr/bin/env python3
"""
Backtest New Weather Models against ACIS Actuals (Kalshi Settlement Source).

Uses Open-Meteo Previous Runs API to get what each model PREDICTED for each day,
then compares against ACIS (RCC-ACIS) actual high temps — the same source Kalshi
uses for settlement.

Outputs: MAE, bias, max error per model per city, plus recommended initial weights.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ACIS stations for all 19 trading cities (matches Kalshi settlement stations)
ACIS_STATIONS = {
    "New York":       "KNYC",
    "Chicago":        "KMDW",
    "Miami":          "KMIA",
    "Denver":         "KDEN",
    "Austin":         "KAUS",
    "Minneapolis":    "KMSP",
    "Washington DC":  "KDCA",
    "Atlanta":        "KATL",
    "Philadelphia":   "KPHL",
    "Houston":        "KHOU",   # Hobby Airport (Kalshi CLIHOU)
    "Dallas":         "KDFW",
    "Seattle":        "KSEA",
    "Boston":         "KBOS",
    "Phoenix":        "KPHX",
    "Oklahoma City":  "KOKC",
    "Las Vegas":      "KLAS",
    "San Francisco":  "KSFO",
    "San Antonio":    "KSAT",
    "New Orleans":    "KMSY",
}

# City coordinates for Open-Meteo (same as fast_scanner.py)
CITY_COORDS = {
    "New York":       (40.7789, -73.9692, "America/New_York"),
    "Chicago":        (41.7868, -87.7522, "America/Chicago"),
    "Miami":          (25.7959, -80.2870, "America/New_York"),
    "Denver":         (39.8561, -104.6737, "America/Denver"),
    "Austin":         (30.1945, -97.6699, "America/Chicago"),
    "Minneapolis":    (44.8831, -93.2289, "America/Chicago"),
    "Washington DC":  (38.8512, -77.0402, "America/New_York"),
    "Atlanta":        (33.6407, -84.4277, "America/New_York"),
    "Philadelphia":   (39.8721, -75.2411, "America/New_York"),
    "Houston":        (29.6454, -95.2789, "America/Chicago"),
    "Dallas":         (32.8998, -97.0403, "America/Chicago"),
    "Seattle":        (47.4502, -122.3088, "America/Los_Angeles"),
    "Boston":         (42.3656, -71.0096, "America/New_York"),
    "Phoenix":        (33.4373, -112.0078, "America/Phoenix"),
    "Oklahoma City":  (35.3931, -97.6007, "America/Chicago"),
    "Las Vegas":      (36.0840, -115.1537, "America/Los_Angeles"),
    "San Francisco":  (37.6213, -122.3790, "America/Los_Angeles"),
    "San Antonio":    (29.5337, -98.4698, "America/Chicago"),
    "New Orleans":    (29.9934, -90.2580, "America/Chicago"),
}

# Models to backtest via Previous Runs API
# HRRR not available on previous-runs, MET Norway untested
BACKTEST_MODELS = {
    "Canadian GEM":          "gem_global",
    "JMA":                   "jma_gsm",
    "UKMO":                  "ukmo_global_deterministic_10km",
    "Meteo-France Arpege":   "arpege_world",
    # Also backtest existing models for comparison
    "GFS":                   "gfs_seamless",
    "ECMWF":                 "ecmwf_ifs025",  # via ensemble API
    "Ensemble ICON":         "icon_seamless",  # via ensemble API
    "Ensemble GFS":          "gfs_seamless",   # via ensemble API
    "Ensemble ECMWF":        "ecmwf_ifs025",   # via ensemble API
}

# Models that use ensemble API instead of forecast API
ENSEMBLE_MODELS = {"ECMWF", "Ensemble ICON", "Ensemble GFS", "Ensemble ECMWF"}

RESULTS_FILE = os.path.join(BASE_DIR, "backtest_new_models_results.json")
ACIS_CACHE_FILE = os.path.join(BASE_DIR, "acis_actuals_all_cities.json")


def post_json(url, body, timeout=15):
    """POST JSON to RCC-ACIS."""
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={
        "Content-Type": "application/json",
        "User-Agent": "KingClaw-Backtest/1.0"
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def fetch_json(url, timeout=15):
    req = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "User-Agent": "KingClaw-Backtest/1.0"
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def pull_acis_actuals(days=365):
    """Pull ACIS actual high temps for all cities."""
    print(f"═══ Pulling ACIS actuals ({days} days) for {len(ACIS_STATIONS)} cities ═══")
    
    # Check cache
    if os.path.exists(ACIS_CACHE_FILE):
        with open(ACIS_CACHE_FILE) as f:
            cached = json.load(f)
        # Use cache if it has all cities and is recent (within 24h)
        cached_cities = set(cached.get("cities", {}).keys())
        if cached_cities >= set(ACIS_STATIONS.keys()):
            pulled = cached.get("pulled_at", "")
            if pulled:
                try:
                    pulled_dt = datetime.fromisoformat(pulled)
                    if (datetime.now(timezone.utc) - pulled_dt).total_seconds() < 86400:
                        print(f"  Using cache from {pulled}")
                        return cached
                except:
                    pass
    
    end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    
    result = {"source": "RCC-ACIS", "pulled_at": datetime.now(timezone.utc).isoformat(), "cities": {}}
    
    for city, station in ACIS_STATIONS.items():
        print(f"  {city} ({station})...", end=" ")
        try:
            body = {
                "sid": station,
                "sdate": start_date,
                "edate": end_date,
                "elems": [{"name": "maxt"}]
            }
            resp = post_json("https://data.rcc-acis.org/StnData", body)
            data = {}
            for row in resp.get("data", []):
                date_str = row[0]
                val = row[1]
                if val not in ("M", "T", "", None):
                    try:
                        data[date_str] = float(val)
                    except:
                        pass
            result["cities"][city] = {"station": station, "data": data}
            print(f"{len(data)} days")
            time.sleep(0.5)
        except Exception as e:
            print(f"FAIL ({e})")
            result["cities"][city] = {"station": station, "data": {}}
    
    # Cache results
    with open(ACIS_CACHE_FILE, "w") as f:
        json.dump(result, f, indent=2)
    
    return result


def pull_model_forecasts(model_name, om_model_id, lat, lon, tz, days=365):
    """Pull historical forecasts from Open-Meteo Previous Runs API."""
    tz_encoded = tz.replace("/", "%2F")
    
    if model_name in ENSEMBLE_MODELS:
        base = "previous-runs-api.open-meteo.com"
        path = "ensemble"
    else:
        base = "previous-runs-api.open-meteo.com"
        path = "forecast"
    
    url = (f"https://{base}/v1/{path}?"
           f"latitude={lat}&longitude={lon}&models={om_model_id}"
           f"&daily=temperature_2m_max&temperature_unit=fahrenheit"
           f"&timezone={tz_encoded}&past_days={days}&forecast_days=0")
    
    data = fetch_json(url)
    if not data or "daily" not in data:
        return {}
    
    dates = data["daily"].get("time", [])
    
    # For multi-model responses, find the right column
    temp_key = f"temperature_2m_max_{om_model_id}"
    temps = data["daily"].get(temp_key, data["daily"].get("temperature_2m_max", []))
    
    result = {}
    for d, t in zip(dates, temps):
        if t is not None:
            result[d] = float(t)
    
    return result


def compute_accuracy(forecasts, actuals):
    """Compute MAE, bias, max error, RMSE from forecast vs actual dicts."""
    errors = []
    for date, pred in forecasts.items():
        if date in actuals:
            actual = actuals[date]
            errors.append(pred - actual)
    
    if not errors:
        return None
    
    abs_errors = [abs(e) for e in errors]
    mae = sum(abs_errors) / len(abs_errors)
    bias = sum(errors) / len(errors)
    max_error = max(abs_errors)
    rmse = (sum(e**2 for e in errors) / len(errors)) ** 0.5
    
    # Count errors within key thresholds
    within_1 = sum(1 for e in abs_errors if e <= 1.0) / len(abs_errors) * 100
    within_2 = sum(1 for e in abs_errors if e <= 2.0) / len(abs_errors) * 100
    within_3 = sum(1 for e in abs_errors if e <= 3.0) / len(abs_errors) * 100
    
    return {
        "mae": round(mae, 2),
        "bias": round(bias, 2),
        "max_error": round(max_error, 1),
        "rmse": round(rmse, 2),
        "n": len(errors),
        "within_1f": round(within_1, 1),
        "within_2f": round(within_2, 1),
        "within_3f": round(within_3, 1),
    }


def compute_initial_weight(mae):
    """Compute initial weight from MAE using inverse-MAE method.
    Lower MAE = higher weight. MAE > 5°F gets weight 0 (too inaccurate)."""
    if mae > 5.0:
        return 0.0
    if mae < 0.5:
        mae = 0.5  # Floor to prevent infinite weight
    # Inverse MAE, scaled so MAE=2.0 gets weight ~1.0
    return round(2.0 / mae, 3)


def run(days=92):
    print(f"\n{'='*60}")
    print(f"  BACKTEST: New Weather Models vs ACIS Actuals")
    print(f"  Window: {days} days")
    print(f"  Models: {len(BACKTEST_MODELS)}")
    print(f"  Cities: {len(ACIS_STATIONS)}")
    print(f"{'='*60}\n")
    
    # Step 1: Pull ACIS actuals
    acis = pull_acis_actuals(days=days)
    
    # Step 2: Pull historical forecasts for each model × city
    all_results = {}
    
    for model_name, om_model_id in BACKTEST_MODELS.items():
        print(f"\n═══ {model_name} ({om_model_id}) ═══")
        model_results = {}
        
        for city, (lat, lon, tz) in CITY_COORDS.items():
            actuals = acis.get("cities", {}).get(city, {}).get("data", {})
            if not actuals:
                print(f"  {city}: no ACIS data, skip")
                continue
            
            print(f"  {city}...", end=" ")
            try:
                forecasts = pull_model_forecasts(model_name, om_model_id, lat, lon, tz, days=days)
                if not forecasts:
                    print("no forecast data")
                    continue
                
                acc = compute_accuracy(forecasts, actuals)
                if acc:
                    model_results[city] = acc
                    print(f"MAE={acc['mae']}°F, bias={acc['bias']:+.1f}°F, n={acc['n']}, within 2°F: {acc['within_2f']}%")
                else:
                    print("no overlap")
                
                time.sleep(1.5)  # Rate limit spacing
            except Exception as e:
                print(f"FAIL ({e})")
                time.sleep(2)
        
        if model_results:
            # Aggregate across all cities
            all_maes = [r["mae"] for r in model_results.values()]
            all_n = sum(r["n"] for r in model_results.values())
            avg_mae = sum(all_maes) / len(all_maes)
            suggested_weight = compute_initial_weight(avg_mae)
            
            all_results[model_name] = {
                "cities": model_results,
                "aggregate": {
                    "avg_mae": round(avg_mae, 2),
                    "min_mae": round(min(all_maes), 2),
                    "max_mae": round(max(all_maes), 2),
                    "total_days": all_n,
                    "cities_tested": len(model_results),
                    "suggested_weight": suggested_weight,
                }
            }
            
            print(f"\n  📊 {model_name} OVERALL: avg MAE={avg_mae:.2f}°F across {len(model_results)} cities")
            print(f"     Suggested initial weight: {suggested_weight}")
    
    # Step 3: Save results
    with open(RESULTS_FILE, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\n✅ Results saved to {RESULTS_FILE}")
    
    # Step 4: Print summary table
    print(f"\n{'='*70}")
    print(f"  SUMMARY — Model Rankings by Average MAE")
    print(f"{'='*70}")
    print(f"  {'Model':<25} {'Avg MAE':>8} {'Cities':>7} {'Days':>6} {'Weight':>8}")
    print(f"  {'-'*25} {'-'*8} {'-'*7} {'-'*6} {'-'*8}")
    
    ranked = sorted(all_results.items(), key=lambda x: x[1]["aggregate"]["avg_mae"])
    for model_name, data in ranked:
        agg = data["aggregate"]
        print(f"  {model_name:<25} {agg['avg_mae']:>7.2f}°F {agg['cities_tested']:>6} {agg['total_days']:>6} {agg['suggested_weight']:>7.3f}")
    
    print(f"\n  Models with avg MAE > 5°F get weight=0 (too inaccurate for trading)")
    print(f"  Weights will be refined over time as live accuracy data accumulates")
    
    return all_results


if __name__ == "__main__":
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 92
    run(days=days)
