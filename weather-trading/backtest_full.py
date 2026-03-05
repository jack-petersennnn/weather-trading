#!/usr/bin/env python3
"""
FULL Backtest: ALL Weather Models vs ACIS Actuals (Kalshi Settlement Source).

Tests every model we use (active + training) across all 19 trading cities,
365 days, against ACIS actual high temps — the same source Kalshi settles from.

Models tested via Open-Meteo Previous Runs API:
  - Forecast: GFS, ICON, HRRR, Canadian GEM, JMA, UKMO, Arpege, MET Norway
  - ECMWF (separate endpoint, uses forecast path on previous-runs)
  - Ensemble: ECMWF, ICON, GFS (also forecast path on previous-runs)

Models tested via their own API (recent days only):
  - Tomorrow.io (limited to ~5 days forecast, no historical)
  - Visual Crossing (limited to ~15 days forecast, no historical)

NWS tested separately (different API).

Outputs: backtest_full_results.json with MAE/bias/RMSE/max_error per model×city
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

# ALL models to backtest via Previous Runs API
# Key: our source name → (open-meteo model id, api_path)
# api_path: "forecast" for all previous-runs (including ensembles)
BACKTEST_MODELS = {
    # Forecast models
    "GFS":                   ("gfs_seamless", "forecast"),
    "ICON":                  ("icon_seamless", "forecast"),
    "HRRR":                  ("ncep_hrrr_conus", "forecast"),
    "Canadian GEM":          ("gem_global", "forecast"),
    "JMA":                   ("jma_gsm", "forecast"),
    "UKMO":                  ("ukmo_global_deterministic_10km", "forecast"),
    "Meteo-France Arpege":   ("arpege_world", "forecast"),
    "MET Norway":            ("metno_seamless", "forecast"),
    # ECMWF (uses forecast path on previous-runs too)
    "ECMWF":                 ("ecmwf_ifs025", "forecast"),
    # Ensemble models (all use forecast path on previous-runs API)
    "Ensemble ECMWF":        ("ecmwf_ifs025", "forecast"),
    "Ensemble ICON":         ("icon_seamless", "forecast"),
    "Ensemble GFS":          ("gfs_seamless", "forecast"),
}

# Note: Tomorrow.io and Visual Crossing don't have historical forecast APIs.
# Their accuracy can only be tracked going forward via training_logger.

RESULTS_FILE = os.path.join(BASE_DIR, "backtest_full_results.json")
ACIS_CACHE_FILE = os.path.join(BASE_DIR, "acis_actuals_365d.json")


def post_json(url, body, timeout=15):
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
    
    if os.path.exists(ACIS_CACHE_FILE):
        with open(ACIS_CACHE_FILE) as f:
            cached = json.load(f)
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
    
    result = {"source": "RCC-ACIS", "pulled_at": datetime.now(timezone.utc).isoformat(),
              "days_requested": days, "cities": {}}
    
    for city, station in ACIS_STATIONS.items():
        print(f"  {city} ({station})...", end=" ", flush=True)
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
    
    with open(ACIS_CACHE_FILE, "w") as f:
        json.dump(result, f, indent=2)
    
    return result


def pull_model_forecasts(model_name, om_model_id, api_path, lat, lon, tz, days=365):
    """Pull historical forecasts from Open-Meteo Previous Runs API."""
    tz_encoded = tz.replace("/", "%2F")
    
    url = (f"https://previous-runs-api.open-meteo.com/v1/{api_path}?"
           f"latitude={lat}&longitude={lon}&models={om_model_id}"
           f"&daily=temperature_2m_max&temperature_unit=fahrenheit"
           f"&timezone={tz_encoded}&past_days={days}&forecast_days=0")
    
    data = fetch_json(url)
    if not data or "daily" not in data:
        return {}
    
    dates = data["daily"].get("time", [])
    
    # Previous Runs API returns "temperature_2m_max" when single model requested
    temps = data["daily"].get("temperature_2m_max", [])
    if not temps:
        # Try model-specific key
        temp_key = f"temperature_2m_max_{om_model_id}"
        temps = data["daily"].get(temp_key, [])
    
    result = {}
    for d, t in zip(dates, temps):
        if t is not None:
            result[d] = float(t)
    
    return result


def compute_accuracy(forecasts, actuals):
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


def compute_weight(mae):
    """Inverse-MAE weight. MAE > 5°F → weight=0."""
    if mae > 5.0:
        return 0.0
    if mae < 0.5:
        mae = 0.5
    return round(2.0 / mae, 3)


def run(days=365):
    print(f"\n{'='*70}")
    print(f"  FULL BACKTEST: ALL Weather Models vs ACIS Actuals")
    print(f"  Window: {days} days | Models: {len(BACKTEST_MODELS)} | Cities: {len(ACIS_STATIONS)}")
    print(f"  Total API calls: ~{len(BACKTEST_MODELS) * len(ACIS_STATIONS)} (rate limited)")
    print(f"{'='*70}\n")
    
    # Step 1: Pull ACIS actuals
    acis = pull_acis_actuals(days=days)
    
    # Step 2: Backtest each model × city
    all_results = {}
    total_calls = 0
    
    for model_name, (om_model_id, api_path) in BACKTEST_MODELS.items():
        print(f"\n═══ {model_name} ({om_model_id} via /{api_path}) ═══")
        model_results = {}
        
        for city, (lat, lon, tz) in CITY_COORDS.items():
            actuals = acis.get("cities", {}).get(city, {}).get("data", {})
            if not actuals:
                print(f"  {city}: no ACIS data, skip")
                continue
            
            print(f"  {city}...", end=" ", flush=True)
            try:
                forecasts = pull_model_forecasts(model_name, om_model_id, api_path, lat, lon, tz, days=days)
                total_calls += 1
                if not forecasts:
                    print("no forecast data")
                    continue
                
                acc = compute_accuracy(forecasts, actuals)
                if acc:
                    model_results[city] = acc
                    print(f"MAE={acc['mae']}°F, bias={acc['bias']:+.1f}°F, n={acc['n']}, ≤2°F: {acc['within_2f']}%")
                else:
                    print("no overlap")
                
                time.sleep(1.2)  # Rate limit: ~50/min to be safe
            except Exception as e:
                print(f"FAIL ({e})")
                time.sleep(3)
        
        if model_results:
            all_maes = [r["mae"] for r in model_results.values()]
            all_biases = [r["bias"] for r in model_results.values()]
            all_n = sum(r["n"] for r in model_results.values())
            avg_mae = sum(all_maes) / len(all_maes)
            avg_bias = sum(all_biases) / len(all_biases)
            suggested_weight = compute_weight(avg_mae)
            
            all_results[model_name] = {
                "om_model_id": om_model_id,
                "cities": model_results,
                "aggregate": {
                    "avg_mae": round(avg_mae, 2),
                    "avg_bias": round(avg_bias, 2),
                    "min_mae": round(min(all_maes), 2),
                    "max_mae": round(max(all_maes), 2),
                    "total_days": all_n,
                    "cities_tested": len(model_results),
                    "suggested_weight": suggested_weight,
                }
            }
            
            print(f"\n  📊 {model_name}: avg MAE={avg_mae:.2f}°F, bias={avg_bias:+.2f}°F, weight={suggested_weight}")
        
        # Save intermediate results after each model (crash protection)
        _interim = {"partial": True, "models_done": list(all_results.keys()),
                     "total_calls": total_calls, "results": all_results}
        with open(RESULTS_FILE + ".partial", "w") as f:
            json.dump(_interim, f, indent=2)
    
    # Step 3: Save final results
    final = {
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "days": days,
        "total_api_calls": total_calls,
        "models": len(all_results),
        "cities": len(ACIS_STATIONS),
        "results": all_results,
    }
    with open(RESULTS_FILE, "w") as f:
        json.dump(final, f, indent=2)
    
    # Clean up partial
    partial = RESULTS_FILE + ".partial"
    if os.path.exists(partial):
        os.remove(partial)
    
    # Step 4: Print summary
    print(f"\n{'='*80}")
    print(f"  FULL BACKTEST RESULTS — {days} Days × {len(ACIS_STATIONS)} Cities")
    print(f"{'='*80}")
    print(f"  {'Model':<25} {'Avg MAE':>8} {'Bias':>7} {'Cities':>7} {'Days':>7} {'≤2°F':>6} {'Weight':>8}")
    print(f"  {'-'*25} {'-'*8} {'-'*7} {'-'*7} {'-'*7} {'-'*6} {'-'*8}")
    
    ranked = sorted(all_results.items(), key=lambda x: x[1]["aggregate"]["avg_mae"])
    for model_name, data in ranked:
        agg = data["aggregate"]
        # Compute average within_2f across cities
        w2fs = [data["cities"][c]["within_2f"] for c in data["cities"]]
        avg_w2f = sum(w2fs) / len(w2fs) if w2fs else 0
        print(f"  {model_name:<25} {agg['avg_mae']:>7.2f}°F {agg['avg_bias']:>+6.2f} {agg['cities_tested']:>6} {agg['total_days']:>7} {avg_w2f:>5.1f}% {agg['suggested_weight']:>7.3f}")
    
    print(f"\n  Note: Ensemble ECMWF/GFS/ICON show DETERMINISTIC results here because")
    print(f"  Previous Runs API doesn't return ensemble means. Live ensemble endpoint")
    print(f"  may differ. These results still validate the underlying model quality.")
    print(f"\n  Tomorrow.io & Visual Crossing: no historical API available.")
    print(f"  Their accuracy tracked via training_logger going forward.")
    
    print(f"\n  Total API calls: {total_calls}")
    print(f"  ✅ Results saved to {RESULTS_FILE}")
    
    return all_results


if __name__ == "__main__":
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 365
    run(days=days)
