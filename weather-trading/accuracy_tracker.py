#!/usr/bin/env python3
"""Forecast Accuracy Tracker v3 — compares predictions vs actuals, auto-calibrates source weights."""

import json
import os
import statistics
import urllib.request
from datetime import datetime, timezone, timedelta

ANALYSIS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/analysis.json"
FORECAST_HISTORY_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/forecast_history.json"
ACCURACY_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/accuracy.json"
WEIGHTS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/source_weights.json"

CITIES_COORDS = {
    "New York":    {"lat": 40.7831, "lon": -73.9712, "tz": "America/New_York"},
    "Chicago":     {"lat": 41.8781, "lon": -87.6298, "tz": "America/Chicago"},
    "Miami":       {"lat": 25.7617, "lon": -80.1918, "tz": "America/New_York"},
    "Denver":      {"lat": 39.7392, "lon": -104.9903, "tz": "America/Denver"},
    "Los Angeles": {"lat": 34.0522, "lon": -118.2437, "tz": "America/Los_Angeles"},
    "Austin":      {"lat": 30.2672, "lon": -97.7431, "tz": "America/Chicago"},
}

# ACIS station IDs — same stations Kalshi settles on (NWS CLI integer high temps)
CITY_ACIS_STATIONS = {
    "New York": "KNYC", "Chicago": "KMDW", "Miami": "KMIA", "Denver": "KDEN",
    "Los Angeles": "KLAX", "Austin": "KAUS", "Philadelphia": "KPHL",
    "Phoenix": "KPHX", "Las Vegas": "KLAS", "Atlanta": "KATL", "Boston": "KBOS",
    "Seattle": "KSEA", "San Francisco": "KSFO", "Houston": "KHOU",
    "San Antonio": "KSAT", "New Orleans": "KMSY", "Oklahoma City": "KOKC",
    "Dallas": "KDFW", "Minneapolis": "KMSP", "Washington DC": "KDCA",
}

# Default weights for reference
DEFAULT_WEIGHTS = {
    "NWS Hourly": 1.5, "ECMWF": 1.4, "NWS Forecast": 1.2,
    "Best Match": 1.1, "GFS": 1.0, "Ensemble ICON": 0.9,
    "Ensemble GFS": 0.9, "Ensemble ECMWF": 0.9,
    "Tomorrow.io": 1.0, "Visual Crossing": 1.0,
}


def fetch_json(url, timeout=15):
    req = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "User-Agent": "KingClaw-Accuracy/3.0"
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"    ✗ {e}")
        return None


def get_actual_temp_acis(city, date_str):
    """Get actual high temp from RCC-ACIS (same source Kalshi settles on).
    Returns integer °F matching the NWS CLI report, or None if not yet available."""
    station = CITY_ACIS_STATIONS.get(city)
    if not station:
        return get_actual_temp_openmeteo(city, date_str)  # fallback for unmapped cities
    try:
        body = json.dumps({
            "sid": station,
            "sdate": date_str,
            "edate": date_str,
            "elems": [{"name": "maxt"}]
        }).encode()
        req = urllib.request.Request(
            "https://data.rcc-acis.org/StnData",
            data=body,
            headers={"Content-Type": "application/json", "User-Agent": "KingClaw-Accuracy/3.1"}
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode())
        for row in data.get("data", []):
            val = row[1]
            if val not in ("M", "T", "S", ""):
                return int(val)
        return None
    except Exception as e:
        print(f"    ✗ ACIS error for {city}: {e}")
        return get_actual_temp_openmeteo(city, date_str)  # fallback


def get_actual_temp_openmeteo(city, date_str):
    """Fallback: Open-Meteo archive (fractional, NOT what Kalshi settles on)."""
    coords = CITIES_COORDS.get(city)
    if not coords:
        return None
    lat, lon, tz = coords["lat"], coords["lon"], coords["tz"]
    tz_encoded = tz.replace("/", "%2F")
    url = (f"https://archive-api.open-meteo.com/v1/archive?"
           f"latitude={lat}&longitude={lon}"
           f"&start_date={date_str}&end_date={date_str}"
           f"&daily=temperature_2m_max&temperature_unit=fahrenheit"
           f"&timezone={tz_encoded}")
    data = fetch_json(url)
    if not data or "daily" not in data:
        return None
    temps = data["daily"].get("temperature_2m_max", [])
    return round(temps[0]) if temps and temps[0] is not None else None


def get_actual_temp(lat, lon, tz, date_str, city=None):
    """Get actual high temp. Uses ACIS (Kalshi settlement source) when city is known."""
    if city and city in CITY_ACIS_STATIONS:
        return get_actual_temp_acis(city, date_str)
    return get_actual_temp_openmeteo(city or "unknown", date_str)


def load_existing():
    if os.path.exists(ACCURACY_FILE):
        with open(ACCURACY_FILE) as f:
            data = json.load(f)
        # Handle old format: "forecasts" → "comparisons"
        if "forecasts" in data and "comparisons" not in data:
            data["comparisons"] = data.pop("forecasts")
        if "comparisons" not in data:
            data["comparisons"] = []
        return data
    return {"comparisons": [], "city_stats": {}, "overall": {}, "source_accuracy": {}}


def load_backtest_mae():
    """Load long-term backtest MAE data from source_accuracy_acis.json (364 days)."""
    bt_file = os.path.join(os.path.dirname(ACCURACY_FILE), "source_accuracy_acis.json")
    if not os.path.exists(bt_file):
        return {}
    try:
        with open(bt_file) as f:
            data = json.load(f)
        # Average MAE across all cities per source
        from collections import defaultdict
        source_maes = defaultdict(list)
        for city_data in data.get("long_term", {}).values():
            for src, info in city_data.items():
                if isinstance(info, dict) and "mae" in info:
                    source_maes[src].append(info["mae"])
        return {src: statistics.mean(maes) for src, maes in source_maes.items() if maes}
    except:
        return {}


def auto_calibrate_weights(comparisons):
    """Auto-calibrate source weights based on live accuracy + backtest data.
    Sources with more data (backtest) get more trusted weights.
    Sources with only live data (few days) get blended with defaults."""
    print("\n  Auto-calibrating source weights...")

    source_errors = {}  # source_name -> list of absolute errors

    for comp in comparisons:
        actual = comp.get("actual")
        source_forecasts = comp.get("source_forecasts", {})
        if actual is None or not source_forecasts:
            continue
        for source, forecast in source_forecasts.items():
            if source not in source_errors:
                source_errors[source] = []
            source_errors[source].append(abs(forecast - actual))

    if not source_errors:
        print("    No source-level data available for calibration")
        return None

    # Compute live MAE per source
    live_mae = {}
    for source, errors in source_errors.items():
        if len(errors) >= 1:
            live_mae[source] = statistics.mean(errors)

    if not live_mae:
        return None

    # Load backtest MAE (364 days, 4 sources: Best Match, ECMWF, GFS, ICON)
    backtest_mae = load_backtest_mae()
    if backtest_mae:
        print(f"    📊 Backtest data: {len(backtest_mae)} sources, 364 days each")

    # Blend live + backtest MAE
    # Backtest = proven over 364 days but older data
    # Live = recent but only a few days
    # Formula: more live data → trust live more
    source_mae = {}
    for source in set(list(live_mae.keys()) + list(backtest_mae.keys())):
        lv = live_mae.get(source)
        bt = backtest_mae.get(source)
        n_live = len(source_errors.get(source, []))
        
        if lv and bt:
            # Blend: live gets 40-80% based on sample count (caps at 20 samples)
            live_blend = min(0.8, 0.4 + (n_live / 20) * 0.4)
            source_mae[source] = live_blend * lv + (1 - live_blend) * bt
        elif lv:
            source_mae[source] = lv  # live only (no backtest for this source)
        elif bt:
            source_mae[source] = bt  # backtest only (source not in live scanner yet)

    # Convert MAE to weights: lower MAE → higher weight
    avg_mae = statistics.mean(source_mae.values())
    new_weights = {}
    for source, mae in source_mae.items():
        if mae > 0:
            ratio = avg_mae / mae
            weight = max(0.5, min(2.0, ratio))
            # Blend with default weight — less blending for sources with more data
            default = DEFAULT_WEIGHTS.get(source, 1.0)
            n_total = len(source_errors.get(source, []))
            if source in backtest_mae:
                n_total += 30  # Credit backtested sources with equivalent of 30 extra live samples
            blend = min(n_total / 10, 0.85)  # max 85% data at 10+ equivalent samples
            weight = blend * weight + (1 - blend) * default
        else:
            weight = DEFAULT_WEIGHTS.get(source, 1.0)
        new_weights[source] = round(weight, 3)

    # Print calibration results
    for source in sorted(new_weights.keys()):
        mae = source_mae.get(source, 0)
        old = DEFAULT_WEIGHTS.get(source, 1.0)
        new = new_weights[source]
        n_live = len(source_errors.get(source, []))
        bt_tag = " +BT" if source in backtest_mae else ""
        print(f"    {source:<20} MAE={mae:.2f}°F  weight: {old:.1f} → {new:.3f}  (n={n_live}{bt_tag})")

    # Apply MAE cutoff — sources above threshold get weight 0 (excluded from ensemble)
    # They're still tracked so they can recover over time
    MAE_CUTOFF = 4.0  # °F — if you're off by more than 4 degrees on average, you're out
    excluded = []
    for source, mae in source_mae.items():
        if mae > MAE_CUTOFF:
            new_weights[source] = 0.0
            excluded.append(f"{source} (MAE={mae:.1f}°F)")
    
    if excluded:
        print(f"\n    🚫 Excluded (MAE > {MAE_CUTOFF}°F): {', '.join(excluded)}")
        print(f"    (Still tracked — will auto-include if MAE improves)")

    return new_weights, source_mae


def save_weights(weights, source_mae=None):
    """Save updated weights + MAE data to source_weights.json."""
    try:
        data = {
            "weights": weights,
            "last_calibrated": datetime.now(timezone.utc).isoformat(),
        }
        # Include MAE data so scanner can see source quality
        if source_mae:
            data["source_mae"] = {k: round(v, 2) for k, v in source_mae.items()}
        
        if os.path.exists(WEIGHTS_FILE):
            with open(WEIGHTS_FILE) as f:
                existing = json.load(f)
            history = existing.get("calibration_history", [])
            history.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "weights": weights
            })
            data["calibration_history"] = history[-10:]
        with open(WEIGHTS_FILE, "w") as f:
            json.dump(data, f, indent=2)
        print(f"    Saved updated weights to {WEIGHTS_FILE}")
    except Exception as e:
        print(f"    Warning: Could not save weights: {e}")


def run():
    print("╔══════════════════════════════════════════════╗")
    print("║  KingClaw Accuracy Tracker v3.0              ║")
    print("╚══════════════════════════════════════════════╝")
    now = datetime.now(timezone.utc)
    today_str = now.strftime("%Y-%m-%d")
    print(f"Time: {now.strftime('%Y-%m-%d %H:%M UTC')}")

    # Read from forecast_history.json (persistent) — NOT analysis.json (gets overwritten)
    forecast_history = {}
    try:
        with open(FORECAST_HISTORY_FILE) as f:
            forecast_history = json.load(f)
    except FileNotFoundError:
        pass
    
    # Fallback: also check analysis.json for any current data
    try:
        with open(ANALYSIS_FILE) as f:
            analysis = json.load(f)
    except FileNotFoundError:
        analysis = {}

    # Build a unified list of city/date/forecast entries from both sources
    forecast_entries = []
    
    # Primary: forecast_history.json (has all historical snapshots)
    for key, entry in forecast_history.items():
        city = entry.get("city", "")
        target_date = entry.get("target_date", "")
        ensemble_mean = entry.get("final_ensemble_mean")
        source_forecasts = entry.get("final_source_forecasts", {})
        series = entry.get("series", "")
        calibrated_std = entry.get("final_calibrated_std")
        if city and target_date and ensemble_mean is not None:
            forecast_entries.append({
                "city": city, "series": series, "target_date": target_date,
                "ensemble_mean": ensemble_mean, "source_forecasts": source_forecasts,
                "calibrated_std": calibrated_std,
            })
    
    # Fallback: analysis.json events (for anything not in history yet)
    history_keys = {(e["city"], e["target_date"]) for e in forecast_entries}
    for series, cdata in analysis.get("cities", {}).items():
        city = cdata.get("city", "")
        for event in cdata.get("events", []):
            td = event.get("target_date")
            em = event.get("ensemble_mean")
            if td and em is not None and (city, td) not in history_keys:
                forecast_entries.append({
                    "city": city, "series": series, "target_date": td,
                    "ensemble_mean": em, "source_forecasts": event.get("source_forecasts", {}),
                    "calibrated_std": event.get("calibrated_std"),
                })

    existing = load_existing()
    checked_keys = {(c["city"], c["target_date"]) for c in existing["comparisons"]}
    new_comparisons = 0

    for entry in forecast_entries:
        city = entry["city"]
        target_date = entry["target_date"]
        ensemble_mean = entry["ensemble_mean"]
        series = entry["series"]

        coords = CITIES_COORDS.get(city)
        if not coords:
            continue

        if target_date >= today_str:
            continue

        if (city, target_date) in checked_keys:
            continue

        print(f"  Checking {city} {target_date}...", end=" ")
        actual = get_actual_temp(coords["lat"], coords["lon"], coords["tz"], target_date, city=city)

        if actual is None:
            print("no actual data yet")
            continue

        error = round(abs(ensemble_mean - actual), 1)

        source_forecasts = entry.get("source_forecasts", {})

        comparison = {
            "city": city,
            "series": series,
            "target_date": target_date,
            "predicted": ensemble_mean,
            "actual": actual,
            "error": error,
            "direction_correct": abs(ensemble_mean - actual) <= 3,
            "sources_used": list(source_forecasts.keys()),
            "source_forecasts": source_forecasts,
            "ensemble_std": entry.get("ensemble_std"),
            "calibrated_std": entry.get("calibrated_std"),
            "checked_at": now.isoformat(),
        }
        existing["comparisons"].append(comparison)
        checked_keys.add((city, target_date))
        new_comparisons += 1
        print(f"predicted={ensemble_mean} actual={actual} error={error}°F")

    # Compute stats
    comparisons = existing["comparisons"]
    if comparisons:
        all_errors = [c["error"] for c in comparisons]
        existing["overall"] = {
            "mae": round(statistics.mean(all_errors), 2),
            "median_error": round(statistics.median(all_errors), 2),
            "max_error": round(max(all_errors), 1),
            "min_error": round(min(all_errors), 1),
            "total_predictions": len(comparisons),
            "direction_accuracy": round(sum(1 for c in comparisons if c["direction_correct"]) / len(comparisons) * 100, 1),
            "updated": now.isoformat(),
        }

        # Per-city stats
        city_stats = {}
        for c in comparisons:
            city = c["city"]
            if city not in city_stats:
                city_stats[city] = {"errors": [], "correct": 0, "total": 0}
            city_stats[city]["errors"].append(c["error"])
            city_stats[city]["total"] += 1
            if c["direction_correct"]:
                city_stats[city]["correct"] += 1

        for city, s in city_stats.items():
            city_stats[city] = {
                "mae": round(statistics.mean(s["errors"]), 2),
                "predictions": s["total"],
                "direction_accuracy": round(s["correct"] / s["total"] * 100, 1) if s["total"] > 0 else 0,
            }

        existing["city_stats"] = city_stats

        if city_stats:
            best = min(city_stats, key=lambda c: city_stats[c]["mae"])
            worst = max(city_stats, key=lambda c: city_stats[c]["mae"])
            existing["overall"]["best_city"] = best
            existing["overall"]["best_city_mae"] = city_stats[best]["mae"]
            existing["overall"]["worst_city"] = worst
            existing["overall"]["worst_city_mae"] = city_stats[worst]["mae"]

    # Auto-calibrate source weights
    cal_result = auto_calibrate_weights(comparisons)
    if cal_result:
        new_weights, source_mae = cal_result
        save_weights(new_weights, source_mae=source_mae)
        existing["source_accuracy"] = {
            s: {"mae": round(m, 2), "weight": new_weights.get(s, 1.0)}
            for s, m in source_mae.items()
        }

    with open(ACCURACY_FILE, "w") as f:
        json.dump(existing, f, indent=2)

    print(f"\n  New comparisons: {new_comparisons}")
    print(f"  Total tracked: {len(comparisons)}")
    if existing.get("overall", {}).get("mae") is not None:
        print(f"  Overall MAE: {existing['overall']['mae']}°F")
    print(f"Saved to {ACCURACY_FILE}")
    return existing


if __name__ == "__main__":
    run()
