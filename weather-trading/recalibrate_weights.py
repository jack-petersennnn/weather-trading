#!/usr/bin/env python3
"""
Weight Recalibrator
Two-tier weighting: 70% long-term + 30% rolling 30-day, per-city weights.
Uses inverse MAE against NWS CLI actuals.

Usage:
    python3 recalibrate_weights.py           # Full recalibration
    python3 recalibrate_weights.py --update   # Append new data and recalculate
"""
import json, os, sys, statistics
from datetime import datetime, timedelta
from collections import defaultdict

BASE_DIR = os.path.dirname(__file__)
ACTUALS_FILE = os.path.join(BASE_DIR, "nws_cli_actuals.json")
FORECASTS_FILE = os.path.join(BASE_DIR, "historical_forecasts.json")
WEIGHTS_FILE = os.path.join(BASE_DIR, "source_weights.json")
CALIBRATION_FILE = os.path.join(BASE_DIR, "city_calibration.json")

LONG_TERM_WEIGHT = 0.70
ROLLING_WEIGHT = 0.30
ROLLING_DAYS = 30

# All 10 sources — some won't have historical data
ALL_SOURCES = [
    "NWS Forecast", "NWS Hourly", "ECMWF", "GFS", "Best Match",
    "Ensemble ICON", "Ensemble GFS", "Ensemble ECMWF",
    "Tomorrow.io", "Visual Crossing"
]

def compute_mae(actuals_dict, forecasts_dict, dates=None):
    """Compute MAE between actuals and forecasts for given dates."""
    if dates is None:
        dates = set(actuals_dict.keys()) & set(forecasts_dict.keys())
    else:
        dates = set(dates) & set(actuals_dict.keys()) & set(forecasts_dict.keys())
    
    if not dates:
        return None
    
    errors = [abs(forecasts_dict[d] - actuals_dict[d]) for d in dates]
    return statistics.mean(errors)

def inverse_mae_weights(mae_dict):
    """Convert MAE dict to normalized weights (inverse MAE). Lower MAE = higher weight."""
    if not mae_dict:
        return {}
    
    # Inverse: weight = 1/MAE
    inv = {s: 1.0 / mae for s, mae in mae_dict.items() if mae and mae > 0}
    if not inv:
        return {}
    
    # Normalize to mean=1.0
    mean_inv = statistics.mean(inv.values())
    return {s: round(w / mean_inv, 3) for s, w in inv.items()}

def recalibrate(update_mode=False):
    print("⚖️  Weight Recalibrator (NWS CLI Backtest)")
    print("=" * 60)
    
    with open(ACTUALS_FILE) as f:
        actuals = json.load(f)
    with open(FORECASTS_FILE) as f:
        forecasts = json.load(f)
    
    # Merge NWS CLI + extended actuals per city
    merged_actuals = {}
    for city, city_data in actuals.get("cities", {}).items():
        merged = {}
        merged.update(city_data.get("extended_data", {}))  # extended first
        merged.update(city_data.get("data", {}))  # NWS CLI overrides
        merged_actuals[city] = merged
    
    # Determine date ranges
    all_dates = set()
    for city_actual in merged_actuals.values():
        all_dates.update(city_actual.keys())
    
    if not all_dates:
        print("❌ No actuals data")
        return
    
    all_dates_sorted = sorted(all_dates)
    cutoff_30d = (datetime.strptime(all_dates_sorted[-1], "%Y-%m-%d") - timedelta(days=ROLLING_DAYS)).strftime("%Y-%m-%d")
    rolling_dates = {d for d in all_dates if d >= cutoff_30d}
    
    print(f"  Date range: {all_dates_sorted[0]} to {all_dates_sorted[-1]} ({len(all_dates)} days)")
    print(f"  Rolling window: {cutoff_30d} to {all_dates_sorted[-1]} ({len(rolling_dates)} days)")
    
    # === Global weights ===
    print("\n📊 Computing global weights...")
    
    global_long_mae = defaultdict(list)
    global_roll_mae = defaultdict(list)
    
    # Per-city weights
    city_long_mae = defaultdict(lambda: defaultdict(list))
    city_roll_mae = defaultdict(lambda: defaultdict(list))
    
    for city in actuals.get("cities", {}):
        actual_data = actuals["cities"][city].get("data", {})
        forecast_data = forecasts.get("cities", {}).get(city, {})
        
        for source_name, source_preds in forecast_data.items():
            # Long-term errors
            for d in actual_data:
                if d in source_preds:
                    err = abs(source_preds[d] - actual_data[d])
                    global_long_mae[source_name].append(err)
                    city_long_mae[city][source_name].append(err)
                    
                    if d in rolling_dates:
                        global_roll_mae[source_name].append(err)
                        city_roll_mae[city][source_name].append(err)
    
    # Compute blended global weights
    long_mae = {s: statistics.mean(errs) for s, errs in global_long_mae.items() if errs}
    roll_mae = {s: statistics.mean(errs) for s, errs in global_roll_mae.items() if errs}
    
    long_weights = inverse_mae_weights(long_mae)
    roll_weights = inverse_mae_weights(roll_mae)
    
    # Blend
    global_weights = {}
    for source in set(long_weights) | set(roll_weights):
        lw = long_weights.get(source)
        rw = roll_weights.get(source)
        if lw is not None and rw is not None:
            global_weights[source] = round(LONG_TERM_WEIGHT * lw + ROLLING_WEIGHT * rw, 3)
        elif lw is not None:
            global_weights[source] = lw
        elif rw is not None:
            global_weights[source] = rw
    
    # For sources without historical data, keep existing weights
    existing_weights = {}
    if os.path.exists(WEIGHTS_FILE):
        with open(WEIGHTS_FILE) as f:
            existing_weights = json.load(f).get("weights", {})
    
    for source in ALL_SOURCES:
        if source not in global_weights and source in existing_weights:
            global_weights[source] = existing_weights[source]
            print(f"  ⚠ {source}: No historical data, keeping existing weight {existing_weights[source]}")
    
    print(f"\n  {'Source':<20} {'LT MAE':>7} {'LT Wt':>7} {'Roll MAE':>9} {'Roll Wt':>8} {'Final':>7}")
    print(f"  {'─'*65}")
    for source in sorted(global_weights.keys(), key=lambda s: global_weights[s], reverse=True):
        lt_m = long_mae.get(source)
        lt_w = long_weights.get(source)
        rl_m = roll_mae.get(source)
        rl_w = roll_weights.get(source)
        fw = global_weights[source]
        print(f"  {source:<20} {lt_m if lt_m else 'N/A':>7} {lt_w if lt_w else 'N/A':>7} "
              f"{rl_m if rl_m else 'N/A':>9} {rl_w if rl_w else 'N/A':>8} {fw:>7}")
    
    # === Per-city weights ===
    print("\n📊 Computing per-city weights...")
    city_weights = {}
    
    for city in actuals.get("cities", {}):
        lt_mae = {s: statistics.mean(errs) for s, errs in city_long_mae[city].items() if errs}
        rl_mae = {s: statistics.mean(errs) for s, errs in city_roll_mae[city].items() if errs}
        
        lt_w = inverse_mae_weights(lt_mae)
        rl_w = inverse_mae_weights(rl_mae)
        
        blended = {}
        for source in set(lt_w) | set(rl_w):
            lw = lt_w.get(source)
            rw = rl_w.get(source)
            if lw is not None and rw is not None:
                blended[source] = round(LONG_TERM_WEIGHT * lw + ROLLING_WEIGHT * rw, 3)
            elif lw is not None:
                blended[source] = lw
            elif rw is not None:
                blended[source] = rw
        
        # Fill missing from global
        for source in ALL_SOURCES:
            if source not in blended and source in global_weights:
                blended[source] = global_weights[source]
        
        city_weights[city] = blended
        
        print(f"\n  📍 {city}")
        best = sorted(blended.items(), key=lambda x: x[1], reverse=True)[:3]
        worst = sorted(blended.items(), key=lambda x: x[1])[:2]
        print(f"    Best:  {', '.join(f'{s}={w:.3f}' for s, w in best)}")
        print(f"    Worst: {', '.join(f'{s}={w:.3f}' for s, w in worst)}")
    
    # === Update city_calibration.json std multipliers ===
    print("\n📊 Updating city calibration std multipliers...")
    city_cal = {}
    if os.path.exists(CALIBRATION_FILE):
        with open(CALIBRATION_FILE) as f:
            city_cal = json.load(f)
    
    for city in actuals.get("cities", {}):
        actual_data = actuals["cities"][city].get("data", {})
        forecast_data = forecasts.get("cities", {}).get(city, {})
        
        # Compute actual forecast errors to calibrate std
        all_city_errors = []
        for source_name, source_preds in forecast_data.items():
            for d in actual_data:
                if d in source_preds:
                    all_city_errors.append(abs(source_preds[d] - actual_data[d]))
        
        if all_city_errors and len(all_city_errors) >= 10:
            # std multiplier based on typical prediction error magnitude
            # Higher errors = need wider confidence intervals
            city_mae = statistics.mean(all_city_errors)
            # Normalize: average MAE of ~3°F = multiplier 1.0
            multiplier = round(city_mae / 3.0, 2)
            multiplier = max(0.5, min(2.0, multiplier))  # clamp
            
            if city in city_cal.get("calibrations", {}):
                old = city_cal["calibrations"][city].get("adjusted_std_multiplier", 1.0)
                city_cal["calibrations"][city]["adjusted_std_multiplier"] = multiplier
                city_cal["calibrations"][city]["historical_mae"] = round(city_mae, 2)
                city_cal["calibrations"][city]["sample_size"] = len(all_city_errors)
                print(f"  {city}: std_mult {old} → {multiplier} (MAE={city_mae:.2f}°)")
    
    city_cal["last_updated"] = datetime.utcnow().isoformat() + "Z"
    
    with open(CALIBRATION_FILE, "w") as f:
        json.dump(city_cal, f, indent=2)
    print(f"  💾 Updated {CALIBRATION_FILE}")
    
    # === Build output ===
    output = {
        "weights": global_weights,
        "city_weights": city_weights,
        "last_calibrated": datetime.utcnow().isoformat() + "Z",
        "method": "nws_cli_backtest_70_30",
        "stats": {
            "long_term_days": len(all_dates),
            "rolling_days": len(rolling_dates),
            "long_term_weight": LONG_TERM_WEIGHT,
            "rolling_weight": ROLLING_WEIGHT,
            "date_range": f"{all_dates_sorted[0]} to {all_dates_sorted[-1]}",
            "sources_with_historical": sorted(long_mae.keys()),
            "sources_without_historical": [s for s in ALL_SOURCES if s not in long_mae],
        },
        "mae_details": {
            "global_long_term": {s: round(v, 2) for s, v in long_mae.items()},
            "global_rolling": {s: round(v, 2) for s, v in roll_mae.items()},
        },
        "calibration_history": []
    }
    
    # Preserve calibration history
    if os.path.exists(WEIGHTS_FILE):
        with open(WEIGHTS_FILE) as f:
            old = json.load(f)
        output["calibration_history"] = old.get("calibration_history", [])[-5:]
        output["calibration_history"].append({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "weights": global_weights,
            "method": "nws_cli_backtest_70_30"
        })
    
    with open(WEIGHTS_FILE, "w") as f:
        json.dump(output, f, indent=2)
    
    print(f"\n💾 Saved weights to {WEIGHTS_FILE}")
    print(f"\n✅ Recalibration complete!")
    print(f"   Global weights: {json.dumps(global_weights, indent=2)}")
    
    return output

def append_new_actual(city, date_str, actual_temp):
    """Hook for settle_trades.py: append a new NWS CLI actual and trigger recalibration."""
    if not os.path.exists(ACTUALS_FILE):
        return
    
    with open(ACTUALS_FILE) as f:
        data = json.load(f)
    
    if city not in data.get("cities", {}):
        return
    
    data["cities"][city]["data"][date_str] = actual_temp
    
    with open(ACTUALS_FILE, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"  📝 Appended NWS CLI actual: {city} {date_str} = {actual_temp}°F")

if __name__ == "__main__":
    if "--update" in sys.argv:
        print("Running incremental update mode...")
    recalibrate()
