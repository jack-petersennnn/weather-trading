#!/usr/bin/env python3
"""
Weekly recalibration: blend last 7 days of live forecast accuracy into
the 365-day backtest results, then regenerate city configs.
"""

import json, math, os, sys, statistics
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen, Request
from urllib.parse import quote

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# City coordinates for Open-Meteo archive API
CITY_COORDS = {
    "New York":      (40.7831, -73.9712, "America/New_York"),
    "Chicago":       (41.8781, -87.6298, "America/Chicago"),
    "Miami":         (25.7617, -80.1918, "America/New_York"),
    "Denver":        (39.7392, -104.9903, "America/Denver"),
    "Austin":        (30.2672, -97.7431, "America/Chicago"),
    "Atlanta":       (33.749, -84.388, "America/New_York"),
    "Boston":        (42.3601, -71.0589, "America/New_York"),
    "Dallas":        (32.7767, -96.797, "America/Chicago"),
    "Houston":       (29.7604, -95.3698, "America/Chicago"),
    "Las Vegas":     (36.1699, -115.1398, "America/Los_Angeles"),
    "Minneapolis":   (44.9778, -93.265, "America/Chicago"),
    "New Orleans":   (29.9511, -90.0715, "America/Chicago"),
    "Oklahoma City": (35.4676, -97.5164, "America/Chicago"),
    "Philadelphia":  (39.9526, -75.1652, "America/New_York"),
    "Phoenix":       (33.4484, -112.074, "America/Phoenix"),
    "San Antonio":   (29.4241, -98.4936, "America/Chicago"),
    "San Francisco": (37.7749, -122.4194, "America/Los_Angeles"),
    "Seattle":       (47.6062, -122.3321, "America/Los_Angeles"),
    "Washington DC": (38.9072, -77.0369, "America/New_York"),
}

def fetch_json(url):
    try:
        req = Request(url, headers={"User-Agent": "weather-recal/1.0"})
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"  ✗ fetch error: {e}")
        return None


def step1_pull_actuals(dates):
    """Pull ACIS actuals for all 19 cities for given dates via Open-Meteo archive."""
    print("\n=== Step 1: Pull actuals from Open-Meteo Archive ===")
    actuals = {}  # city -> date -> temp_max_f
    
    start_date = min(dates)
    end_date = max(dates)
    
    for city, (lat, lon, tz) in CITY_COORDS.items():
        tz_enc = quote(tz, safe='')
        url = (f"https://archive-api.open-meteo.com/v1/archive?"
               f"latitude={lat}&longitude={lon}"
               f"&start_date={start_date}&end_date={end_date}"
               f"&daily=temperature_2m_max&temperature_unit=fahrenheit"
               f"&timezone={tz_enc}")
        data = fetch_json(url)
        if not data or "daily" not in data:
            print(f"  ✗ {city}: no data")
            continue
        
        daily_dates = data["daily"].get("time", [])
        daily_temps = data["daily"].get("temperature_2m_max", [])
        
        actuals[city] = {}
        for d, t in zip(daily_dates, daily_temps):
            if d in dates and t is not None:
                actuals[city][d] = t
        
        count = len(actuals[city])
        print(f"  ✓ {city}: {count} days")
    
    return actuals


def step2_3_compute_weekly_stats(actuals, forecasts, dates):
    """Compare forecasts vs actuals, compute MAE + bias per model per city."""
    print("\n=== Steps 2-3: Compute weekly MAE + bias ===")
    
    # model -> city -> {errors: [], biases: []}
    stats = {}
    
    for date in dates:
        for city in CITY_COORDS:
            actual = actuals.get(city, {}).get(date)
            if actual is None:
                continue
            
            key = f"{city}|{date}"
            entry = forecasts.get(key, {})
            all_fc = {}
            all_fc.update(entry.get("active_forecasts", {}))
            all_fc.update(entry.get("training_forecasts", {}))
            
            for model, fc_val in all_fc.items():
                if fc_val is None:
                    continue
                if model not in stats:
                    stats[model] = {}
                if city not in stats[model]:
                    stats[model][city] = {"errors": [], "biases": []}
                
                error = abs(fc_val - actual)
                bias = fc_val - actual  # positive = warm bias
                stats[model][city]["errors"].append(error)
                stats[model][city]["biases"].append(bias)
    
    # Compute MAE and mean bias
    weekly = {}  # model -> city -> {mae, bias, n}
    for model in stats:
        weekly[model] = {}
        for city in stats[model]:
            errors = stats[model][city]["errors"]
            biases = stats[model][city]["biases"]
            if errors:
                weekly[model][city] = {
                    "mae": statistics.mean(errors),
                    "bias": statistics.mean(biases),
                    "n": len(errors)
                }
    
    # Print summary
    for model in sorted(weekly):
        city_maes = [weekly[model][c]["mae"] for c in weekly[model]]
        if city_maes:
            avg = statistics.mean(city_maes)
            print(f"  {model}: avg MAE={avg:.2f}°F across {len(city_maes)} cities")
    
    return weekly


def step4_5_blend_results(weekly_stats):
    """Blend weekly stats into backtest_full_results.json."""
    print("\n=== Steps 4-5: Blend into backtest results ===")
    
    bt_path = os.path.join(BASE_DIR, "backtest_full_results.json")
    with open(bt_path) as f:
        bt = json.load(f)
    
    old_days = bt.get("days", 365)
    results = bt.get("results", {})
    
    changes = {"updated": 0, "new_models": [], "new_cities": []}
    
    for model in weekly_stats:
        if model not in results:
            # New model not in backtest (training model with live data)
            results[model] = {"om_model_id": "", "cities": {}}
            changes["new_models"].append(model)
        
        for city in weekly_stats[model]:
            w = weekly_stats[model][city]
            week_mae = w["mae"]
            week_bias = w["bias"]
            week_n = w["n"]
            
            if city not in results[model].get("cities", {}):
                if "cities" not in results[model]:
                    results[model]["cities"] = {}
                results[model]["cities"][city] = {
                    "mae": week_mae,
                    "bias": week_bias,
                    "n": week_n,
                    "max_error": max(week_mae * 2, 5.0),
                    "rmse": week_mae * 1.2,
                    "within_1f": 0,
                    "within_2f": 0,
                    "within_3f": 0,
                }
                changes["new_cities"].append(f"{model}/{city}")
            else:
                old = results[model]["cities"][city]
                old_n = old.get("n", old_days)
                
                # Weighted blend
                new_n = old_n + week_n
                new_mae = (old["mae"] * old_n + week_mae * week_n) / new_n
                new_bias = (old["bias"] * old_n + week_bias * week_n) / new_n
                
                old["mae"] = round(new_mae, 3)
                old["bias"] = round(new_bias, 3)
                old["n"] = new_n
                changes["updated"] += 1
    
    bt["days"] = old_days + 7
    bt["last_recalibrated"] = datetime.now(timezone.utc).isoformat()
    bt["results"] = results
    
    with open(bt_path, "w") as f:
        json.dump(bt, f, indent=2)
    
    print(f"  Updated {changes['updated']} model/city pairs")
    print(f"  New models added: {changes['new_models'] or 'none'}")
    print(f"  Days now: {bt['days']}")
    
    return bt, changes


def step7_update_source_weights(bt):
    """Update source_weights.json from new backtest averages."""
    print("\n=== Step 7: Update source_weights.json ===")
    
    sw_path = os.path.join(BASE_DIR, "source_weights.json")
    with open(sw_path) as f:
        sw = json.load(f)
    
    results = bt.get("results", {})
    model_maes = {}
    
    for model, mdata in results.items():
        cities = mdata.get("cities", {})
        maes = [c["mae"] for c in cities.values() if "mae" in c]
        if maes:
            model_maes[model] = statistics.mean(maes)
    
    # Weight = inverse MAE (higher MAE = lower weight), normalized
    if model_maes:
        min_mae = min(model_maes.values())
        new_weights = {}
        for model, mae in model_maes.items():
            if mae > 0:
                new_weights[model] = round(min_mae / mae * 2.0, 3)  # Scale so best = 2.0
            else:
                new_weights[model] = 2.0
        
        # Keep existing models that aren't in backtest (like "Best Match")
        for model in sw.get("weights", {}):
            if model not in new_weights:
                new_weights[model] = sw["weights"][model]
        
        sw["weights"] = new_weights
        sw["source_mae"] = {m: round(v, 2) for m, v in model_maes.items()}
        sw["last_calibrated"] = datetime.now(timezone.utc).isoformat()
        
        with open(sw_path, "w") as f:
            json.dump(sw, f, indent=2)
        
        print(f"  Updated weights for {len(new_weights)} models")
        for m in sorted(model_maes, key=model_maes.get):
            print(f"    {m}: MAE={model_maes[m]:.2f}°F, weight={new_weights.get(m, 0):.3f}")


def step8_check_training_promotions(bt):
    """Check training models for promotion to active."""
    print("\n=== Step 8: Check training model promotions ===")
    
    TRAINING = ['HRRR', 'MET Norway', 'Tomorrow.io', 'Visual Crossing', 'NWS Forecast', 'NWS Hourly']
    results = bt.get("results", {})
    promotions = []
    
    for model in TRAINING:
        if model not in results:
            print(f"  {model}: not in results yet")
            continue
        
        cities = results[model].get("cities", {})
        for city, data in cities.items():
            n = data.get("n", 0)
            mae = data.get("mae", 99)
            bias = data.get("bias", 0)
            
            # Corrected MAE
            if abs(bias) >= mae:
                corr_mae = mae * 0.3
            else:
                corr_mae = math.sqrt(mae**2 - bias**2)
            
            if n >= 14 and corr_mae < 2.0:
                promotions.append({
                    "model": model,
                    "city": city,
                    "n": n,
                    "mae": mae,
                    "corrected_mae": round(corr_mae, 2),
                    "bias": bias
                })
                print(f"  ✓ PROMOTE: {model} in {city} — {n} days, corrected MAE={corr_mae:.2f}°F")
            elif n >= 14:
                print(f"  ✗ {model} in {city}: {n} days but corrected MAE={corr_mae:.2f}°F > 2.0")
            else:
                print(f"  … {model} in {city}: only {n} days tracked")
    
    return promotions


def generate_report(bt, weekly_stats, actuals, changes, promotions, old_config, new_config):
    """Generate the Telegram report."""
    print("\n=== Step 10: Generate report ===")
    
    lines = ["👑📊 Weekly Weather Model Recalibration Report",
             f"Week ending: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
             f"Total backtest days: {bt.get('days', '?')}",
             ""]
    
    # Compare tradeable cities
    old_tradeable = set(old_config.get("tradeable_cities", []))
    new_tradeable = set(new_config.get("tradeable_cities", []))
    gained = new_tradeable - old_tradeable
    lost = old_tradeable - new_tradeable
    
    lines.append(f"🏙️ Tradeable cities: {len(new_tradeable)}")
    if gained:
        lines.append(f"  ✅ Gained: {', '.join(gained)}")
    if lost:
        lines.append(f"  ❌ Lost: {', '.join(lost)}")
    if not gained and not lost:
        lines.append("  No changes")
    lines.append("")
    
    # Family count changes per city
    lines.append("📊 Active model families per city:")
    old_cities = old_config.get("cities", {})
    new_cities = new_config.get("cities", {})
    for city in sorted(new_cities):
        old_w = old_cities.get(city, {}).get("weights", {})
        new_w = new_cities.get(city, {}).get("weights", {})
        old_active = sum(1 for v in old_w.values() if v > 0)
        new_active = sum(1 for v in new_w.values() if v > 0)
        delta = new_active - old_active
        marker = ""
        if delta > 0:
            marker = f" (+{delta} ✅)"
        elif delta < 0:
            marker = f" ({delta} ⚠️)"
        lines.append(f"  {city}: {new_active}{marker}")
    lines.append("")
    
    # Training model promotions
    if promotions:
        lines.append("🎓 Training model promotions:")
        for p in promotions:
            lines.append(f"  {p['model']} → {p['city']}: cMAE={p['corrected_mae']}°F ({p['n']}d)")
    else:
        lines.append("🎓 No training model promotions this week")
    lines.append("")
    
    # Top 3 and bottom 3 models (by global avg MAE)
    results = bt.get("results", {})
    model_avgs = {}
    for model, mdata in results.items():
        maes = [c["mae"] for c in mdata.get("cities", {}).values()]
        if maes:
            model_avgs[model] = statistics.mean(maes)
    
    sorted_models = sorted(model_avgs.items(), key=lambda x: x[1])
    lines.append("🏆 Top 3 models:")
    for m, mae in sorted_models[:3]:
        lines.append(f"  {m}: {mae:.2f}°F MAE")
    lines.append("")
    lines.append("📉 Bottom 3 models:")
    for m, mae in sorted_models[-3:]:
        lines.append(f"  {m}: {mae:.2f}°F MAE")
    lines.append("")
    
    # Notable bias shifts (this week vs overall)
    lines.append("🧭 Notable weekly bias observations:")
    bias_notes = []
    for model in weekly_stats:
        for city in weekly_stats[model]:
            w = weekly_stats[model][city]
            if abs(w["bias"]) > 3.0:
                bias_notes.append(f"  {model}/{city}: {w['bias']:+.1f}°F this week")
    if bias_notes:
        for bn in bias_notes[:5]:
            lines.append(bn)
    else:
        lines.append("  No extreme biases (>3°F) this week")
    
    # Bias drift analysis (7-day vs 14-day)
    lines.append("\n🧭 Bias drift analysis (7d vs 14d):")
    try:
        from bias_drift_tracker import compute_bias_drift
        bias_drift_results = compute_bias_drift()
        
        summary = bias_drift_results.get("summary", {})
        lines.append(f"  Regime shifts detected: {summary.get('warming_shift', 0)} warming, {summary.get('cooling_shift', 0)} cooling")
        
        # Show cities with regime shifts
        flagged_cities = []
        for city, data in bias_drift_results.get("cities", {}).items():
            if data["status"] in ["warming_shift", "cooling_shift"]:
                shift_type = "🔴" if data["status"] == "warming_shift" else "🔵"
                flagged_cities.append(f"  {shift_type} {city}: δ={data['delta']:+.1f}°F")
        
        if flagged_cities:
            for fc in flagged_cities[:3]:  # Limit to top 3
                lines.append(fc)
            if len(flagged_cities) > 3:
                lines.append(f"  ... and {len(flagged_cities) - 3} others")
        else:
            lines.append("  ✅ All cities stable (no regime shifts)")
            
    except Exception as e:
        lines.append(f"  ⚠️  Bias drift analysis failed: {str(e)}")
    
    # Data coverage
    covered_dates = set()
    for city in actuals:
        for d in actuals[city]:
            covered_dates.add(d)
    lines.append(f"\n📅 Dates covered: {', '.join(sorted(covered_dates))}")
    lines.append(f"📈 Model/city pairs updated: {changes['updated']}")
    
    report = "\n".join(lines)
    print(report)
    return report


def main():
    print("=" * 60)
    print("WEEKLY WEATHER MODEL RECALIBRATION")
    print(f"Run time: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)
    
    # Determine the 7-day window
    # We want the last 7 days that could have settled actuals (today is Mar 1)
    # Open-Meteo archive typically has up to yesterday
    today = datetime(2026, 3, 1)
    dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, 8)]
    dates.sort()
    print(f"\nTarget dates: {dates}")
    
    # Load forecast log
    with open(os.path.join(BASE_DIR, "training_forecast_log.json")) as f:
        forecasts = json.load(f)
    
    # Check which dates actually have forecasts
    avail_dates = set()
    for key in forecasts:
        d = key.split("|")[1]
        if d in dates:
            avail_dates.add(d)
    print(f"Dates with forecasts: {sorted(avail_dates)}")
    dates = sorted(avail_dates)
    
    if not dates:
        print("ERROR: No forecast data for the last 7 days!")
        sys.exit(1)
    
    # Save old config for comparison
    cfg_path = os.path.join(BASE_DIR, "city_model_config.json")
    with open(cfg_path) as f:
        old_config = json.load(f)
    
    # Step 1: Pull actuals
    actuals = step1_pull_actuals(dates)
    
    # Steps 2-3: Compute weekly stats
    weekly_stats = step2_3_compute_weekly_stats(actuals, forecasts, dates)
    
    if not weekly_stats:
        print("ERROR: No forecast/actual pairs found!")
        sys.exit(1)
    
    # Steps 4-5: Blend into backtest
    bt, changes = step4_5_blend_results(weekly_stats)
    
    # Step 7: Update source weights
    step7_update_source_weights(bt)
    
    # Step 8: Check training promotions
    promotions = step8_check_training_promotions(bt)
    
    # Load new config for comparison
    with open(cfg_path) as f:
        new_config = json.load(f)
    
    # Generate report
    report = generate_report(bt, weekly_stats, actuals, changes, promotions, old_config, new_config)
    
    # Save report
    with open(os.path.join(BASE_DIR, "recalibration_report.md"), "w") as f:
        f.write(report)
    
    return report


if __name__ == "__main__":
    main()
