#!/usr/bin/env python3
"""Full recalibration pipeline: collect NWS CLI, compare with Open-Meteo, compute weights."""

import json, requests, time, statistics, re
from datetime import datetime, timedelta
from collections import defaultdict

WORKDIR = "/home/ubuntu/.openclaw/workspace/weather-trading"

CITIES = {
    "New York":    {"lat": 40.7831, "lon": -73.9712, "tz": "America/New_York",    "cli": "NYC"},
    "Chicago":     {"lat": 41.8781, "lon": -87.6298, "tz": "America/Chicago",     "cli": "ORD"},
    "Miami":       {"lat": 25.7617, "lon": -80.1918, "tz": "America/New_York",    "cli": "MIA"},
    "Denver":      {"lat": 39.7392, "lon": -104.9903,"tz": "America/Denver",      "cli": "DEN"},
    "Los Angeles": {"lat": 34.0522, "lon": -118.2437,"tz": "America/Los_Angeles", "cli": "LAX"},
    "Austin":      {"lat": 30.2672, "lon": -97.7431, "tz": "America/Chicago",     "cli": "AUS"},
}

# ── Step 1: Collect NWS CLI data ──
def collect_nws_cli():
    print("=== Step 1: Collecting NWS CLI data ===")
    with open(f"{WORKDIR}/nws_cli_actuals.json") as f:
        actuals = json.load(f)
    
    for city, info in CITIES.items():
        code = info["cli"]
        print(f"  Fetching NWS CLI for {city} ({code})...")
        try:
            resp = requests.get(
                f"https://api.weather.gov/products/types/CLI/locations/{code}",
                headers={"User-Agent": "weather-trading/1.0"},
                timeout=30
            )
            if resp.status_code != 200:
                print(f"    HTTP {resp.status_code}, skipping")
                continue
            products = resp.json().get("@graph", [])
            print(f"    Found {len(products)} CLI products")
            
            existing = set(actuals["cities"][city].get("data", {}).keys())
            new_count = 0
            
            for product in products[:30]:  # Get up to 30 most recent
                prod_id = product.get("id", "")
                try:
                    detail = requests.get(
                        f"https://api.weather.gov/products/{prod_id}",
                        headers={"User-Agent": "weather-trading/1.0"},
                        timeout=15
                    )
                    if detail.status_code != 200:
                        continue
                    text = detail.json().get("productText", "")
                    
                    # Parse date
                    date_match = re.search(r'CLIMATE REPORT FOR\s+.*?(\d{1,2}/\d{1,2}/\d{4})', text, re.IGNORECASE)
                    if not date_match:
                        date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', text)
                    if not date_match:
                        continue
                    
                    date_str = date_match.group(1)
                    dt = datetime.strptime(date_str, "%m/%d/%Y")
                    date_key = dt.strftime("%Y-%m-%d")
                    
                    # Parse max temp
                    max_match = re.search(r'MAXIMUM\s+TEMPERATURE.*?(\d+)', text)
                    if not max_match:
                        max_match = re.search(r'TODAY.*?MAXIMUM.*?(\d+)', text)
                    if not max_match:
                        # Try table format
                        lines = text.split('\n')
                        for i, line in enumerate(lines):
                            if 'MAXIMUM' in line.upper() and 'TEMPERATURE' in line.upper():
                                nums = re.findall(r'\b(\d{1,3})\b', line)
                                if nums:
                                    max_match = type('', (), {'group': lambda self, x: nums[0]})()
                                    break
                    
                    if max_match:
                        temp = int(max_match.group(1))
                        if 0 < temp < 140:  # Sanity check
                            if date_key not in existing:
                                new_count += 1
                            actuals["cities"][city]["data"][date_key] = temp
                    
                    time.sleep(0.3)
                except Exception as e:
                    continue
            
            print(f"    Total: {len(actuals['cities'][city]['data'])} days ({new_count} new)")
            time.sleep(0.5)
        except Exception as e:
            print(f"    Error: {e}")
    
    with open(f"{WORKDIR}/nws_cli_actuals.json", "w") as f:
        json.dump(actuals, f, indent=2)
    
    return actuals

# ── Step 2: Fetch any missing Open-Meteo archive data ──
def ensure_openmeteo_actuals(actuals):
    print("\n=== Step 2: Ensuring Open-Meteo archive actuals ===")
    end_date = "2026-02-19"
    start_date = "2025-02-20"
    
    for city, info in CITIES.items():
        existing = actuals["cities"][city].get("extended_data", {})
        if len(existing) >= 360:
            print(f"  {city}: already have {len(existing)} days, checking for gaps...")
        
        # Fetch full range to fill any gaps
        url = (f"https://archive-api.open-meteo.com/v1/archive?"
               f"latitude={info['lat']}&longitude={info['lon']}"
               f"&start_date={start_date}&end_date={end_date}"
               f"&daily=temperature_2m_max&temperature_unit=fahrenheit"
               f"&timezone={info['tz']}")
        
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                dates = data.get("daily", {}).get("time", [])
                temps = data.get("daily", {}).get("temperature_2m_max", [])
                
                if not actuals["cities"][city].get("extended_data"):
                    actuals["cities"][city]["extended_data"] = {}
                
                added = 0
                for d, t in zip(dates, temps):
                    if t is not None and d not in existing:
                        actuals["cities"][city]["extended_data"][d] = round(t, 1)
                        added += 1
                
                total = len(actuals["cities"][city]["extended_data"])
                print(f"  {city}: {total} total days ({added} new from Open-Meteo)")
            else:
                print(f"  {city}: HTTP {resp.status_code}")
        except Exception as e:
            print(f"  {city}: Error: {e}")
        
        time.sleep(0.5)
    
    with open(f"{WORKDIR}/nws_cli_actuals.json", "w") as f:
        json.dump(actuals, f, indent=2)
    
    return actuals

# ── Step 3: NWS CLI vs Open-Meteo delta analysis ──
def compute_deltas(actuals):
    print("\n=== Step 3: NWS CLI vs Open-Meteo Delta Analysis ===")
    deltas = {}
    
    for city in CITIES:
        cli_data = actuals["cities"][city].get("data", {})
        ext_data = actuals["cities"][city].get("extended_data", {})
        
        city_deltas = []
        for date, cli_temp in cli_data.items():
            if date in ext_data:
                delta = ext_data[date] - cli_temp
                city_deltas.append({"date": date, "cli": cli_temp, "openmeteo": ext_data[date], "delta": round(delta, 1)})
        
        if city_deltas:
            avg_delta = statistics.mean([d["delta"] for d in city_deltas])
            std_delta = statistics.stdev([d["delta"] for d in city_deltas]) if len(city_deltas) > 1 else 0
            mae = statistics.mean([abs(d["delta"]) for d in city_deltas])
            deltas[city] = {
                "n_comparisons": len(city_deltas),
                "mean_delta": round(avg_delta, 2),
                "std_delta": round(std_delta, 2),
                "mae": round(mae, 2),
                "details": city_deltas
            }
            print(f"  {city}: {len(city_deltas)} overlap days, mean delta={avg_delta:.2f}°F, MAE={mae:.2f}°F")
        else:
            deltas[city] = {"n_comparisons": 0, "mean_delta": 0, "std_delta": 0, "mae": 0}
            print(f"  {city}: no overlapping days")
    
    return deltas

# ── Step 4: Source accuracy analysis ──
def analyze_accuracy(actuals):
    print("\n=== Step 4: Source Accuracy Analysis ===")
    with open(f"{WORKDIR}/historical_forecasts.json") as f:
        forecasts = json.load(f)
    
    # Use extended_data (Open-Meteo) as ground truth for full backtest
    results = {}
    city_results = {}
    
    for city in CITIES:
        ext_data = actuals["cities"][city].get("extended_data", {})
        city_forecasts = forecasts.get("cities", {}).get(city, {})
        
        city_results[city] = {}
        
        for model, model_data in city_forecasts.items():
            errors = []
            for date, forecast_temp in model_data.items():
                if date in ext_data and forecast_temp is not None:
                    error = abs(forecast_temp - ext_data[date])
                    errors.append(error)
            
            if errors:
                mae = statistics.mean(errors)
                if model not in results:
                    results[model] = []
                results[model].extend(errors)
                city_results[city][model] = {
                    "mae": round(mae, 2),
                    "n_days": len(errors)
                }
    
    print("\n  Overall MAE by source:")
    overall = {}
    for model, errors in sorted(results.items(), key=lambda x: statistics.mean(x[1])):
        mae = statistics.mean(errors)
        overall[model] = round(mae, 2)
        print(f"    {model}: {mae:.2f}°F ({len(errors)} days)")
    
    print("\n  Per-city MAE:")
    for city in CITIES:
        best = min(city_results[city].items(), key=lambda x: x[1]["mae"])
        worst = max(city_results[city].items(), key=lambda x: x[1]["mae"])
        print(f"    {city}: best={best[0]} ({best[1]['mae']}°F), worst={worst[0]} ({worst[1]['mae']}°F)")
    
    # Also check accuracy against NWS CLI specifically
    print("\n  MAE against NWS CLI (ground truth):")
    cli_accuracy = {}
    for city in CITIES:
        cli_data = actuals["cities"][city].get("data", {})
        city_forecasts = forecasts.get("cities", {}).get(city, {})
        
        for model, model_data in city_forecasts.items():
            errors = []
            for date, forecast_temp in model_data.items():
                if date in cli_data and forecast_temp is not None:
                    error = abs(forecast_temp - cli_data[date])
                    errors.append(error)
            if errors:
                if model not in cli_accuracy:
                    cli_accuracy[model] = []
                cli_accuracy[model].extend(errors)
    
    for model, errors in sorted(cli_accuracy.items(), key=lambda x: statistics.mean(x[1])):
        mae = statistics.mean(errors)
        print(f"    {model}: {mae:.2f}°F ({len(errors)} days vs NWS CLI)")
    
    return overall, city_results

# ── Step 5: Recalibrate weights ──
def recalibrate(actuals, city_results):
    print("\n=== Step 5: Recalibrating Weights ===")
    
    with open(f"{WORKDIR}/source_weights.json") as f:
        old_weights_data = json.load(f)
    old_weights = old_weights_data["weights"]
    
    with open(f"{WORKDIR}/historical_forecasts.json") as f:
        forecasts = json.load(f)
    
    # Compute global weights from inverse MAE
    global_maes = {}
    for city in CITIES:
        for model, info in city_results[city].items():
            if model not in global_maes:
                global_maes[model] = []
            global_maes[model].append(info["mae"])
    
    avg_maes = {m: statistics.mean(v) for m, v in global_maes.items()}
    
    # Inverse MAE weighting, normalized
    inv_maes = {m: 1.0 / mae for m, mae in avg_maes.items()}
    total_inv = sum(inv_maes.values())
    n_models = len(inv_maes)
    
    # Scale so average weight = 1.0 (total weights = n_models)
    new_global = {m: round((v / total_inv) * n_models, 3) for m, v in inv_maes.items()}
    
    # Per-city weights using same approach
    city_weights = {}
    for city in CITIES:
        city_maes = {m: info["mae"] for m, info in city_results[city].items()}
        inv = {m: 1.0 / mae for m, mae in city_maes.items()}
        total = sum(inv.values())
        n = len(inv)
        city_weights[city] = {m: round((v / total) * n, 3) for m, v in inv.items()}
    
    # 70% long-term global / 30% per-city blend
    blended_city_weights = {}
    for city in CITIES:
        blended = {}
        for model in new_global:
            if model in city_weights[city]:
                g = new_global[model]
                c = city_weights[city][model]
                blended[model] = round(0.7 * g + 0.3 * c, 3)
        blended_city_weights[city] = blended
    
    # Also need to map to the sources used in analyzer.py
    # The forecasts use: Best Match, GFS, ECMWF, Ensemble ICON, Ensemble GFS, Ensemble ECMWF
    # The old weights have: NWS Forecast, NWS Hourly, ECMWF, GFS, Best Match, Ensemble ICON, etc.
    # Keep old sources that aren't in historical forecasts, update those that are
    
    final_global = dict(old_weights)  # start with old
    for model, weight in new_global.items():
        final_global[model] = weight
    
    print("\n  Global weights (old → new):")
    for model in sorted(set(list(old_weights.keys()) + list(new_global.keys()))):
        old_w = old_weights.get(model, "N/A")
        new_w = final_global.get(model, "N/A")
        marker = " ←" if old_w != new_w else ""
        print(f"    {model}: {old_w} → {new_w}{marker}")
    
    # Save
    output = {
        "weights": final_global,
        "city_weights": blended_city_weights,
        "last_calibrated": datetime.utcnow().isoformat() + "+00:00",
        "calibration_method": "12-month backtest, inverse-MAE, 70% global / 30% per-city",
        "calibration_history": old_weights_data.get("calibration_history", [])[-5:] + [{
            "timestamp": datetime.utcnow().isoformat() + "+00:00",
            "weights": final_global
        }]
    }
    
    with open(f"{WORKDIR}/source_weights.json", "w") as f:
        json.dump(output, f, indent=2)
    
    print("\n  Saved to source_weights.json")
    return old_weights, final_global, blended_city_weights

# ── Step 6: Update analyzer.py ──
def check_analyzer():
    print("\n=== Step 6: Checking analyzer.py ===")
    with open(f"{WORKDIR}/analyzer.py") as f:
        code = f.read()
    
    if "city_weights" in code:
        print("  analyzer.py already references city_weights")
    else:
        print("  analyzer.py needs city_weights support - will patch")
    
    return code

# ── Main ──
if __name__ == "__main__":
    print("Starting full recalibration pipeline...")
    print(f"Timestamp: {datetime.utcnow().isoformat()}Z\n")
    
    # Step 1
    actuals = collect_nws_cli()
    
    # Step 2
    actuals = ensure_openmeteo_actuals(actuals)
    
    # Step 3
    deltas = compute_deltas(actuals)
    
    # Step 4
    overall_mae, city_results = analyze_accuracy(actuals)
    
    # Step 5
    old_weights, new_weights, city_weights = recalibrate(actuals, city_results)
    
    # Step 6
    analyzer_code = check_analyzer()
    
    # Generate report
    print("\n=== Generating Report ===")
    
    report = f"""# Weather Trading Recalibration Report
Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}

## Data Collection Summary

| City | NWS CLI Days | Open-Meteo Days | Overlap Days |
|------|-------------|-----------------|--------------|
"""
    for city in CITIES:
        cli_n = len(actuals["cities"][city].get("data", {}))
        ext_n = len(actuals["cities"][city].get("extended_data", {}))
        overlap = deltas[city]["n_comparisons"]
        report += f"| {city} | {cli_n} | {ext_n} | {overlap} |\n"
    
    report += f"""
**Date range**: 2025-02-20 to 2026-02-19 (12 months)
**Primary actuals source**: Open-Meteo archive API (temperature_2m_max)
**Ground truth validation**: NWS CLI reports (7-8 days per city)

## NWS CLI vs Open-Meteo Delta

This measures how much Open-Meteo archive differs from official NWS CLI reports.

| City | Mean Delta (°F) | Std Dev | MAE | N Days |
|------|----------------|---------|-----|--------|
"""
    for city in CITIES:
        d = deltas[city]
        report += f"| {city} | {d['mean_delta']:+.2f} | {d['std_delta']:.2f} | {d['mae']:.2f} | {d['n_comparisons']} |\n"
    
    report += """
*Positive delta = Open-Meteo reads higher than NWS CLI*

## Source Accuracy (MAE vs Open-Meteo Actuals)

### Overall
| Source | MAE (°F) |
|--------|----------|
"""
    for model, mae in sorted(overall_mae.items(), key=lambda x: x[1]):
        report += f"| {model} | {mae:.2f} |\n"
    
    report += """
### Per-City Best/Worst
| City | Best Source | Best MAE | Worst Source | Worst MAE |
|------|-----------|----------|-------------|-----------|
"""
    for city in CITIES:
        cr = city_results[city]
        best = min(cr.items(), key=lambda x: x[1]["mae"])
        worst = max(cr.items(), key=lambda x: x[1]["mae"])
        report += f"| {city} | {best[0]} | {best[1]['mae']:.2f} | {worst[0]} | {worst[1]['mae']:.2f} |\n"
    
    report += """
## Weight Changes

### Global Weights (Old → New)
| Source | Old Weight | New Weight | Change |
|--------|-----------|------------|--------|
"""
    for model in sorted(set(list(old_weights.keys()) + list(new_weights.keys()))):
        old_w = old_weights.get(model, 0)
        new_w = new_weights.get(model, 0)
        if isinstance(old_w, (int, float)) and isinstance(new_w, (int, float)):
            change = new_w - old_w
            report += f"| {model} | {old_w:.3f} | {new_w:.3f} | {change:+.3f} |\n"
        else:
            report += f"| {model} | {old_w} | {new_w} | - |\n"
    
    report += """
### Per-City Weight Highlights
"""
    for city in CITIES:
        cw = city_weights.get(city, {})
        if cw:
            best = max(cw.items(), key=lambda x: x[1])
            worst = min(cw.items(), key=lambda x: x[1])
            report += f"- **{city}**: highest={best[0]} ({best[1]:.3f}), lowest={worst[0]} ({worst[1]:.3f})\n"
    
    report += """
## Methodology
- **Actuals**: Open-Meteo archive `temperature_2m_max` (validated against NWS CLI where available)
- **Forecasts**: Open-Meteo historical API with models: Best Match, GFS, ECMWF, Ensemble ICON/GFS/ECMWF
- **Weight formula**: Inverse MAE, normalized so average weight = 1.0
- **Blending**: 70% global weights + 30% per-city weights
- **NWS sources** (NWS Forecast, NWS Hourly, Tomorrow.io, Visual Crossing): Retained from previous calibration (no historical forecast data available for backtesting)

## Expected Improvement
With per-city weights, cities where specific models consistently outperform will get better predictions.
The weighted ensemble should reduce overall MAE compared to equal-weight averaging.
"""
    
    with open(f"{WORKDIR}/recalibration_report.md", "w") as f:
        f.write(report)
    
    print("Report saved to recalibration_report.md")
    print("\n✅ Recalibration complete!")
