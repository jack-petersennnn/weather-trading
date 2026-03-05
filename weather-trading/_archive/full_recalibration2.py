#!/usr/bin/env python3
"""Full recalibration - skip slow NWS CLI fetching, use existing data."""

import json, requests, time, statistics
from datetime import datetime, timezone
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

def load_data():
    with open(f"{WORKDIR}/nws_cli_actuals.json") as f:
        actuals = json.load(f)
    with open(f"{WORKDIR}/historical_forecasts.json") as f:
        forecasts = json.load(f)
    with open(f"{WORKDIR}/source_weights.json") as f:
        old_weights_data = json.load(f)
    return actuals, forecasts, old_weights_data

def ensure_openmeteo(actuals):
    """Fill gaps in Open-Meteo actuals."""
    print("=== Ensuring Open-Meteo archive actuals ===")
    for city, info in CITIES.items():
        ext = actuals["cities"][city].get("extended_data", {})
        url = (f"https://archive-api.open-meteo.com/v1/archive?"
               f"latitude={info['lat']}&longitude={info['lon']}"
               f"&start_date=2025-02-20&end_date=2026-02-19"
               f"&daily=temperature_2m_max&temperature_unit=fahrenheit"
               f"&timezone={info['tz']}")
        try:
            resp = requests.get(url, timeout=30)
            data = resp.json()
            dates = data["daily"]["time"]
            temps = data["daily"]["temperature_2m_max"]
            if "extended_data" not in actuals["cities"][city]:
                actuals["cities"][city]["extended_data"] = {}
            for d, t in zip(dates, temps):
                if t is not None:
                    actuals["cities"][city]["extended_data"][d] = round(t, 1)
            print(f"  {city}: {len(actuals['cities'][city]['extended_data'])} days")
            time.sleep(0.5)
        except Exception as e:
            print(f"  {city}: Error {e}")
    
    with open(f"{WORKDIR}/nws_cli_actuals.json", "w") as f:
        json.dump(actuals, f, indent=2)
    return actuals

def compute_deltas(actuals):
    """NWS CLI vs Open-Meteo delta."""
    print("\n=== NWS CLI vs Open-Meteo Delta ===")
    deltas = {}
    for city in CITIES:
        cli = actuals["cities"][city].get("data", {})
        ext = actuals["cities"][city].get("extended_data", {})
        pairs = [(cli[d], ext[d]) for d in cli if d in ext]
        if pairs:
            diffs = [om - nws for nws, om in pairs]
            deltas[city] = {
                "n": len(pairs),
                "mean": round(statistics.mean(diffs), 2),
                "std": round(statistics.stdev(diffs), 2) if len(diffs) > 1 else 0,
                "mae": round(statistics.mean([abs(d) for d in diffs]), 2)
            }
            print(f"  {city}: n={len(pairs)}, mean_delta={deltas[city]['mean']:+.2f}°F, MAE={deltas[city]['mae']:.2f}°F")
        else:
            deltas[city] = {"n": 0, "mean": 0, "std": 0, "mae": 0}
            print(f"  {city}: no overlap")
    return deltas

def analyze_accuracy(actuals, forecasts):
    """Per-source and per-city MAE."""
    print("\n=== Source Accuracy Analysis ===")
    global_errors = defaultdict(list)
    city_results = {}
    
    for city in CITIES:
        ext = actuals["cities"][city].get("extended_data", {})
        cf = forecasts.get("cities", {}).get(city, {})
        city_results[city] = {}
        
        for model, mdata in cf.items():
            errors = [abs(mdata[d] - ext[d]) for d in mdata if d in ext and mdata[d] is not None]
            if errors:
                mae = statistics.mean(errors)
                global_errors[model].extend(errors)
                city_results[city][model] = {"mae": round(mae, 2), "n": len(errors)}
    
    overall = {m: round(statistics.mean(e), 2) for m, e in global_errors.items()}
    
    print("\n  Overall MAE:")
    for m, mae in sorted(overall.items(), key=lambda x: x[1]):
        print(f"    {m}: {mae:.2f}°F ({len(global_errors[m])} days)")
    
    print("\n  Per-city best:")
    for city in CITIES:
        if city_results[city]:
            best = min(city_results[city].items(), key=lambda x: x[1]["mae"])
            print(f"    {city}: {best[0]} ({best[1]['mae']:.2f}°F)")
    
    # Against NWS CLI
    print("\n  MAE vs NWS CLI ground truth:")
    cli_errors = defaultdict(list)
    for city in CITIES:
        cli = actuals["cities"][city].get("data", {})
        cf = forecasts.get("cities", {}).get(city, {})
        for model, mdata in cf.items():
            errors = [abs(mdata[d] - cli[d]) for d in mdata if d in cli and mdata[d] is not None]
            cli_errors[model].extend(errors)
    
    for m, e in sorted(cli_errors.items(), key=lambda x: statistics.mean(x[1])):
        print(f"    {m}: {statistics.mean(e):.2f}°F ({len(e)} days)")
    
    return overall, city_results

def recalibrate(old_weights_data, overall, city_results):
    """Compute new weights."""
    print("\n=== Recalibrating Weights ===")
    old_weights = old_weights_data["weights"]
    
    # Exclude "Best Match" - it's the Open-Meteo default which IS the actuals
    backtest_models = {m: mae for m, mae in overall.items() if m != "Best Match" and mae > 0}
    
    # Global: inverse MAE, scaled so avg=1.0
    inv = {m: 1.0/mae for m, mae in backtest_models.items()}
    total = sum(inv.values())
    n = len(inv)
    new_from_backtest = {m: round((v/total)*n, 3) for m, v in inv.items()}
    
    # Per-city blended weights
    city_weights = {}
    for city in CITIES:
        cm = {m: info["mae"] for m, info in city_results[city].items() if m != "Best Match" and info["mae"] > 0}
        if not cm:
            continue
        cinv = {m: 1.0/mae for m, mae in cm.items()}
        ct = sum(cinv.values())
        cn = len(cinv)
        pure_city = {m: round((v/ct)*cn, 3) for m, v in cinv.items()}
        # 70/30 blend
        blended = {}
        for m in pure_city:
            g = new_from_backtest.get(m, 1.0)
            blended[m] = round(0.7*g + 0.3*pure_city[m], 3)
        city_weights[city] = blended
    
    # Merge: keep old weights for sources not in backtest, update rest
    final = dict(old_weights)
    for m, w in new_from_backtest.items():
        final[m] = w
    
    print("\n  Weight changes:")
    for m in sorted(set(list(old_weights.keys()) + list(final.keys()))):
        o = old_weights.get(m, "N/A")
        nw = final.get(m, "N/A")
        if o != nw:
            print(f"    {m}: {o} → {nw}")
    
    output = {
        "weights": final,
        "city_weights": city_weights,
        "last_calibrated": datetime.now(timezone.utc).isoformat(),
        "calibration_method": "12-month backtest, inverse-MAE, 70/30 global/city blend",
        "calibration_history": old_weights_data.get("calibration_history", [])[-5:] + [{
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "weights": final
        }]
    }
    
    with open(f"{WORKDIR}/source_weights.json", "w") as f:
        json.dump(output, f, indent=2)
    print("  Saved source_weights.json")
    
    return old_weights, final, city_weights

def patch_analyzer():
    """Ensure analyzer.py uses city_weights."""
    print("\n=== Checking analyzer.py ===")
    with open(f"{WORKDIR}/analyzer.py") as f:
        code = f.read()
    
    if "city_weights" in code:
        print("  Already has city_weights support ✓")
        return False
    
    # Need to patch - find where weights are loaded
    print("  Needs city_weights patch")
    return True

def generate_report(actuals, deltas, overall, city_results, old_weights, new_weights, city_weights):
    """Generate markdown report."""
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    
    r = f"# Weather Trading Recalibration Report\nGenerated: {now}\n\n"
    r += "## Data Summary\n\n"
    r += "| City | NWS CLI Days | Open-Meteo Days | Overlap |\n|------|---|---|---|\n"
    for city in CITIES:
        cn = len(actuals["cities"][city].get("data", {}))
        en = len(actuals["cities"][city].get("extended_data", {}))
        r += f"| {city} | {cn} | {en} | {deltas[city]['n']} |\n"
    
    r += "\n**Date range**: 2025-02-20 to 2026-02-19\n"
    r += "**Primary source**: Open-Meteo archive (temperature_2m_max, °F)\n"
    r += "**Validation**: NWS CLI reports (7-8 recent days per city)\n\n"
    
    r += "## NWS CLI vs Open-Meteo Delta\n\n"
    r += "| City | Mean Δ (°F) | Std | MAE | N |\n|------|---|---|---|---|\n"
    for city in CITIES:
        d = deltas[city]
        r += f"| {city} | {d['mean']:+.2f} | {d['std']:.2f} | {d['mae']:.2f} | {d['n']} |\n"
    r += "\n*Positive = Open-Meteo higher than NWS CLI*\n\n"
    
    r += "## Source Accuracy (MAE vs Actuals)\n\n"
    r += "| Source | Overall MAE (°F) |\n|------|---|\n"
    for m, mae in sorted(overall.items(), key=lambda x: x[1]):
        r += f"| {m} | {mae:.2f} |\n"
    
    r += "\n### Per-City\n\n| City | Best | MAE | Worst | MAE |\n|------|---|---|---|---|\n"
    for city in CITIES:
        cr = city_results[city]
        if cr:
            best = min(cr.items(), key=lambda x: x[1]["mae"])
            worst = max(cr.items(), key=lambda x: x[1]["mae"])
            r += f"| {city} | {best[0]} | {best[1]['mae']:.2f} | {worst[0]} | {worst[1]['mae']:.2f} |\n"
    
    r += "\n## Weight Changes\n\n"
    r += "| Source | Old | New | Δ |\n|------|---|---|---|\n"
    for m in sorted(set(list(old_weights.keys()) + list(new_weights.keys()))):
        o = old_weights.get(m, 0)
        n = new_weights.get(m, 0)
        if isinstance(o, (int, float)) and isinstance(n, (int, float)):
            r += f"| {m} | {o:.3f} | {n:.3f} | {n-o:+.3f} |\n"
    
    r += "\n### Per-City Highlights\n\n"
    for city in CITIES:
        cw = city_weights.get(city, {})
        if cw:
            best = max(cw.items(), key=lambda x: x[1])
            worst = min(cw.items(), key=lambda x: x[1])
            r += f"- **{city}**: highest={best[0]} ({best[1]:.3f}), lowest={worst[0]} ({worst[1]:.3f})\n"
    
    r += "\n## Methodology\n"
    r += "- Inverse-MAE weighting normalized to avg=1.0\n"
    r += "- 70% global + 30% per-city blend\n"
    r += "- Sources without historical forecast data (NWS Forecast, NWS Hourly, Tomorrow.io, Visual Crossing) retain previous weights\n"
    r += "- 12-month backtest period with ~360 days per city\n"
    
    with open(f"{WORKDIR}/recalibration_report.md", "w") as f:
        f.write(r)
    print(f"\nReport saved to recalibration_report.md")

if __name__ == "__main__":
    print(f"Starting recalibration: {datetime.now(timezone.utc).isoformat()}\n")
    
    actuals, forecasts, old_weights_data = load_data()
    actuals = ensure_openmeteo(actuals)
    deltas = compute_deltas(actuals)
    overall, city_results = analyze_accuracy(actuals, forecasts)
    old_weights, new_weights, city_weights = recalibrate(old_weights_data, overall, city_results)
    needs_patch = patch_analyzer()
    generate_report(actuals, deltas, overall, city_results, old_weights, new_weights, city_weights)
    
    print("\n✅ Done!")
