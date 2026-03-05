#!/usr/bin/env python3
"""Full ACIS-based recalibration: pull actuals, pull archive forecasts, compute MAE, update weights."""

import json, os, sys, time, urllib.request, urllib.error
from datetime import datetime, timezone, timedelta

DIR = os.path.dirname(os.path.abspath(__file__))
SDATE = "2025-02-20"
EDATE = "2026-02-19"

STATIONS = {
    "New York":    {"sid": "KNYC", "lat": 40.7831, "lon": -73.9712, "tz": "America/New_York"},
    "Chicago":     {"sid": "KORD", "lat": 41.8781, "lon": -87.6298, "tz": "America/Chicago"},
    "Miami":       {"sid": "KMIA", "lat": 25.7617, "lon": -80.1918, "tz": "America/New_York"},
    "Denver":      {"sid": "KDEN", "lat": 39.7392, "lon": -104.9903, "tz": "America/Denver"},
    "Los Angeles": {"sid": "KLAX", "lat": 34.0522, "lon": -118.2437, "tz": "America/Los_Angeles"},
    "Austin":      {"sid": "KAUS", "lat": 30.2672, "lon": -97.7431, "tz": "America/Chicago"},
}

def post_json(url, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())

def fetch_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "weather-recal/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())

# ── Step 1: ACIS Actuals ──────────────────────────────────────
def pull_acis():
    print("═══ Step 1: Pulling ACIS actuals ═══")
    result = {"source": "RCC-ACIS", "pulled_at": datetime.now(timezone.utc).isoformat(), "cities": {}}
    for city, info in STATIONS.items():
        print(f"  {city} ({info['sid']})...", end=" ")
        body = {"sid": info["sid"], "sdate": SDATE, "edate": EDATE, "elems": [{"name": "maxt"}]}
        resp = post_json("https://data.rcc-acis.org/StnData", body)
        data = {}
        for row in resp.get("data", []):
            date_str, val = row[0], row[1]
            if val not in ("M", "T", "S") and val != "":
                try:
                    data[date_str] = int(round(float(val)))
                except (ValueError, TypeError):
                    pass
        result["cities"][city] = {"station": info["sid"], "data": data}
        print(f"{len(data)} days")
    
    path = os.path.join(DIR, "acis_actuals.json")
    with open(path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"  Saved to acis_actuals.json\n")
    return result

# ── Step 2: Open-Meteo Archive Forecasts ──────────────────────
def pull_forecasts():
    print("═══ Step 2: Pulling Open-Meteo archive forecasts ═══")
    
    models = {
        "Best Match": {"base": "https://archive-api.open-meteo.com/v1/archive", "params": ""},
        "ECMWF":      {"base": "https://archive-api.open-meteo.com/v1/archive", "params": "&models=ecmwf_ifs025"},
        "GFS":        {"base": "https://archive-api.open-meteo.com/v1/archive", "params": "&models=gfs_seamless"},
        "ICON":       {"base": "https://archive-api.open-meteo.com/v1/archive", "params": "&models=icon_seamless"},
        "Ensemble ECMWF": {"base": "https://ensemble-api.open-meteo.com/v1/ensemble", "params": "&models=ecmwf_ifs025"},
        "Ensemble GFS":   {"base": "https://ensemble-api.open-meteo.com/v1/ensemble", "params": "&models=gfs_seamless"},
        "Ensemble ICON":  {"base": "https://ensemble-api.open-meteo.com/v1/ensemble", "params": "&models=icon_seamless"},
    }
    
    result = {"sources": {}, "pulled_at": datetime.now(timezone.utc).isoformat()}
    
    for model_name, model_cfg in models.items():
        result["sources"][model_name] = {}
        for city, info in STATIONS.items():
            tz_enc = info["tz"].replace("/", "%2F")
            url = (f"{model_cfg['base']}?latitude={info['lat']}&longitude={info['lon']}"
                   f"&start_date={SDATE}&end_date={EDATE}"
                   f"&daily=temperature_2m_max&temperature_unit=fahrenheit"
                   f"&timezone={tz_enc}{model_cfg['params']}")
            print(f"  {model_name} / {city}...", end=" ")
            try:
                data = fetch_json(url)
                dates = data.get("daily", {}).get("time", [])
                temps = data.get("daily", {}).get("temperature_2m_max", [])
                city_data = {}
                for d, t in zip(dates, temps):
                    if t is not None:
                        city_data[d] = round(t, 1)
                result["sources"][model_name][city] = city_data
                print(f"{len(city_data)} days")
            except Exception as e:
                print(f"FAILED ({e})")
                result["sources"][model_name][city] = {}
            time.sleep(0.3)
    
    path = os.path.join(DIR, "historical_forecasts.json")
    with open(path, "w") as f:
        json.dump(result, f)
    print(f"  Saved to historical_forecasts.json\n")
    return result

# ── Step 3-5: MAE, Rankings, Weights ──────────────────────────
def compute_accuracy(acis, forecasts):
    print("═══ Step 3: Computing MAE ═══")
    results = {}  # city -> source -> {mae, n, errors}
    
    for city in STATIONS:
        actuals = acis["cities"][city]["data"]
        results[city] = {}
        for source_name, source_cities in forecasts["sources"].items():
            city_forecasts = source_cities.get(city, {})
            errors = []
            for date_str, actual in actuals.items():
                if date_str in city_forecasts:
                    forecast = city_forecasts[date_str]
                    errors.append(abs(actual - forecast))
            if errors:
                mae = sum(errors) / len(errors)
                results[city][source_name] = {"mae": round(mae, 2), "n": len(errors), "max_error": round(max(errors), 1)}
                print(f"  {city:15s} | {source_name:20s} | MAE: {mae:5.2f}°F | n={len(errors)}")
    
    # Also compute last-30-day MAE
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    rolling = {}
    for city in STATIONS:
        actuals = acis["cities"][city]["data"]
        rolling[city] = {}
        for source_name, source_cities in forecasts["sources"].items():
            city_forecasts = source_cities.get(city, {})
            errors = []
            for date_str, actual in actuals.items():
                if date_str >= cutoff and date_str in city_forecasts:
                    errors.append(abs(actual - city_forecasts[date_str]))
            if errors:
                rolling[city][source_name] = {"mae": round(sum(errors)/len(errors), 2), "n": len(errors)}
    
    accuracy = {"long_term": results, "rolling_30d": rolling}
    path = os.path.join(DIR, "source_accuracy_acis.json")
    with open(path, "w") as f:
        json.dump(accuracy, f, indent=2)
    print(f"\n  Saved to source_accuracy_acis.json\n")
    return accuracy

def compute_weights(accuracy):
    print("═══ Step 4-5: Rankings & Weights ═══")
    long_term = accuracy["long_term"]
    rolling = accuracy["rolling_30d"]
    
    # Print rankings
    print("\n  Best/Worst source per city:")
    for city in STATIONS:
        if city not in long_term or not long_term[city]:
            continue
        ranked = sorted(long_term[city].items(), key=lambda x: x[1]["mae"])
        best = ranked[0]
        worst = ranked[-1]
        print(f"    {city:15s} | Best: {best[0]:20s} ({best[1]['mae']:.2f}°F) | Worst: {worst[0]:20s} ({worst[1]['mae']:.2f}°F)")
    
    # Compute per-city weights: 70% long-term + 30% rolling, using 1/MAE
    all_sources = set()
    for city_data in long_term.values():
        all_sources.update(city_data.keys())
    
    city_weights = {}
    for city in STATIONS:
        cw = {}
        for source in all_sources:
            lt_mae = long_term.get(city, {}).get(source, {}).get("mae")
            rl_mae = rolling.get(city, {}).get(source, {}).get("mae")
            if lt_mae and lt_mae > 0:
                if rl_mae and rl_mae > 0:
                    blended_mae = 0.7 * lt_mae + 0.3 * rl_mae
                else:
                    blended_mae = lt_mae
                cw[source] = 1.0 / blended_mae
        # Normalize so average = 1.0
        if cw:
            avg = sum(cw.values()) / len(cw)
            cw = {s: round(v / avg, 3) for s, v in cw.items()}
        city_weights[city] = cw
    
    # Global weights: average across cities
    global_weights = {}
    for source in all_sources:
        vals = [city_weights[c].get(source) for c in city_weights if source in city_weights[c]]
        if vals:
            global_weights[source] = round(sum(vals) / len(vals), 3)
    
    # Add non-backtestable sources with default weight 1.0
    for src in ["NWS Forecast", "NWS Hourly", "Tomorrow.io", "Visual Crossing"]:
        if src not in global_weights:
            global_weights[src] = 1.0
            for city in city_weights:
                if src not in city_weights[city]:
                    city_weights[city][src] = 1.0
    
    # Count long-term days
    lt_days = 0
    for city_data in long_term.values():
        for src_data in city_data.values():
            lt_days = max(lt_days, src_data.get("n", 0))
    
    weights_data = {
        "weights": global_weights,
        "city_weights": city_weights,
        "last_calibrated": datetime.now(timezone.utc).isoformat(),
        "method": "acis_backtest_70_30",
        "stats": {"long_term_days": lt_days, "rolling_days": 30, "blend": "70/30"}
    }
    
    path = os.path.join(DIR, "source_weights.json")
    with open(path, "w") as f:
        json.dump(weights_data, f, indent=2)
    print(f"\n  Global weights: {json.dumps(global_weights, indent=4)}")
    print(f"  Saved to source_weights.json\n")
    return weights_data

# ── Step 8: Report ────────────────────────────────────────────
def write_report(acis, accuracy, weights):
    print("═══ Step 8: Writing report ═══")
    
    # Load old weights for comparison
    old_weights = {
        "NWS Forecast": 0.925, "NWS Hourly": 0.902, "ECMWF": 1.45,
        "GFS": 0.674, "Best Match": 1.086, "Ensemble ICON": 0.752,
        "Ensemble GFS": 0.674, "Ensemble ECMWF": 1.45,
        "Tomorrow.io": 1.066, "Visual Crossing": 0.885
    }
    
    lines = ["# Recalibration Report — ACIS Ground Truth\n"]
    lines.append(f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")
    lines.append(f"**Date Range:** {SDATE} to {EDATE}\n")
    lines.append(f"**Method:** 70% long-term / 30% rolling-30d MAE, inverse-MAE weights, ACIS actuals\n\n")
    
    # Data coverage
    lines.append("## Data Coverage\n\n| City | ACIS Days | Station |\n|---|---|---|\n")
    for city in STATIONS:
        n = len(acis["cities"][city]["data"])
        lines.append(f"| {city} | {n} | {STATIONS[city]['sid']} |\n")
    
    # Cross-check
    lines.append("\n## ACIS Cross-Check (NYC Feb 12-18)\n\n")
    expected = {"2025-02-12": 36, "2025-02-13": 38, "2025-02-14": 46, "2025-02-15": 40,
                "2025-02-16": 39, "2025-02-17": 47, "2025-02-18": 41}
    nyc_data = acis["cities"]["New York"]["data"]
    all_match = True
    for d, exp in expected.items():
        got = nyc_data.get(d, "N/A")
        match = "✅" if got == exp else "❌"
        if got != exp:
            all_match = False
        lines.append(f"- {d}: expected {exp}, got {got} {match}\n")
    if all_match:
        lines.append("\n**All cross-check values match!** ✅\n")
    
    # Per-city MAE table
    long_term = accuracy["long_term"]
    lines.append("\n## Per-City MAE (°F) — Long Term\n\n")
    sources = sorted(set(s for cd in long_term.values() for s in cd))
    header = "| City | " + " | ".join(sources) + " |\n"
    sep = "|---|" + "|".join(["---"]*len(sources)) + "|\n"
    lines.append(header)
    lines.append(sep)
    for city in STATIONS:
        row = f"| {city} |"
        for src in sources:
            mae = long_term.get(city, {}).get(src, {}).get("mae", "-")
            row += f" {mae} |"
        lines.append(row + "\n")
    
    # Best/worst per city
    lines.append("\n## Best & Worst Source per City\n\n| City | Best Source | MAE | Worst Source | MAE |\n|---|---|---|---|---|\n")
    for city in STATIONS:
        if city not in long_term or not long_term[city]:
            continue
        ranked = sorted(long_term[city].items(), key=lambda x: x[1]["mae"])
        b, w = ranked[0], ranked[-1]
        lines.append(f"| {city} | {b[0]} | {b[1]['mae']} | {w[0]} | {w[1]['mae']} |\n")
    
    # Old vs new weights
    new_w = weights["weights"]
    lines.append("\n## Weight Changes (Old → New)\n\n| Source | Old | New | Change |\n|---|---|---|---|\n")
    all_srcs = sorted(set(list(old_weights.keys()) + list(new_w.keys())))
    for src in all_srcs:
        o = old_weights.get(src, "-")
        n = new_w.get(src, "-")
        if isinstance(o, (int, float)) and isinstance(n, (int, float)):
            delta = n - o
            lines.append(f"| {src} | {o:.3f} | {n:.3f} | {delta:+.3f} |\n")
        else:
            lines.append(f"| {src} | {o} | {n} | new |\n")
    
    # Denver analysis
    lines.append("\n## Denver-Specific Analysis\n\n")
    if "Denver" in long_term:
        ranked = sorted(long_term["Denver"].items(), key=lambda x: x[1]["mae"])
        lines.append("Denver source ranking by MAE:\n\n")
        for i, (src, info) in enumerate(ranked, 1):
            lines.append(f"{i}. **{src}**: {info['mae']}°F MAE (n={info['n']})\n")
        lines.append(f"\nDenver city weights: {json.dumps(weights['city_weights'].get('Denver', {}), indent=2)}\n")
    
    # Non-backtested sources
    lines.append("\n## Sources Not Backtested\n\n")
    lines.append("- **NWS Forecast / NWS Hourly**: Real-time only, no historical archive. Weight kept at 1.0.\n")
    lines.append("- **Tomorrow.io / Visual Crossing**: Require paid API keys for historical data. Weight kept at 1.0.\n")
    
    path = os.path.join(DIR, "recalibration_report.md")
    with open(path, "w") as f:
        f.writelines(lines)
    print(f"  Saved to recalibration_report.md\n")

# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    acis = pull_acis()
    forecasts = pull_forecasts()
    accuracy = compute_accuracy(acis, forecasts)
    weights = compute_weights(accuracy)
    write_report(acis, accuracy, weights)
    print("═══ DONE ═══")
