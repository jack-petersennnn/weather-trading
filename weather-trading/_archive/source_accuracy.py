#!/usr/bin/env python3
"""
Source Accuracy Analyzer
Compares forecast sources against NWS CLI actuals (primary) and Open-Meteo archive actuals (extended).
Two-tier analysis: NWS CLI actuals are authoritative, extended data provides statistical depth.
"""
import json, os, statistics
from datetime import datetime
from collections import defaultdict

ACTUALS_FILE = os.path.join(os.path.dirname(__file__), "nws_cli_actuals.json")
FORECASTS_FILE = os.path.join(os.path.dirname(__file__), "historical_forecasts.json")
REPORT_FILE = os.path.join(os.path.dirname(__file__), "accuracy_report.json")

SEASONS = {"Winter": [12, 1, 2], "Spring": [3, 4, 5], "Summer": [6, 7, 8], "Fall": [9, 10, 11]}

def get_season(date_str):
    month = int(date_str[5:7])
    for s, ms in SEASONS.items():
        if month in ms:
            return s
    return "Unknown"

def get_all_actuals(city_data):
    """Get merged actuals: NWS CLI (authoritative) + extended (Open-Meteo archive).
    Returns (merged_dict, nws_dates_set)."""
    nws = city_data.get("data", {})
    ext = city_data.get("extended_data", {})
    merged = {}
    merged.update(ext)  # extended first
    merged.update(nws)  # NWS overrides
    return merged, set(nws.keys())

def compute_stats(errors):
    if not errors:
        return None
    return {
        "mae": round(statistics.mean(errors), 2),
        "median_ae": round(statistics.median(errors), 2),
        "rmse": round((sum(e**2 for e in errors) / len(errors)) ** 0.5, 2),
        "n": len(errors),
    }

def analyze():
    print("📊 Source Accuracy Analyzer (NWS CLI + Extended)")
    print("=" * 60)
    
    with open(ACTUALS_FILE) as f:
        actuals = json.load(f)
    with open(FORECASTS_FILE) as f:
        forecasts = json.load(f)
    
    # Collect errors
    all_errors = defaultdict(list)
    nws_errors = defaultdict(list)  # NWS CLI only
    city_errors = defaultdict(lambda: defaultdict(list))
    city_nws_errors = defaultdict(lambda: defaultdict(list))
    season_errors = defaultdict(lambda: defaultdict(list))
    bias_data = defaultdict(list)
    city_bias = defaultdict(lambda: defaultdict(list))
    
    for city in actuals.get("cities", {}):
        merged_actuals, nws_dates = get_all_actuals(actuals["cities"][city])
        forecast_data = forecasts.get("cities", {}).get(city, {})
        
        for source_name, source_preds in forecast_data.items():
            for date_str, pred in source_preds.items():
                if date_str not in merged_actuals:
                    continue
                actual = merged_actuals[date_str]
                err = abs(pred - actual)
                signed = pred - actual
                
                all_errors[source_name].append(err)
                city_errors[city][source_name].append(err)
                season_errors[get_season(date_str)][source_name].append(err)
                bias_data[source_name].append(signed)
                city_bias[city][source_name].append(signed)
                
                if date_str in nws_dates:
                    nws_errors[source_name].append(err)
                    city_nws_errors[city][source_name].append(err)
    
    report = {"generated": datetime.utcnow().isoformat() + "Z", 
              "overall": {}, "nws_cli_only": {}, "by_city": {}, "by_season": {}, "bias": {}}
    
    # === Overall (all actuals) ===
    print("\n🎯 OVERALL SOURCE ACCURACY (All Actuals — NWS CLI + Extended)")
    print(f"  {'Source':<20} {'MAE':>6} {'MedAE':>7} {'RMSE':>6} {'Bias':>7} {'N':>5}")
    print(f"  {'─'*55}")
    
    for source in sorted(all_errors.keys(), key=lambda s: statistics.mean(all_errors[s])):
        errs = all_errors[source]
        bias = statistics.mean(bias_data[source])
        stats = compute_stats(errs)
        print(f"  {source:<20} {stats['mae']:>5.2f}° {stats['median_ae']:>6.2f}° {stats['rmse']:>5.2f}° {bias:>+6.2f}° {stats['n']:>5}")
        report["overall"][source] = {**stats, "bias": round(bias, 2)}
    
    # === NWS CLI Only ===
    if nws_errors:
        print(f"\n🏛️ NWS CLI ONLY ({sum(len(v) for v in nws_errors.values())} comparisons)")
        print(f"  {'Source':<20} {'MAE':>6} {'N':>5}")
        print(f"  {'─'*35}")
        for source in sorted(nws_errors.keys(), key=lambda s: statistics.mean(nws_errors[s])):
            errs = nws_errors[source]
            mae = statistics.mean(errs)
            print(f"  {source:<20} {mae:>5.2f}° {len(errs):>5}")
            report["nws_cli_only"][source] = {"mae": round(mae, 2), "n": len(errs)}
    
    # === Per-city ===
    print("\n🏙️ PER-CITY ACCURACY")
    for city in sorted(city_errors.keys()):
        print(f"\n  📍 {city}")
        print(f"    {'Source':<20} {'MAE':>6} {'Bias':>7} {'N':>5}")
        print(f"    {'─'*40}")
        
        city_stats = {}
        for source in sorted(city_errors[city].keys(), key=lambda s: statistics.mean(city_errors[city][s])):
            errs = city_errors[city][source]
            bias = statistics.mean(city_bias[city][source])
            mae = statistics.mean(errs)
            city_stats[source] = {"mae": round(mae, 2), "bias": round(bias, 2), "n": len(errs)}
            nws_mae = ""
            if source in city_nws_errors[city] and city_nws_errors[city][source]:
                nws_mae = f" (NWS: {statistics.mean(city_nws_errors[city][source]):.2f}°)"
            print(f"    {source:<20} {mae:>5.2f}° {bias:>+6.2f}° {len(errs):>5}{nws_mae}")
        
        report["by_city"][city] = city_stats
    
    # === Per-season ===
    print("\n🌤️ PER-SEASON ACCURACY")
    for season in ["Winter", "Spring", "Summer", "Fall"]:
        if season not in season_errors:
            continue
        print(f"\n  {season}")
        print(f"    {'Source':<20} {'MAE':>6} {'N':>5}")
        print(f"    {'─'*35}")
        season_stats = {}
        for source in sorted(season_errors[season].keys(), key=lambda s: statistics.mean(season_errors[season][s])):
            errs = season_errors[season][source]
            mae = statistics.mean(errs)
            season_stats[source] = {"mae": round(mae, 2), "n": len(errs)}
            print(f"    {source:<20} {mae:>5.2f}° {len(errs):>5}")
        report["by_season"][season] = season_stats
    
    # === Rankings ===
    print("\n🏆 OVERALL RANKINGS (lower MAE = better)")
    ranked = sorted(report["overall"].items(), key=lambda x: x[1]["mae"])
    for i, (source, stats) in enumerate(ranked, 1):
        print(f"  {i}. {source:<20} MAE={stats['mae']:.2f}° Bias={stats['bias']:+.2f}°")
    report["rankings"] = [{"rank": i+1, "source": s, **st} for i, (s, st) in enumerate(ranked)]
    
    # === NWS CLI vs Open-Meteo Actual Comparison ===
    print("\n📏 NWS CLI vs Open-Meteo Archive (where both exist)")
    for city in actuals.get("cities", {}):
        nws_data = actuals["cities"][city].get("data", {})
        ext_data = actuals["cities"][city].get("extended_data", {})
        overlap = set(nws_data.keys()) & set(ext_data.keys())
        if overlap:
            diffs = [abs(nws_data[d] - ext_data[d]) for d in overlap]
            mae = statistics.mean(diffs)
            max_diff = max(diffs)
            print(f"  {city}: {len(overlap)} overlapping days, MAE={mae:.2f}°F, Max diff={max_diff:.1f}°F")
            report.setdefault("nws_vs_openmeteo", {})[city] = {
                "overlap_days": len(overlap), "mae": round(mae, 2), "max_diff": round(max_diff, 1)
            }
    
    with open(REPORT_FILE, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\n💾 Report saved to {REPORT_FILE}")
    
    return report

if __name__ == "__main__":
    analyze()
