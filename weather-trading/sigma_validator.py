#!/usr/bin/env python3
"""
Sigma Validator — Computes LIVE 1σ coverage from training_forecast_log + ACIS actuals.

This is the REAL sigma validation for rescue mode gate decisions.
Replaces the stale sigma_optimization_results.json (which was from historical backtest).

Run: python3 sigma_validator.py [--save] [--json]
"""

import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def load_json(filename):
    path = os.path.join(BASE_DIR, filename)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def compute_sigma_coverage():
    """
    Match forecasts (training_forecast_log) to actuals (nws_gridpoint_log)
    and compute per-city 1σ coverage.
    
    Returns dict with global and per-city stats.
    """
    forecast_log = load_json("training_forecast_log.json")
    nws_data = load_json("nws_gridpoint_log.json")
    actuals = nws_data.get("actuals", {})

    if not forecast_log or not actuals:
        return {"error": "Missing forecast or actual data"}

    city_stats = defaultdict(lambda: {
        "total": 0, "within_1sigma": 0, "within_2sigma": 0,
        "errors": [], "stds": [], "dates": []
    })

    for key, entry in forecast_log.items():
        city = entry["city"]
        date = entry["target_date"]
        mean = entry["ensemble_mean"]
        std = entry["ensemble_std"]

        # Find matching actual
        actual_key = f"{city}_{date}"
        if actual_key not in actuals:
            continue

        actual = actuals[actual_key]
        error = actual - mean
        abs_error = abs(error)

        city_stats[city]["total"] += 1
        city_stats[city]["errors"].append(error)
        city_stats[city]["stds"].append(std)
        city_stats[city]["dates"].append(date)

        if abs_error <= std:
            city_stats[city]["within_1sigma"] += 1
        if abs_error <= 2 * std:
            city_stats[city]["within_2sigma"] += 1

    # Compute per-city results
    results = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_source": "LIVE (training_forecast_log + nws_gridpoint_log actuals)",
        "per_city": {},
        "global": {}
    }

    total_all = 0
    within_1sigma_all = 0
    within_2sigma_all = 0

    for city in sorted(city_stats.keys()):
        s = city_stats[city]
        n = s["total"]
        if n == 0:
            continue

        total_all += n
        within_1sigma_all += s["within_1sigma"]
        within_2sigma_all += s["within_2sigma"]

        coverage_1sigma = s["within_1sigma"] / n
        coverage_2sigma = s["within_2sigma"] / n
        mae = sum(abs(e) for e in s["errors"]) / n
        bias = sum(s["errors"]) / n
        avg_std = sum(s["stds"]) / n

        # Assess quality
        if n < 5:
            quality = "insufficient_data"
        elif 0.55 <= coverage_1sigma <= 0.82:
            quality = "good"
        elif coverage_1sigma < 0.55:
            quality = "sigma_too_tight"
        else:
            quality = "sigma_too_wide"

        results["per_city"][city] = {
            "samples": n,
            "coverage_1sigma": round(coverage_1sigma, 4),
            "coverage_1sigma_pct": round(coverage_1sigma * 100, 1),
            "coverage_2sigma": round(coverage_2sigma, 4),
            "coverage_2sigma_pct": round(coverage_2sigma * 100, 1),
            "mae": round(mae, 2),
            "bias": round(bias, 2),
            "avg_std": round(avg_std, 2),
            "quality": quality,
            "date_range": f"{min(s['dates'])} to {max(s['dates'])}",
        }

    if total_all > 0:
        results["global"] = {
            "total_samples": total_all,
            "coverage_1sigma": round(within_1sigma_all / total_all, 4),
            "coverage_1sigma_pct": round(within_1sigma_all / total_all * 100, 1),
            "coverage_2sigma": round(within_2sigma_all / total_all, 4),
            "coverage_2sigma_pct": round(within_2sigma_all / total_all * 100, 1),
            "target_1sigma": 68.3,
            "target_2sigma": 95.4,
        }

    # Gate assessment
    unique_dates = set()
    for city_data in city_stats.values():
        unique_dates.update(city_data["dates"])
    
    clean_days = len(unique_dates)
    
    # Count cities with good coverage (55-82% 1σ and at least 5 samples)
    good_cities = [
        city for city, data in results["per_city"].items()
        if data["quality"] == "good"
    ]
    
    results["gate_progress"] = {
        "clean_days": clean_days,
        "clean_days_target": 30,
        "clean_days_met": clean_days >= 30,
        "good_coverage_cities": good_cities,
        "good_coverage_count": len(good_cities),
        "good_coverage_target": 3,
        "good_coverage_met": len(good_cities) >= 3,
        "date_range": f"{min(unique_dates)} to {max(unique_dates)}" if unique_dates else "N/A",
    }

    return results


def print_report(results):
    """Print a human-readable sigma validation report."""
    if "error" in results:
        print(f"ERROR: {results['error']}")
        return

    g = results["global"]
    print(f"{'='*65}")
    print(f" LIVE SIGMA VALIDATION REPORT")
    print(f" Generated: {results['generated_at'][:19]} UTC")
    print(f" Source: {results['data_source']}")
    print(f"{'='*65}")
    print()
    print(f" Global: {g['coverage_1sigma_pct']}% within ±1σ (target 68.3%)")
    print(f"         {g['coverage_2sigma_pct']}% within ±2σ (target 95.4%)")
    print(f"         {g['total_samples']} forecast/actual pairs")
    print()

    print(f" {'City':20s} {'N':>3s} {'1σ%':>6s} {'2σ%':>6s} {'MAE':>5s} {'Bias':>6s} {'AvgStd':>6s} {'Quality':>18s}")
    print(f" {'-'*70}")
    
    for city, data in sorted(results["per_city"].items()):
        q_icon = {
            "good": "✅",
            "sigma_too_tight": "⚠️ TIGHT",
            "sigma_too_wide": "🔴 WIDE",
            "insufficient_data": "📊 LOW N",
        }.get(data["quality"], "?")
        
        print(f" {city:20s} {data['samples']:3d} {data['coverage_1sigma_pct']:5.1f}% "
              f"{data['coverage_2sigma_pct']:5.1f}% {data['mae']:5.1f} {data['bias']:+5.1f} "
              f"{data['avg_std']:6.1f} {q_icon:>18s}")

    print()
    gate = results["gate_progress"]
    print(f" {'='*65}")
    print(f" GATE PROGRESS (Graduated 2σ Buffer)")
    print(f" {'='*65}")
    d_icon = "✅" if gate["clean_days_met"] else "❌"
    c_icon = "✅" if gate["good_coverage_met"] else "❌"
    print(f" {d_icon} Clean data days: {gate['clean_days']}/{gate['clean_days_target']}")
    print(f"   Date range: {gate['date_range']}")
    print(f" {c_icon} Cities with good 1σ coverage (55-82%): {gate['good_coverage_count']}/{gate['good_coverage_target']}")
    if gate["good_coverage_cities"]:
        print(f"   Good: {', '.join(gate['good_coverage_cities'])}")
    print(f" ❌ Isotonic calibration: {'ready to build' if gate['clean_days_met'] else 'waiting for data'}")
    print(f" ❌ Trade-level counterfactual: not yet run")
    print()


if __name__ == "__main__":
    results = compute_sigma_coverage()
    
    if "--json" in sys.argv:
        print(json.dumps(results, indent=2))
    else:
        print_report(results)
    
    if "--save" in sys.argv:
        path = os.path.join(BASE_DIR, "sigma_validation_live.json")
        with open(path, "w") as f:
            json.dump(results, f, indent=2)
        print(f"💾 Saved to {path}")
