#!/usr/bin/env python3
"""
Training Logger — Captures ALL forecasts from ALL sources (active + training)
for every city on every scan run.

This is critical for:
1. Building accuracy data for new models (HRRR, MET Norway, NWS, etc.)
2. Continuously refining weights for all models
3. Comparing model predictions against ACIS actuals over time

Data stored in training_forecast_log.json, keyed by city|date.
Each entry stores the latest forecast per source with timestamps.
"""

import json
import os
import fcntl
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "training_forecast_log.json")

# Keep ALL data for training sources — never prune, we need complete accuracy picture
# File will grow ~50KB/month (19 cities × 30 days × ~90 bytes/entry) — negligible
MAX_DAYS = None  # No pruning


def log_forecasts(city, target_date, active_forecasts, all_forecasts, ensemble_mean=None, ensemble_std=None):
    """Log all forecasts (active + training) for a city/date combination.
    
    Called by fast_scanner after collecting forecasts for each city.
    """
    now = datetime.now(timezone.utc)
    key = f"{city}|{target_date}"
    
    # Separate training sources
    training_forecasts = {k: v for k, v in all_forecasts.items() if k not in active_forecasts}
    
    entry = {
        "city": city,
        "target_date": str(target_date),
        "last_updated": now.isoformat(),
        "active_forecasts": active_forecasts,
        "training_forecasts": training_forecasts,
        "all_forecasts": all_forecasts,
        "ensemble_mean": ensemble_mean,
        "ensemble_std": ensemble_std,
        "source_count": len(all_forecasts),
        "active_count": len(active_forecasts),
        "training_count": len(training_forecasts),
    }
    
    # Load existing log
    log = {}
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE) as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                log = json.load(f)
                fcntl.flock(f, fcntl.LOCK_UN)
        except:
            log = {}
    
    # Update entry for this city/date
    log[key] = entry
    
    # No pruning — keep all data for complete accuracy tracking
    pruned = log
    
    # Save
    with open(LOG_FILE, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        json.dump(pruned, f, indent=2)
        f.flush()
        fcntl.flock(f, fcntl.LOCK_UN)


def get_forecast_log():
    """Load the full forecast log for accuracy analysis."""
    if not os.path.exists(LOG_FILE):
        return {}
    with open(LOG_FILE) as f:
        return json.load(f)


def compute_accuracy_report(acis_actuals=None):
    """Compare logged forecasts against ACIS actuals.
    
    acis_actuals: dict of {city: {date: actual_high_temp}}
    If not provided, tries to load from acis_actuals_all_cities.json.
    """
    if acis_actuals is None:
        acis_file = os.path.join(BASE_DIR, "acis_actuals_all_cities.json")
        if os.path.exists(acis_file):
            with open(acis_file) as f:
                raw = json.load(f)
            acis_actuals = {city: info.get("data", {}) for city, info in raw.get("cities", {}).items()}
        else:
            print("No ACIS actuals file found")
            return None
    
    log = get_forecast_log()
    
    # Collect errors per source
    source_errors = {}  # source_name -> [(predicted, actual, city, date), ...]
    
    for key, entry in log.items():
        city = entry.get("city", "")
        target_date = entry.get("target_date", "")
        
        actual = acis_actuals.get(city, {}).get(target_date)
        if actual is None:
            continue
        
        for source, predicted in entry.get("all_forecasts", {}).items():
            if source not in source_errors:
                source_errors[source] = []
            source_errors[source].append((predicted, float(actual), city, target_date))
    
    # Compute stats per source
    report = {}
    for source, errors in source_errors.items():
        abs_errors = [abs(p - a) for p, a, _, _ in errors]
        signed_errors = [p - a for p, a, _, _ in errors]
        
        mae = sum(abs_errors) / len(abs_errors)
        bias = sum(signed_errors) / len(signed_errors)
        max_err = max(abs_errors)
        within_1 = sum(1 for e in abs_errors if e <= 1.0) / len(abs_errors) * 100
        within_2 = sum(1 for e in abs_errors if e <= 2.0) / len(abs_errors) * 100
        
        report[source] = {
            "mae": round(mae, 2),
            "bias": round(bias, 2),
            "max_error": round(max_err, 1),
            "n": len(errors),
            "within_1f": round(within_1, 1),
            "within_2f": round(within_2, 1),
        }
    
    return report


if __name__ == "__main__":
    report = compute_accuracy_report()
    if report:
        print(f"\n{'Source':<25} {'MAE':>6} {'Bias':>6} {'MaxErr':>7} {'N':>5} {'<1°F':>6} {'<2°F':>6}")
        print(f"{'-'*25} {'-'*6} {'-'*6} {'-'*7} {'-'*5} {'-'*6} {'-'*6}")
        for source, stats in sorted(report.items(), key=lambda x: x[1]['mae']):
            print(f"{source:<25} {stats['mae']:>5.2f}° {stats['bias']:>+5.2f}° {stats['max_error']:>6.1f}° {stats['n']:>5} {stats['within_1f']:>5.1f}% {stats['within_2f']:>5.1f}%")
