#!/usr/bin/env python3
"""
Forecast Logger — Saves per-source forecast snapshots per city/date.

Called from fast_scanner after each city's forecasts are collected.
Tracks how forecasts evolve over time (up to 20 snapshots per city/date).

This is distinct from training_forecast_log.json which only stores the
latest values. This stores the evolution history.
"""

import json
import os
import fcntl
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "forecast_history.json")


def log_snapshot(city, series, target_date, source_forecasts, ensemble_mean, calibrated_std):
    """Log a forecast snapshot for a city/date.
    
    Args:
        city: City name (e.g. "Phoenix")
        series: Kalshi series (e.g. "KXHIGHTPHX")
        target_date: Target date string (YYYY-MM-DD)
        source_forecasts: Dict of {source_name: temp_f}
        ensemble_mean: Weighted ensemble mean
        calibrated_std: Calibrated standard deviation
    """
    if not target_date or not source_forecasts:
        return
    
    now = datetime.now(timezone.utc)
    key = f"{city}|{target_date}"
    
    # Load existing history
    history = {}
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE) as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                history = json.load(f)
                fcntl.flock(f, fcntl.LOCK_UN)
        except:
            history = {}
    
    # Get or create entry
    entry = history.get(key, {
        "city": city,
        "series": series,
        "target_date": target_date,
        "snapshots": [],
    })
    
    # Add snapshot (keep last 20 to track forecast evolution)
    snapshot = {
        "ts": now.isoformat(),
        "ensemble_mean": ensemble_mean,
        "calibrated_std": calibrated_std,
        "sources": {k: round(v, 2) if isinstance(v, float) else v 
                    for k, v in source_forecasts.items()},
    }
    
    entry["snapshots"].append(snapshot)
    if len(entry["snapshots"]) > 20:
        entry["snapshots"] = entry["snapshots"][-20:]
    
    # Store latest for easy access
    entry["final_ensemble_mean"] = ensemble_mean
    entry["final_calibrated_std"] = calibrated_std
    entry["final_source_forecasts"] = source_forecasts
    entry["last_updated"] = now.isoformat()
    
    history[key] = entry
    
    # Save
    with open(LOG_FILE, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        json.dump(history, f, indent=2)
        f.flush()
        fcntl.flock(f, fcntl.LOCK_UN)


def run():
    """Legacy entry point — reads from analysis.json.
    Kept for backward compatibility but log_snapshot() is preferred."""
    ANALYSIS_FILE = os.path.join(BASE_DIR, "analysis.json")
    if not os.path.exists(ANALYSIS_FILE):
        return
    
    with open(ANALYSIS_FILE) as f:
        analysis = json.load(f)
    
    updates = 0
    for series, cdata in analysis.get("cities", {}).items():
        city = cdata.get("city", "")
        for event in cdata.get("events", []):
            target_date = event.get("target_date")
            source_forecasts = event.get("source_forecasts", {})
            ensemble_mean = event.get("ensemble_mean")
            calibrated_std = event.get("calibrated_std")
            if target_date and source_forecasts:
                log_snapshot(city, series, target_date, source_forecasts,
                            ensemble_mean, calibrated_std)
                updates += 1
    
    if updates:
        print(f"  📝 Logged {updates} forecast snapshots (from analysis.json)")


if __name__ == "__main__":
    run()
