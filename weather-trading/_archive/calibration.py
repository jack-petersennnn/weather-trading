#!/usr/bin/env python3
"""City-Specific Calibration — adjusts forecast std based on historical accuracy per city."""

import json
import os
import statistics
from datetime import datetime, timezone

ACCURACY_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/accuracy.json"
CALIBRATION_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/city_calibration.json"
HIST_TEMPS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/historical-temps.json"

# Default volatility profiles (used when insufficient data)
DEFAULT_PROFILES = {
    "New York":    {"volatility_class": "high",   "default_multiplier": 1.3},
    "Chicago":     {"volatility_class": "high",   "default_multiplier": 1.2},
    "Miami":       {"volatility_class": "low",    "default_multiplier": 0.85},
    "Denver":      {"volatility_class": "high",   "default_multiplier": 1.4},
    "Los Angeles": {"volatility_class": "low",    "default_multiplier": 0.8},
    "Austin":      {"volatility_class": "medium", "default_multiplier": 1.1},
}

# Minimum samples before we trust data-driven calibration
MIN_SAMPLES = 3


def load_calibration():
    """Load existing calibration or return empty."""
    if os.path.exists(CALIBRATION_FILE):
        try:
            with open(CALIBRATION_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"calibrations": {}, "defaults": {"high": 1.3, "medium": 1.1, "low": 0.85}, "last_updated": None}


def save_calibration(data):
    with open(CALIBRATION_FILE, "w") as f:
        json.dump(data, f, indent=2)


def compute_temp_volatility():
    """Compute temperature volatility from historical data."""
    volatility = {}
    if not os.path.exists(HIST_TEMPS_FILE):
        return volatility
    try:
        with open(HIST_TEMPS_FILE) as f:
            hist = json.load(f)
    except Exception:
        return volatility

    for city, cdata in hist.get("cities", {}).items():
        temps = list(cdata.get("temps", {}).values())
        if len(temps) >= 5:
            # Day-to-day changes
            changes = [abs(temps[i] - temps[i-1]) for i in range(1, len(temps))]
            volatility[city] = {
                "temp_std": round(statistics.pstdev(temps), 2),
                "avg_daily_change": round(statistics.mean(changes), 2),
                "max_daily_change": round(max(changes), 2),
            }
    return volatility


def run():
    print("=== KingClaw City Calibration ===")
    now = datetime.now(timezone.utc)
    print(f"Time: {now.strftime('%Y-%m-%d %H:%M UTC')}")

    cal = load_calibration()

    # Load accuracy data
    accuracy_data = {}
    if os.path.exists(ACCURACY_FILE):
        try:
            with open(ACCURACY_FILE) as f:
                acc = json.load(f)
            for comp in acc.get("comparisons", []):
                city = comp.get("city", "")
                if city not in accuracy_data:
                    accuracy_data[city] = []
                accuracy_data[city].append(comp)
        except Exception as e:
            print(f"  Warning: Could not load accuracy data: {e}")

    # Compute temperature volatility
    volatility = compute_temp_volatility()

    # Update calibrations per city
    calibrations = {}
    for city, profile in DEFAULT_PROFILES.items():
        errors = [c["error"] for c in accuracy_data.get(city, [])]
        sample_size = len(errors)

        if sample_size >= MIN_SAMPLES:
            # Data-driven calibration
            mae = statistics.mean(errors)
            # Higher MAE → need larger std multiplier
            # Base: MAE of 2°F is "normal" → multiplier 1.0
            # Scale: every 1°F of MAE above 2 adds 0.2 to multiplier
            data_multiplier = 1.0 + (mae - 2.0) * 0.2
            data_multiplier = max(0.7, min(1.8, data_multiplier))

            # Blend with default (weight data more as samples increase)
            blend = min(sample_size / 10, 1.0)  # full trust at 10+ samples
            multiplier = blend * data_multiplier + (1 - blend) * profile["default_multiplier"]
        else:
            # Use defaults, lightly adjusted by any data we have
            mae = statistics.mean(errors) if errors else None
            multiplier = profile["default_multiplier"]
            if mae is not None and sample_size > 0:
                # Slight adjustment
                data_mult = 1.0 + (mae - 2.0) * 0.2
                data_mult = max(0.7, min(1.8, data_mult))
                blend = sample_size / 10
                multiplier = blend * data_mult + (1 - blend) * profile["default_multiplier"]

        # Also factor in temperature volatility
        vol = volatility.get(city, {})
        if vol.get("avg_daily_change", 0) > 8:
            multiplier *= 1.1  # extra boost for very volatile cities
        elif vol.get("avg_daily_change", 0) < 3:
            multiplier *= 0.95  # slight reduction for stable cities

        multiplier = round(max(0.7, min(1.8, multiplier)), 3)

        calibrations[city] = {
            "historical_mae": round(mae, 2) if mae else None,
            "adjusted_std_multiplier": multiplier,
            "sample_size": sample_size,
            "volatility_class": profile["volatility_class"],
            "temp_volatility": vol,
        }

        status = f"data-driven ({sample_size} samples)" if sample_size >= MIN_SAMPLES else f"default+{sample_size} samples"
        print(f"  {city}: multiplier={multiplier} ({status})")

    cal["calibrations"] = calibrations
    cal["last_updated"] = now.isoformat()
    save_calibration(cal)

    print(f"\nSaved calibration to {CALIBRATION_FILE}")
    return cal


if __name__ == "__main__":
    run()
