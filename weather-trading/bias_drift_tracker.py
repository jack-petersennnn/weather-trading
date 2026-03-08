#!/usr/bin/env python3
"""
7-day vs 14-day bias tracker for weather model monitoring pipeline.

Compares recent short-term bias vs medium-term bias to detect regime drift.
Uses the same data sources and bias calculation as sigma_validator.py.

Bias definition: actual - forecast (same as existing system)
- Positive bias = forecasts running cold (actual higher than forecast)
- Negative bias = forecasts running warm (actual lower than forecast)
"""

import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def load_json(filename):
    """Load JSON file from the base directory."""
    path = os.path.join(BASE_DIR, filename)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def save_json(filename, data):
    """Save data to JSON file."""
    path = os.path.join(BASE_DIR, filename)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def get_bias_data(days_back=14):
    """
    Load forecast/actual pairs and compute per-city bias data.
    Returns dict: city -> [(date, bias), ...] sorted by date desc
    """
    forecast_log = load_json("training_forecast_log.json")
    nws_data = load_json("nws_gridpoint_log.json")
    actuals = nws_data.get("actuals", {})

    if not forecast_log or not actuals:
        return {}

    # Collect bias data per city
    city_bias_data = defaultdict(list)

    for key, entry in forecast_log.items():
        city = entry["city"]
        date = entry["target_date"]
        mean = entry["ensemble_mean"]

        # Find matching actual
        actual_key = f"{city}_{date}"
        if actual_key not in actuals:
            continue

        actual = actuals[actual_key]
        bias = actual - mean  # Same as sigma_validator.py: actual - forecast

        city_bias_data[city].append((date, bias))

    # Sort by date descending and limit to recent data
    cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    
    for city in city_bias_data:
        # Sort by date descending (most recent first)
        city_bias_data[city].sort(key=lambda x: x[0], reverse=True)
        # Filter to recent data
        city_bias_data[city] = [
            (date, bias) for date, bias in city_bias_data[city]
            if date >= cutoff_date
        ]

    return dict(city_bias_data)


def compute_bias_drift():
    """
    Compute 7-day vs 14-day bias drift for all cities.
    Returns dict with results per city.
    """
    bias_data = get_bias_data(days_back=14)
    
    results = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cities": {},
        "summary": {
            "total_cities": 0,
            "stable": 0,
            "warming_shift": 0,
            "cooling_shift": 0,
            "insufficient_data": 0
        }
    }

    for city, data_points in bias_data.items():
        if not data_points:
            continue

        # Split into 7-day and 14-day windows
        recent_7d = data_points[:7]  # Most recent 7 days
        recent_14d = data_points[:14]  # Most recent 14 days

        # Calculate bias for each window
        bias_7d = None
        bias_14d = None
        
        if len(recent_7d) >= 4:  # Minimum 4 samples for 7-day
            bias_7d = sum(bias for _, bias in recent_7d) / len(recent_7d)
        
        if len(recent_14d) >= 4:  # Minimum 4 samples for 14-day
            bias_14d = sum(bias for _, bias in recent_14d) / len(recent_14d)

        # Compute delta and status
        delta = None
        absolute_delta = None
        status = "insufficient_data"
        
        if bias_7d is not None and bias_14d is not None:
            delta = bias_7d - bias_14d
            absolute_delta = abs(delta)
            
            # Apply flagging logic
            if absolute_delta > 0.5:
                if delta > 0.5:
                    status = "warming_shift"  # 7d bias more positive (warmer actuals relative to forecasts)
                elif delta < -0.5:
                    status = "cooling_shift"  # 7d bias more negative (cooler actuals relative to forecasts)
            else:
                status = "stable"

        # Store results
        city_result = {
            "samples_7d": len(recent_7d),
            "samples_14d": len(recent_14d),
            "bias_7d": round(bias_7d, 2) if bias_7d is not None else None,
            "bias_14d": round(bias_14d, 2) if bias_14d is not None else None,
            "delta": round(delta, 2) if delta is not None else None,
            "absolute_delta": round(absolute_delta, 2) if absolute_delta is not None else None,
            "status": status,
            "date_range_7d": f"{recent_7d[-1][0]} to {recent_7d[0][0]}" if recent_7d else "N/A",
            "date_range_14d": f"{recent_14d[-1][0]} to {recent_14d[0][0]}" if recent_14d else "N/A",
        }
        
        results["cities"][city] = city_result
        
        # Update summary counts
        results["summary"]["total_cities"] += 1
        results["summary"][status] += 1

    return results


def print_bias_drift_report(results):
    """Print a formatted bias drift report."""
    print(f"{'='*70}")
    print(f" BIAS DRIFT TRACKER REPORT")
    print(f" Generated: {results['generated_at'][:19]} UTC")
    print(f"{'='*70}")
    print()
    
    summary = results["summary"]
    print(f" Summary: {summary['total_cities']} cities analyzed")
    print(f"   ✅ Stable: {summary['stable']}")
    print(f"   🔴 Warming shift: {summary['warming_shift']}")
    print(f"   🔵 Cooling shift: {summary['cooling_shift']}")
    print(f"   ⚠️  Insufficient data: {summary['insufficient_data']}")
    print()

    # Print detailed results
    print(f" {'City':20s} {'7d N':>4s} {'14d N':>5s} {'7d Bias':>8s} {'14d Bias':>9s} {'Delta':>6s} {'Status':>15s}")
    print(f" {'-'*75}")
    
    # Sort cities by absolute delta (descending) to show most concerning first
    sorted_cities = sorted(
        results["cities"].items(),
        key=lambda x: x[1].get("absolute_delta", 0) if x[1].get("absolute_delta") is not None else 0,
        reverse=True
    )
    
    for city, data in sorted_cities:
        # Format bias values
        bias_7d_str = f"{data['bias_7d']:+5.1f}" if data['bias_7d'] is not None else "  N/A"
        bias_14d_str = f"{data['bias_14d']:+5.1f}" if data['bias_14d'] is not None else "   N/A"
        delta_str = f"{data['delta']:+5.1f}" if data['delta'] is not None else "  N/A"
        
        # Status icon
        status_icon = {
            "stable": "✅ stable",
            "warming_shift": "🔴 warming_shift",
            "cooling_shift": "🔵 cooling_shift",
            "insufficient_data": "⚠️ insufficient_data"
        }.get(data["status"], data["status"])
        
        print(f" {city:20s} {data['samples_7d']:4d} {data['samples_14d']:5d} "
              f"{bias_7d_str:>8s} {bias_14d_str:>9s} {delta_str:>6s} {status_icon:>15s}")

    print()
    
    # Show flagged cities with details
    flagged = [(city, data) for city, data in results["cities"].items() 
               if data["status"] in ["warming_shift", "cooling_shift"]]
    
    if flagged:
        print(f" {'='*70}")
        print(" REGIME SHIFT ALERTS")
        print(f" {'='*70}")
        for city, data in flagged:
            print(f" 🚨 {city}: {data['status'].replace('_', ' ').title()}")
            print(f"    7-day bias: {data['bias_7d']:+.1f}°F ({data['date_range_7d']})")
            print(f"    14-day bias: {data['bias_14d']:+.1f}°F ({data['date_range_14d']})")
            print(f"    Delta: {data['delta']:+.1f}°F (threshold: ±0.5°F)")
            print()
    else:
        print(" ✅ No regime shifts detected (all deltas within ±0.5°F threshold)")
        print()


def main():
    """Main function to run bias drift analysis."""
    results = compute_bias_drift()
    
    if "--json" in sys.argv:
        print(json.dumps(results, indent=2))
    else:
        print_bias_drift_report(results)
    
    # Always save results for historical tracking
    save_json("bias_drift_state.json", results)
    
    if "--save" in sys.argv or "--json" not in sys.argv:
        print(f"💾 Results saved to bias_drift_state.json")

    return results


if __name__ == "__main__":
    main()