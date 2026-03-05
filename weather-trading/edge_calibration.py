#!/usr/bin/env python3
"""
Edge Calibration Tracker — Did our 80% predictions actually hit 80%?

Buckets predictions by probability range and tracks actual outcomes.
This is the single most important metric for knowing if our model works.
"""

import json
import os
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CALIBRATION_FILE = os.path.join(BASE_DIR, "edge_calibration.json")

# Probability buckets: [0.5-0.6), [0.6-0.7), [0.7-0.8), [0.8-0.9), [0.9-1.0]
BUCKETS = [(0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 1.0)]


def _load():
    if os.path.exists(CALIBRATION_FILE):
        try:
            with open(CALIBRATION_FILE) as f:
                return json.load(f)
        except:
            pass
    return {"predictions": [], "summary": {}}


def _save(data):
    with open(CALIBRATION_FILE, "w") as f:
        json.dump(data, f, indent=2)


def record_prediction(ticker, direction, our_prob, market_price, edge, city=""):
    """Record a prediction at entry time. Outcome filled in later by settler."""
    data = _load()
    data["predictions"].append({
        "ticker": ticker,
        "direction": direction,
        "our_prob": round(our_prob, 4),
        "market_price": round(market_price, 4),
        "edge": round(edge, 4),
        "city": city,
        "ts": datetime.now(timezone.utc).isoformat(),
        "outcome": None,  # filled by record_outcome
    })
    _save(data)


def record_outcome(ticker, direction, won: bool):
    """Record whether a prediction was correct."""
    data = _load()
    for pred in reversed(data["predictions"]):
        if pred["ticker"] == ticker and pred["direction"] == direction and pred["outcome"] is None:
            pred["outcome"] = "win" if won else "loss"
            break
    _save(data)
    _rebuild_summary(data)


def _rebuild_summary(data=None):
    if data is None:
        data = _load()
    
    summary = {}
    for lo, hi in BUCKETS:
        key = f"{lo:.1f}-{hi:.1f}"
        preds = [p for p in data["predictions"] 
                 if p["outcome"] is not None and lo <= p["our_prob"] < hi]
        wins = sum(1 for p in preds if p["outcome"] == "win")
        total = len(preds)
        expected_rate = (lo + hi) / 2
        actual_rate = wins / total if total > 0 else None
        summary[key] = {
            "total": total,
            "wins": wins,
            "expected_rate": round(expected_rate, 2),
            "actual_rate": round(actual_rate, 4) if actual_rate is not None else None,
            "calibration_error": round(actual_rate - expected_rate, 4) if actual_rate is not None else None,
        }
    
    # Overall
    all_settled = [p for p in data["predictions"] if p["outcome"] is not None]
    total = len(all_settled)
    wins = sum(1 for p in all_settled if p["outcome"] == "win")
    
    # Weighted expected win rate (based on our predicted probs)
    if all_settled:
        expected_wins = sum(p["our_prob"] for p in all_settled)
        expected_wr = expected_wins / total
        actual_wr = wins / total
        summary["overall"] = {
            "total": total,
            "wins": wins,
            "actual_win_rate": round(actual_wr, 4),
            "expected_win_rate": round(expected_wr, 4),
            "calibration_error": round(actual_wr - expected_wr, 4),
        }
    
    data["summary"] = summary
    _save(data)
    return summary


def get_summary():
    """Get calibration summary."""
    data = _load()
    if not data.get("summary") or not data["summary"].get("overall"):
        return _rebuild_summary(data)
    return data["summary"]


def print_report():
    """Print human-readable calibration report."""
    s = get_summary()
    print("\n📊 Edge Calibration Report")
    print("=" * 50)
    for lo, hi in BUCKETS:
        key = f"{lo:.1f}-{hi:.1f}"
        b = s.get(key, {})
        if b.get("total", 0) == 0:
            print(f"  {key}: no data")
            continue
        actual = b["actual_rate"]
        expected = b["expected_rate"]
        err = b["calibration_error"]
        emoji = "✅" if abs(err) < 0.10 else "⚠️" if abs(err) < 0.20 else "🔴"
        print(f"  {emoji} {key}: {b['wins']}/{b['total']} = {actual:.0%} actual vs {expected:.0%} expected (err: {err:+.0%})")
    
    overall = s.get("overall", {})
    if overall:
        print(f"\n  Overall: {overall['wins']}/{overall['total']} = {overall['actual_win_rate']:.0%} "
              f"(expected {overall['expected_win_rate']:.0%}, err {overall['calibration_error']:+.0%})")


if __name__ == "__main__":
    print_report()
