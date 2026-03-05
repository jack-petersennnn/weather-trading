#!/usr/bin/env python3
"""
Per-City Daily Logger — Logs every forecast, trade, and decision per city per day.
Used for learning and recalibration. Logs stored in logs/cities/YYYY-MM-DD/<city>.jsonl
"""

import json
import os
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs", "cities")


def _get_log_path(city: str, date_str: str = None) -> str:
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    day_dir = os.path.join(LOG_DIR, date_str)
    os.makedirs(day_dir, exist_ok=True)
    safe_city = city.replace(" ", "_").lower()
    return os.path.join(day_dir, f"{safe_city}.jsonl")


def log_event(city: str, event_type: str, data: dict, date_str: str = None):
    """Append a log entry for a city. Types: forecast, trade, skip, sell, settle, error"""
    path = _get_log_path(city, date_str)
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "type": event_type,
        **data
    }
    with open(path, "a") as f:
        f.write(json.dumps(entry) + "\n")


def log_forecast(city: str, sources: dict, mean: float, std: float, target_date: str):
    """Log a forecast snapshot for a city."""
    log_event(city, "forecast", {
        "target_date": target_date,
        "sources": sources,
        "ensemble_mean": round(mean, 2),
        "ensemble_std": round(std, 2),
        "source_count": len(sources),
    })


def log_trade_decision(city: str, action: str, ticker: str, direction: str,
                       edge: float, entry_price: float, contracts: int,
                       cost_cents: int, reason: str = "", target_date: str = ""):
    """Log a trade placement or skip."""
    log_event(city, action, {  # action = "trade" or "skip"
        "target_date": target_date,
        "ticker": ticker,
        "direction": direction,
        "edge": round(edge, 4),
        "entry_price": round(entry_price, 4),
        "contracts": contracts,
        "cost_cents": cost_cents,
        "reason": reason,
    })


def log_position_action(city: str, action: str, ticker: str, details: dict):
    """Log PM actions (sell, hold, adjust)."""
    log_event(city, action, {"ticker": ticker, **details})


def get_city_log(city: str, date_str: str = None) -> list:
    """Read all log entries for a city on a given day."""
    path = _get_log_path(city, date_str)
    if not os.path.exists(path):
        return []
    entries = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries


if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 2:
        city = sys.argv[1]
        date_str = sys.argv[2] if len(sys.argv) >= 3 else None
        entries = get_city_log(city, date_str)
        for e in entries:
            print(json.dumps(e, indent=2))
    else:
        print("Usage: city_logger.py <city> [YYYY-MM-DD]")
