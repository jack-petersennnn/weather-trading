#!/usr/bin/env python3
"""
Hypothetical Trade Tracker — logs trades that WOULD have been placed
if not filtered by source spread, MIN_SOURCES, or 2σ buffer.
Useful for evaluating filter effectiveness.
"""

import json
import os
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HYPO_FILE = os.path.join(BASE_DIR, "hypothetical_trades.json")


def log_hypothetical(city, ticker, direction, edge, entry_price_cents,
                     reason_skipped, ensemble_mean, ensemble_std, source_spread):
    """Append a hypothetical trade to the JSON log file."""
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "city": city,
        "ticker": ticker,
        "direction": direction,
        "edge": round(edge, 4) if edge is not None else None,
        "entry_price_cents": entry_price_cents,
        "reason_skipped": reason_skipped,
        "ensemble_mean": round(ensemble_mean, 2) if ensemble_mean is not None else None,
        "ensemble_std": round(ensemble_std, 2) if ensemble_std is not None else None,
        "source_spread": round(source_spread, 2) if source_spread is not None else None,
    }

    data = []
    if os.path.exists(HYPO_FILE):
        try:
            with open(HYPO_FILE) as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError):
            data = []

    data.append(entry)

    # Keep last 2000 entries to avoid unbounded growth
    if len(data) > 2000:
        data = data[-1500:]

    with open(HYPO_FILE, "w") as f:
        json.dump(data, f, indent=2)
