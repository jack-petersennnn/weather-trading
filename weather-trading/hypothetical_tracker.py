#!/usr/bin/env python3
"""
Hypothetical Trade Tracker — Paper trading journal.
Logs trades that WOULD have been placed with full context:
who said what, what the market offered, what we'd bet, and why.
"""

import json
import os
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HYPO_FILE = os.path.join(BASE_DIR, "hypothetical_trades.json")


def log_hypothetical(city, ticker, direction, edge, entry_price_cents,
                     reason_skipped, ensemble_mean, ensemble_std, source_spread,
                     # New fields for paper trading journal
                     our_probability=None,
                     market_probability=None,
                     kelly_fraction=None,
                     contracts=None,
                     expected_profit_cents=None,
                     sources_used=None,
                     source_forecasts=None,
                     bias_correction=None,
                     sigma_multiplier=None,
                     target_date=None,
                     bracket=None,
                     strike=None,
                     notes=None):
    """Append a hypothetical trade to the JSON log file.
    
    This serves as our paper trading journal. Every trade the bot WOULD make
    gets logged here with full reasoning and context.
    """
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "city": city,
        "target_date": target_date,
        "ticker": ticker,
        "direction": direction,
        # Market context
        "market_price_cents": entry_price_cents,
        "market_probability": round(market_probability, 4) if market_probability is not None else None,
        "our_probability": round(our_probability, 4) if our_probability is not None else None,
        "edge": round(edge, 4) if edge is not None else None,
        # Position sizing
        "kelly_fraction": round(kelly_fraction, 6) if kelly_fraction is not None else None,
        "contracts": contracts,
        "expected_profit_cents": round(expected_profit_cents, 1) if expected_profit_cents is not None else None,
        # Forecast detail
        "ensemble_mean": round(ensemble_mean, 2) if ensemble_mean is not None else None,
        "ensemble_std": round(ensemble_std, 2) if ensemble_std is not None else None,
        "source_spread": round(source_spread, 2) if source_spread is not None else None,
        "sigma_multiplier": sigma_multiplier,
        "bias_correction": round(bias_correction, 2) if bias_correction is not None else None,
        "bracket": bracket,
        "strike": strike,
        # What sources contributed
        "sources_used": sources_used,  # list of source names
        "source_forecasts": source_forecasts,  # dict of {source: predicted_temp}
        # Decision
        "reason_skipped": reason_skipped,  # why we didn't trade (or None if we would have)
        "notes": notes,
    }

    # Strip None values to keep the file compact
    entry = {k: v for k, v in entry.items() if v is not None}

    # Also keep the old field name for backward compat
    if "market_price_cents" in entry:
        entry["entry_price_cents"] = entry["market_price_cents"]

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
