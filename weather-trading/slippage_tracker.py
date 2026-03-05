#!/usr/bin/env python3
"""
Slippage Tracker — Records intended vs actual fill prices.
Answers: "Are we getting the prices we think we're getting?"
"""

import json
import os
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SLIPPAGE_FILE = os.path.join(BASE_DIR, "slippage_log.json")


def _load():
    if os.path.exists(SLIPPAGE_FILE):
        try:
            with open(SLIPPAGE_FILE) as f:
                return json.load(f)
        except:
            pass
    return {"entries": [], "summary": {}}


def _save(data):
    with open(SLIPPAGE_FILE, "w") as f:
        json.dump(data, f, indent=2)


def record(ticker, side, intended_price_cents, actual_fill_price_cents, contracts, order_type="entry"):
    """Record a fill with slippage data.
    
    Args:
        intended_price_cents: what we submitted as limit price
        actual_fill_price_cents: what we actually got filled at (from order response)
        order_type: 'entry' or 'exit'
    """
    data = _load()
    slippage = actual_fill_price_cents - intended_price_cents
    # For entries: positive slippage = we paid more (bad)
    # For exits: positive slippage = we got more (good)
    
    data["entries"].append({
        "ticker": ticker,
        "side": side,
        "intended": intended_price_cents,
        "actual": actual_fill_price_cents,
        "slippage_cents": slippage,
        "contracts": contracts,
        "type": order_type,
        "ts": datetime.now(timezone.utc).isoformat(),
    })
    
    # Keep last 500
    data["entries"] = data["entries"][-500:]
    
    # Rebuild summary
    entries_with_data = [e for e in data["entries"] if e.get("slippage_cents") is not None]
    if entries_with_data:
        entry_slips = [e["slippage_cents"] for e in entries_with_data if e["type"] == "entry"]
        exit_slips = [e["slippage_cents"] for e in entries_with_data if e["type"] == "exit"]
        
        data["summary"] = {
            "total_records": len(entries_with_data),
            "entry_avg_slippage": round(sum(entry_slips) / len(entry_slips), 2) if entry_slips else 0,
            "exit_avg_slippage": round(sum(exit_slips) / len(exit_slips), 2) if exit_slips else 0,
            "zero_slippage_pct": round(sum(1 for e in entries_with_data if e["slippage_cents"] == 0) / len(entries_with_data), 3),
        }
    
    _save(data)


def get_summary():
    data = _load()
    return data.get("summary", {})
