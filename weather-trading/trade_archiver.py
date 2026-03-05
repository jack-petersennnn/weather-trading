#!/usr/bin/env python3
"""
Trade Archiver — Moves closed trades from trades.json to trades_archive.json.
Keeps trades.json lean with only open positions.
Run after settler or manually.
"""

import json
import os
import fcntl
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRADES_FILE = os.path.join(BASE_DIR, "trades.json")
ARCHIVE_FILE = os.path.join(BASE_DIR, "trades_archive.json")


def archive():
    """Move all non-open trades to archive. Returns count archived."""
    if not os.path.exists(TRADES_FILE):
        return 0
    
    with open(TRADES_FILE) as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        data = json.load(f)
        fcntl.flock(f, fcntl.LOCK_UN)
    
    trades = data.get("trades", [])
    open_trades = [t for t in trades if t.get("status") == "open"]
    closed_trades = [t for t in trades if t.get("status") != "open"]
    
    if not closed_trades:
        print("No closed trades to archive.")
        return 0
    
    # Load existing archive
    archive_data = {"trades": [], "archived_at": []}
    if os.path.exists(ARCHIVE_FILE):
        try:
            with open(ARCHIVE_FILE) as f:
                archive_data = json.load(f)
        except:
            archive_data = {"trades": [], "archived_at": []}
    
    # Deduplicate by ticker+timestamp
    existing_keys = {(t.get("ticker"), t.get("timestamp")) for t in archive_data.get("trades", [])}
    new_archived = 0
    for t in closed_trades:
        key = (t.get("ticker"), t.get("timestamp"))
        if key not in existing_keys:
            archive_data["trades"].append(t)
            new_archived += 1
    
    archive_data["archived_at"].append(datetime.now(timezone.utc).isoformat())
    
    # Save archive
    with open(ARCHIVE_FILE, "w") as f:
        json.dump(archive_data, f, indent=2)
    
    # Save trades.json with only open trades
    data["trades"] = open_trades
    with open(TRADES_FILE, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        json.dump(data, f, indent=2)
        f.flush()
        fcntl.flock(f, fcntl.LOCK_UN)
    
    print(f"📦 Archived {new_archived} closed trades. trades.json: {len(open_trades)} open remaining.")
    return new_archived


if __name__ == "__main__":
    archive()
