#!/usr/bin/env python3
"""
Rebuild Canonical Event Ledger

This script rebuilds a clean event ledger using:
1. Complete fill data from Kalshi API (no duplicates)
2. Deduplicated settlement data (1 per market instead of 5)

The result should fix the ~$483 reconciliation gap caused by duplicate entries.
"""

import json
import os
from collections import defaultdict
from datetime import datetime, timezone

def normalize_api_fill(api_fill):
    """Convert API fill format to internal ledger format."""
    # Map API action to direction
    action = api_fill.get("action", "").upper()
    direction = "BUY" if action == "BUY" else "SELL"
    
    # Get side (API has lowercase, we want uppercase)
    side = api_fill.get("side", "").upper()
    
    # Get quantity
    qty = api_fill.get("count", 0)
    
    # Get price in cents
    # API has yes_price and no_price, choose based on side
    if side == "YES":
        price_cents = api_fill.get("yes_price", 0)
    else:
        price_cents = api_fill.get("no_price", 0)
    
    # Get fee in cents
    fee_cost = float(api_fill.get("fee_cost", 0))
    fee_cents = round(fee_cost * 100)
    
    # Create normalized fill event
    return {
        "type": "FILL",
        "fill_id": api_fill.get("fill_id", api_fill.get("trade_id")),
        "order_id": api_fill.get("order_id", ""),
        "market_ticker": api_fill.get("ticker", api_fill.get("market_ticker", "")),
        "side": side,
        "dir": direction,
        "qty": qty,
        "price_cents": price_cents,
        "fee_cents": fee_cents,
        "ts": api_fill.get("ts", 0),
    }

def deduplicate_settlements(old_ledger_path):
    """Extract and deduplicate settlements from old ledger."""
    settlements_by_ticker = {}
    
    with open(old_ledger_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or '"type":"SETTLEMENT"' not in line:
                continue
                
            try:
                event = json.loads(line)
                if event.get("type") != "SETTLEMENT":
                    continue
                    
                ticker = event.get("market_ticker")
                if not ticker:
                    continue
                
                # Keep the settlement with most complete data (has payout_cents)
                # or if same completeness, keep the last one (latest timestamp)
                existing = settlements_by_ticker.get(ticker)
                
                if not existing:
                    settlements_by_ticker[ticker] = event
                else:
                    # Prefer settlements with payout_cents data
                    event_has_payout = "payout_cents" in event
                    existing_has_payout = "payout_cents" in existing
                    
                    if event_has_payout and not existing_has_payout:
                        settlements_by_ticker[ticker] = event
                    elif event_has_payout == existing_has_payout:
                        # Same payout data availability, use newer timestamp
                        event_ts = event.get("ts", 0)
                        existing_ts = existing.get("ts", 0)
                        if event_ts > existing_ts:
                            settlements_by_ticker[ticker] = event
                            
            except json.JSONDecodeError:
                continue
    
    return list(settlements_by_ticker.values())

def main():
    print("Rebuilding canonical event ledger...")
    
    # Load complete fills from API
    try:
        with open("kalshi_fills_complete.json", 'r') as f:
            api_fills = json.load(f)
        print(f"Loaded {len(api_fills)} unique fills from API")
    except FileNotFoundError:
        print("ERROR: kalshi_fills_complete.json not found. Run pull_complete_fills.py first.")
        return 1
    
    # Deduplicate settlements from old ledger
    old_ledger = "ledger.jsonl.backup"
    if not os.path.exists(old_ledger):
        print(f"ERROR: {old_ledger} not found. Cannot extract settlements.")
        return 1
        
    settlements = deduplicate_settlements(old_ledger)
    print(f"Extracted {len(settlements)} deduplicated settlements")
    
    # Create new canonical ledger
    new_ledger = "ledger_canonical.jsonl"
    events = []
    
    # Add normalized fills
    for api_fill in api_fills:
        normalized = normalize_api_fill(api_fill)
        events.append(normalized)
    
    # Add deduplicated settlements
    events.extend(settlements)
    
    # Sort all events by timestamp (handle both string and int timestamps)
    def get_timestamp(event):
        ts = event.get("ts", 0)
        if isinstance(ts, str):
            # Try to parse ISO format to timestamp
            try:
                return datetime.fromisoformat(ts.replace('Z', '+00:00')).timestamp()
            except:
                return 0
        return ts
    
    events.sort(key=get_timestamp)
    
    # Write new canonical ledger
    with open(new_ledger, 'w') as f:
        for event in events:
            f.write(json.dumps(event, separators=(",", ":")) + "\n")
    
    print(f"Created {new_ledger} with {len(events)} total events")
    print(f"  {len(api_fills)} fills (unique)")
    print(f"  {len(settlements)} settlements (deduplicated)")
    
    # Show the difference
    with open("ledger.jsonl.backup", 'r') as f:
        old_events = sum(1 for line in f if line.strip())
    print(f"Old ledger had {old_events} total events")
    print(f"Removed {old_events - len(events)} duplicate events")
    
    return 0

if __name__ == "__main__":
    exit(main())