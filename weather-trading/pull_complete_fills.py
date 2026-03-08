#!/usr/bin/env python3
"""
Pull Complete Fill History from Kalshi API

This script fetches all historical fills from the Kalshi API and caches them
locally. It's needed because ledger.jsonl only has fills since the bot started
logging, missing earlier historical fills.
"""

import json
import kalshi_client
from datetime import datetime

def main():
    print("Fetching complete fill history from Kalshi API...")
    
    try:
        # Get all fills from API
        all_fills = kalshi_client.get_fills()
        print(f"Retrieved {len(all_fills)} total fills from API")
        
        # Cache to file
        cache_file = "kalshi_fills_complete.json"
        with open(cache_file, 'w') as f:
            json.dump(all_fills, f, indent=2)
        print(f"Cached fills to {cache_file}")
        
        # Show some stats
        if all_fills:
            dates = [f.get('ts', f.get('created_time', '')) for f in all_fills if f.get('ts', f.get('created_time', ''))]
            dates = [d for d in dates if d]  # Filter out empty dates
            if dates:
                print(f"Date range: {min(dates)} to {max(dates)}")
        
        # Count fills in current ledger.jsonl for comparison
        try:
            with open('ledger.jsonl', 'r') as f:
                ledger_fills = sum(1 for line in f if '"type":"FILL"' in line)
            print(f"Fills in current ledger.jsonl: {ledger_fills}")
            print(f"Missing fills (API - ledger): {len(all_fills) - ledger_fills}")
        except FileNotFoundError:
            print("ledger.jsonl not found for comparison")
            
    except Exception as e:
        print(f"Error fetching fills: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())