#!/usr/bin/env python3
"""Convert settlements.csv to JSON format for balance_sim."""

import json
import csv
from datetime import datetime

def convert_settlements_to_json():
    """Convert settlements.csv to JSON format expected by balance_sim."""
    settlements = []
    
    with open('settlements.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            settlement = {
                'ticker': row['ticker'],
                'settled_time': row['ts'],
                'revenue': int(row['revenue']),  # Already in cents
                'fee_cost': 0,  # We'll get settlement fees from ledger_canonical.jsonl
                'result': row['market_result']
            }
            settlements.append(settlement)
    
    # Now add settlement fees from ledger_canonical.jsonl
    settlement_fees = {}
    
    with open('ledger_canonical.jsonl', 'r') as f:
        for line in f:
            if line.strip():
                event = json.loads(line.strip())
                if event.get('type') == 'SETTLEMENT':
                    ticker = event.get('market_ticker')
                    fee = event.get('settle_fee_cents', 0)
                    if ticker and fee > 0:
                        settlement_fees[ticker] = fee
    
    # Apply settlement fees
    for settlement in settlements:
        ticker = settlement['ticker']
        if ticker in settlement_fees:
            settlement['fee_cost'] = settlement_fees[ticker] / 100.0  # Convert to dollars
    
    # Write JSON
    with open('/tmp/kalshi_settlements.json', 'w') as f:
        json.dump(settlements, f, indent=2)
    
    print(f"Converted {len(settlements)} settlements to /tmp/kalshi_settlements.json")
    return settlements

if __name__ == "__main__":
    convert_settlements_to_json()