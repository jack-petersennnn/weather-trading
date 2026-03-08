#!/usr/bin/env python3
"""
Investigate duplicate settlements in the ledger
"""

import json
import os
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LEDGER_FILE = os.path.join(BASE_DIR, "ledger.jsonl")

def investigate_duplicate_settlements():
    """Check for duplicate or suspicious settlements"""
    settlements = []
    
    with open(LEDGER_FILE) as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
                if event['type'] == 'SETTLEMENT':
                    event['line_no'] = line_no
                    settlements.append(event)
            except json.JSONDecodeError:
                continue
    
    print(f"Found {len(settlements)} settlement events")
    
    # Group by ticker
    by_ticker = defaultdict(list)
    for s in settlements:
        ticker = s['market_ticker']
        by_ticker[ticker].append(s)
    
    # Analyze patterns
    print(f"\nSettlement patterns:")
    multi_settlement_tickers = []
    for ticker, sett_list in by_ticker.items():
        if len(sett_list) > 1:
            multi_settlement_tickers.append((ticker, len(sett_list)))
    
    multi_settlement_tickers.sort(key=lambda x: x[1], reverse=True)
    print(f"Tickers with multiple settlements: {len(multi_settlement_tickers)}")
    
    # Show examples
    print(f"\nTop 5 tickers with most settlements:")
    for ticker, count in multi_settlement_tickers[:5]:
        print(f"  {ticker}: {count} settlements")
        sett_list = by_ticker[ticker]
        for i, s in enumerate(sett_list):
            payout = s.get('payout_cents', 0)
            result = s.get('result', 'UNKNOWN')
            fee = s.get('settle_fee_cents', 0)
            ts = s.get('ts', 'NO_TS')
            print(f"    [{i+1}] {result} payout={payout}¢ fee={fee}¢ ts={ts} line={s['line_no']}")
        print()
    
    # Check for exact duplicates
    print(f"Checking for exact duplicates...")
    duplicates_found = 0
    for ticker, sett_list in by_ticker.items():
        if len(sett_list) <= 1:
            continue
        
        for i, s1 in enumerate(sett_list):
            for j, s2 in enumerate(sett_list[i+1:], i+1):
                # Compare key fields
                if (s1.get('result') == s2.get('result') and
                    s1.get('payout_cents') == s2.get('payout_cents') and
                    s1.get('settle_fee_cents') == s2.get('settle_fee_cents') and
                    s1.get('ts') == s2.get('ts')):
                    print(f"  DUPLICATE: {ticker} lines {s1['line_no']} and {s2['line_no']}")
                    duplicates_found += 1
    
    print(f"Exact duplicates found: {duplicates_found}")
    
    # Analyze settlement revenue
    print(f"\nSettlement revenue analysis:")
    total_revenue = sum(s.get('payout_cents', 0) for s in settlements)
    total_fees = sum(s.get('settle_fee_cents', 0) for s in settlements)
    
    print(f"  Total settlement revenue: {total_revenue}¢ = ${total_revenue/100:.2f}")
    print(f"  Total settlement fees: {total_fees}¢ = ${total_fees/100:.2f}")
    
    # Check results distribution
    result_counts = defaultdict(int)
    for s in settlements:
        result_counts[s.get('result', 'UNKNOWN')] += 1
    
    print(f"\nResults distribution:")
    for result, count in sorted(result_counts.items()):
        print(f"  {result}: {count}")
    
    return by_ticker

def check_settlement_logic():
    """Check if settlements follow expected logic"""
    print(f"\n" + "="*50)
    print(f"CHECKING SETTLEMENT LOGIC")
    print(f"="*50)
    
    by_ticker = investigate_duplicate_settlements()
    
    # Find a ticker with multiple settlements to analyze
    for ticker, sett_list in by_ticker.items():
        if len(sett_list) > 1:
            print(f"\nAnalyzing {ticker}:")
            for s in sett_list:
                result = s.get('result', 'UNKNOWN')
                payout = s.get('payout_cents', 0)
                fee = s.get('settle_fee_cents', 0)
                ts = s.get('ts', 'NO_TS')
                print(f"  {result} → {payout}¢ (fee: {fee}¢) at {ts}")
            break
    
    # The key question: should we be summing all settlements or taking the last one?
    print(f"\nKEY QUESTION:")
    print(f"Are these legitimate multiple settlements, or duplicates/corrections?")
    print(f"If legitimate: total settlement revenue is correct")  
    print(f"If duplicates: we're overcounting by factor of ~5")

if __name__ == "__main__":
    check_settlement_logic()