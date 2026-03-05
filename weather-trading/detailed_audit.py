#!/usr/bin/env python3
"""
Detailed Weather Trading System Audit
Comprehensive P&L reconciliation focusing on the gaps
"""

import json
import sys
import os
from collections import defaultdict, Counter
import csv

sys.path.append('/home/ubuntu/.openclaw/workspace/weather-trading')

def load_data():
    """Load all data sources"""
    # Load journal
    with open('/home/ubuntu/.openclaw/workspace/weather-trading/trade_journal.json', 'r') as f:
        journal = json.load(f)
    
    # Load cached Kalshi fills
    with open('/tmp/kalshi_fills_all.json', 'r') as f:
        fills_data = json.load(f)
        fills = fills_data if isinstance(fills_data, list) else fills_data.get('fills', [])
    
    # Load cached Kalshi settlements  
    with open('/tmp/kalshi_settlements.json', 'r') as f:
        settlements_data = json.load(f)
        settlements = settlements_data if isinstance(settlements_data, list) else settlements_data.get('settlements', [])
    
    return journal, fills, settlements

def analyze_journal_pnl(journal):
    """Analyze claimed P&L from the trade journal"""
    total_claimed_pnl = 0
    settled_count = 0
    entry_count = 0
    exit_count = 0
    pnl_by_status = defaultdict(list)
    
    for entry in journal:
        action = entry.get('action', '')
        if action == 'ENTRY':
            entry_count += 1
        elif 'EXIT' in action:
            exit_count += 1
            
        # Check if this entry has settled P&L
        if entry.get('pnl_cents') is not None:
            pnl = entry.get('pnl_cents', 0)
            total_claimed_pnl += pnl
            settled_count += 1
            status = entry.get('status', 'unknown')
            pnl_by_status[status].append(pnl)
    
    return {
        'total_claimed_pnl_cents': total_claimed_pnl,
        'total_claimed_pnl_dollars': total_claimed_pnl / 100,
        'settled_count': settled_count,
        'entry_count': entry_count,
        'exit_count': exit_count,
        'pnl_by_status': dict(pnl_by_status)
    }

def analyze_kalshi_cashflow(fills, settlements):
    """Calculate actual cashflow from Kalshi API data"""
    total_spent = 0  # What we paid out (buys + fees)
    total_received = 0  # What we got (sells + settlements - fees)
    total_fees = 0
    
    buy_count = 0
    sell_count = 0
    
    # Process fills
    for fill in fills:
        action = fill.get('action')
        side = fill.get('side')
        yes_price = fill.get('yes_price', 0)
        count = fill.get('count', 0)
        fee_cost_str = fill.get('fee_cost', '0')
        fee_cost = float(fee_cost_str) * 100 if fee_cost_str else 0
        
        total_fees += fee_cost
        
        if action == 'buy':
            buy_count += 1
            if side == 'yes':
                cost = yes_price * count
            else:  # side == 'no'
                cost = (100 - yes_price) * count
            total_spent += cost + fee_cost
            
        elif action == 'sell':
            sell_count += 1
            if side == 'yes':
                proceeds = yes_price * count
            else:  # side == 'no'  
                proceeds = (100 - yes_price) * count
            total_received += proceeds - fee_cost  # Fees reduce what we get
    
    # Process settlements
    settlement_revenue = 0
    for settlement in settlements:
        revenue = settlement.get('revenue', 0)
        settlement_revenue += revenue
    
    total_received += settlement_revenue
    
    net_cashflow = total_received - total_spent
    
    return {
        'total_spent_cents': total_spent,
        'total_received_cents': total_received,
        'settlement_revenue_cents': settlement_revenue,
        'total_fees_cents': total_fees,
        'net_cashflow_cents': net_cashflow,
        'net_cashflow_dollars': net_cashflow / 100,
        'buy_count': buy_count,
        'sell_count': sell_count,
        'settlement_count': len(settlements)
    }

def analyze_exit_blown_properly(journal):
    """Proper analysis of EXIT_BLOWN entries to see if they were actually 'dead'"""
    exit_blown_entries = []
    
    for i, entry in enumerate(journal):
        if entry.get('action') == 'EXIT_BLOWN':
            # Find the corresponding ENTRY
            ticker = entry.get('ticker')
            entry_data = None
            for j in range(i-1, -1, -1):
                if (journal[j].get('ticker') == ticker and 
                    journal[j].get('action') == 'ENTRY'):
                    entry_data = journal[j]
                    break
            
            analysis = {
                'ticker': ticker,
                'exit_time': entry.get('ts'),
                'exit_reason': entry.get('reasoning', ''),
                'entry_found': entry_data is not None
            }
            
            if entry_data:
                # Extract strike and direction info
                direction = entry_data.get('direction', 'unknown')
                analysis['direction'] = direction
                analysis['entry_time'] = entry_data.get('ts')
                
                # Check if position was settled and what the result was
                if entry.get('actual_temp') is not None:
                    analysis['actual_temp'] = entry.get('actual_temp')
                    analysis['status'] = entry.get('status', 'unknown')
                    analysis['pnl_cents'] = entry.get('pnl_cents', 0)
            
            exit_blown_entries.append(analysis)
    
    return exit_blown_entries

def compare_journal_vs_kalshi_by_ticker(journal, fills):
    """Compare journal P&L vs Kalshi cashflow on a per-ticker basis"""
    # Group journal entries by ticker
    journal_by_ticker = defaultdict(list)
    for entry in journal:
        ticker = entry.get('ticker')
        if ticker:
            journal_by_ticker[ticker].append(entry)
    
    # Group fills by ticker  
    fills_by_ticker = defaultdict(list)
    for fill in fills:
        ticker = fill.get('ticker')
        if ticker:
            fills_by_ticker[ticker].append(fill)
    
    # Compare each ticker
    comparisons = []
    for ticker in set(journal_by_ticker.keys()) | set(fills_by_ticker.keys()):
        journal_entries = journal_by_ticker.get(ticker, [])
        ticker_fills = fills_by_ticker.get(ticker, [])
        
        # Calculate journal P&L for this ticker
        journal_pnl = sum(e.get('pnl_cents', 0) for e in journal_entries if e.get('pnl_cents') is not None)
        
        # Calculate Kalshi cashflow for this ticker
        kalshi_net = 0
        for fill in ticker_fills:
            action = fill.get('action')
            side = fill.get('side') 
            yes_price = fill.get('yes_price', 0)
            count = fill.get('count', 0)
            fee_cost_str = fill.get('fee_cost', '0')
            fee_cost = float(fee_cost_str) * 100 if fee_cost_str else 0
            
            if action == 'buy':
                if side == 'yes':
                    kalshi_net -= (yes_price * count + fee_cost)
                else:
                    kalshi_net -= ((100 - yes_price) * count + fee_cost)
            elif action == 'sell':
                if side == 'yes':
                    kalshi_net += (yes_price * count - fee_cost)
                else:
                    kalshi_net += ((100 - yes_price) * count - fee_cost)
        
        comparison = {
            'ticker': ticker,
            'journal_entries': len(journal_entries),
            'kalshi_fills': len(ticker_fills),
            'journal_pnl_cents': journal_pnl,
            'kalshi_net_cents': kalshi_net,
            'difference_cents': journal_pnl - kalshi_net,
            'difference_dollars': (journal_pnl - kalshi_net) / 100
        }
        
        comparisons.append(comparison)
    
    # Sort by biggest discrepancies
    comparisons.sort(key=lambda x: abs(x['difference_cents']), reverse=True)
    
    return comparisons

def main():
    print("=== DETAILED WEATHER TRADING AUDIT ===")
    
    # Load data
    print("Loading data...")
    journal, fills, settlements = load_data()
    print(f"Loaded: {len(journal)} journal entries, {len(fills)} fills, {len(settlements)} settlements")
    
    # Analyze journal claimed P&L
    print("\n=== JOURNAL P&L ANALYSIS ===")
    journal_analysis = analyze_journal_pnl(journal)
    print(f"Total claimed P&L: ${journal_analysis['total_claimed_pnl_dollars']:.2f}")
    print(f"Settled entries: {journal_analysis['settled_count']}")
    print(f"Total entries: {journal_analysis['entry_count']}")
    print(f"Total exits: {journal_analysis['exit_count']}")
    print("P&L by status:", {k: f"${sum(v)/100:.2f}" for k, v in journal_analysis['pnl_by_status'].items()})
    
    # Analyze Kalshi cashflow
    print("\n=== KALSHI CASHFLOW ANALYSIS ===")
    kalshi_analysis = analyze_kalshi_cashflow(fills, settlements)
    print(f"Net cashflow: ${kalshi_analysis['net_cashflow_dollars']:.2f}")
    print(f"Total spent: ${kalshi_analysis['total_spent_cents']/100:.2f}")
    print(f"Total received: ${kalshi_analysis['total_received_cents']/100:.2f}")
    print(f"Settlement revenue: ${kalshi_analysis['settlement_revenue_cents']/100:.2f}")
    print(f"Total fees: ${kalshi_analysis['total_fees_cents']/100:.2f}")
    print(f"Buys: {kalshi_analysis['buy_count']}, Sells: {kalshi_analysis['sell_count']}")
    
    # Compare the two
    print("\n=== COMPARISON ===")
    gap = kalshi_analysis['net_cashflow_dollars'] - journal_analysis['total_claimed_pnl_dollars']
    print(f"Journal claims: ${journal_analysis['total_claimed_pnl_dollars']:.2f}")
    print(f"Kalshi shows: ${kalshi_analysis['net_cashflow_dollars']:.2f}")
    print(f"Gap: ${gap:.2f}")
    
    # Account balance check
    print(f"\nAccount balance change: $-323.22 (from $510.76 to $187.54)")
    expected_from_kalshi = 510.76 + kalshi_analysis['net_cashflow_dollars']
    print(f"Expected balance from Kalshi: ${expected_from_kalshi:.2f}")
    print(f"Actual balance: $187.54")
    print(f"Balance difference: ${187.54 - expected_from_kalshi:.2f}")
    
    # EXIT_BLOWN analysis  
    print("\n=== EXIT_BLOWN ANALYSIS ===")
    exit_blown = analyze_exit_blown_properly(journal)
    print(f"Total EXIT_BLOWN: {len(exit_blown)}")
    
    # Show some examples
    verified_dead = 0
    for eb in exit_blown[:10]:  # First 10
        if eb.get('actual_temp') is not None and eb.get('pnl_cents') is not None:
            if eb['pnl_cents'] <= 0:
                verified_dead += 1
                
    print(f"Sample of 10 EXIT_BLOWN entries show {verified_dead} were actually losing positions")
    
    # Ticker-by-ticker analysis  
    print("\n=== TICKER COMPARISON (Top 10 Discrepancies) ===")
    ticker_comparisons = compare_journal_vs_kalshi_by_ticker(journal, fills)
    
    for i, comp in enumerate(ticker_comparisons[:10]):
        print(f"{i+1}. {comp['ticker']}")
        print(f"   Journal P&L: ${comp['journal_pnl_cents']/100:.2f}, Kalshi: ${comp['kalshi_net_cents']/100:.2f}")  
        print(f"   Difference: ${comp['difference_dollars']:.2f}")
        print(f"   Entries: {comp['journal_entries']}, Fills: {comp['kalshi_fills']}")
    
    # Save detailed results
    results = {
        'journal_analysis': journal_analysis,
        'kalshi_analysis': kalshi_analysis,
        'gap_dollars': gap,
        'exit_blown_analysis': exit_blown,
        'ticker_comparisons': ticker_comparisons[:20]
    }
    
    with open('/home/ubuntu/.openclaw/workspace/weather-trading/detailed_audit_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nDetailed results saved to detailed_audit_results.json")

if __name__ == "__main__":
    main()