#!/usr/bin/env python3
"""
Test Canonical Ledger Replay

Replay the canonical ledger through the FIFO engine to see if it closes the reconciliation gap.
"""

import json
import os
from ledger import LotEngine

def test_replay():
    print("=== FIFO Engine Reconciliation Test ===\n")
    
    # Test both old and new ledgers
    old_ledger = "ledger.jsonl.backup"
    new_ledger = "ledger_canonical.jsonl"
    
    # Known actual Kalshi balance
    starting_capital = 510.76
    actual_cash = 187.54
    actual_portfolio = 0.00  # No open positions
    actual_total = actual_cash + actual_portfolio
    actual_pnl = actual_total - starting_capital
    
    print(f"Kalshi Account Status:")
    print(f"  Starting capital: ${starting_capital:.2f}")
    print(f"  Current cash: ${actual_cash:.2f}")
    print(f"  Portfolio value: ${actual_portfolio:.2f}")
    print(f"  Total value: ${actual_total:.2f}")
    print(f"  Actual P&L: ${actual_pnl:.2f}")
    print()
    
    for ledger_name, ledger_path in [("OLD (duplicated)", old_ledger), ("NEW (canonical)", new_ledger)]:
        if not os.path.exists(ledger_path):
            print(f"ERROR: {ledger_path} not found")
            continue
            
        print(f"=== {ledger_name} LEDGER REPLAY ===")
        
        # Create fresh engine
        engine = LotEngine()
        engine.rebuild_from_ledger(ledger_path)
        
        # Get results
        total_realized = engine.total_realized_pnl() / 100  # Convert cents to dollars
        total_fees = engine.total_fees_paid() / 100
        open_positions = engine.open_positions()
        
        # Calculate what the balance should be
        expected_cash = starting_capital + total_realized - total_fees
        
        print(f"  Total realized P&L: ${total_realized:.2f}")
        print(f"  Total fees paid: ${total_fees:.2f}")
        print(f"  Open positions: {len(open_positions)} markets")
        print(f"  Expected cash balance: ${expected_cash:.2f}")
        print(f"  Actual cash balance: ${actual_cash:.2f}")
        print(f"  Reconciliation gap: ${actual_cash - expected_cash:.2f}")
        
        # Show the gap as percentage
        gap_pct = abs(actual_cash - expected_cash) / starting_capital * 100
        print(f"  Gap as % of capital: {gap_pct:.2f}%")
        
        if open_positions:
            print(f"  Open position details:")
            for ticker, pos in list(open_positions.items())[:5]:  # Show first 5
                yes_qty = pos.get('YES', 0)
                no_qty = pos.get('NO', 0)
                if yes_qty or no_qty:
                    print(f"    {ticker}: YES={yes_qty}, NO={no_qty}")
            if len(open_positions) > 5:
                print(f"    ... and {len(open_positions) - 5} more")
        
        print()
    
    return 0

if __name__ == "__main__":
    exit(test_replay())