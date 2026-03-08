#!/usr/bin/env python3
"""
Debug pricing differences between balance_sim and FIFO engine approaches.
"""

import json
from ledger import normalize_fill

def main():
    print("=== Debug Pricing Logic ===\n")
    
    # Load a few fills and compare pricing approaches
    api_fills = []
    with open("kalshi_fills_complete.json", 'r') as f:
        all_fills = json.load(f)
        api_fills = all_fills[:5]  # Just first 5 for debugging
    
    print("Comparing pricing approaches for sample fills:\n")
    
    total_balance_sim_effect = 0
    total_fifo_cost = 0
    
    for i, fill in enumerate(api_fills):
        print(f"Fill {i+1}: {fill.get('ticker', 'NO_TICKER')}")
        
        # API fill data
        side = fill.get('side', '').upper()
        action = fill.get('action', '').upper()
        qty = fill.get('count', 0)
        yes_price = fill.get('yes_price', 0)
        no_price = fill.get('no_price', 0)
        fee_cents = round(float(fill.get('fee_cost', 0)) * 100)
        
        print(f"  Side: {side}, Action: {action}, Qty: {qty}")
        print(f"  Yes price: {yes_price}¢, No price: {no_price}¢")
        print(f"  Fee: {fee_cents}¢")
        
        # Balance sim approach (from balance_sim.py)
        if action == 'BUY':
            price = yes_price if side == 'YES' else no_price
            balance_effect = -(price * qty + fee_cents)  # Debit
            print(f"  BalanceSim: BUY {side} at {price}¢ → -{price * qty}¢ - {fee_cents}¢ = {balance_effect}¢")
        else:  # SELL
            price = no_price if side == 'YES' else yes_price  # OTHER side price
            balance_effect = price * qty - fee_cents  # Credit
            print(f"  BalanceSim: SELL {side} (credit {price}¢) → +{price * qty}¢ - {fee_cents}¢ = {balance_effect}¢")
        
        total_balance_sim_effect += balance_effect
        
        # FIFO engine approach
        # First normalize the fill
        direction = "BUY" if action == "BUY" else "SELL"
        fifo_side = side
        fifo_qty = qty
        if side == "YES":
            fifo_price = yes_price
        else:
            fifo_price = no_price
            
        buy_side, norm_qty, eff_price = normalize_fill(fifo_side, direction, fifo_qty, fifo_price)
        
        fifo_cost = eff_price * norm_qty + fee_cents  # Always a cost in FIFO (buy equivalent)
        total_fifo_cost += fifo_cost
        
        print(f"  FIFO: {direction} {fifo_side} → BUY {buy_side} at {eff_price}¢")
        print(f"  FIFO cost: {eff_price} × {norm_qty} + {fee_cents} = {fifo_cost}¢")
        
        print()
    
    print(f"TOTALS for {len(api_fills)} fills:")
    print(f"  BalanceSim total effect: {total_balance_sim_effect}¢")
    print(f"  FIFO total cost: {total_fifo_cost}¢")
    print(f"  Difference: {total_fifo_cost - (-total_balance_sim_effect)}¢")
    
    return 0

if __name__ == "__main__":
    exit(main())