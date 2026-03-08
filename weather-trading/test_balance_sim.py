#!/usr/bin/env python3
"""
Test balance simulation using the balance_sim.py approach with our API fills data.
"""

import json
from datetime import datetime, timezone

class BalanceSimulator:
    """Copy of balance_sim.py approach - tracks actual cash flow."""
    
    def __init__(self, starting_balance_cents=0):
        self.balance = starting_balance_cents
        self.total_fill_fees = 0
        self.total_settlement_fees = 0
        self.fill_count = 0
        self.settlement_count = 0
    
    def apply_fill(self, side, action, qty, yes_price, no_price, fee_cents=0):
        """Apply a single fill using balance_sim logic."""
        self.fill_count += 1
        
        side = side.lower()
        action = action.lower()
        
        # Always deduct fees
        self.balance -= fee_cents
        self.total_fill_fees += fee_cents
        
        if action == 'buy':
            # Buy: debit the side's price
            price = yes_price if side == 'yes' else no_price
            self.balance -= price * qty
        else:
            # Sell: credit the OTHER side's price
            price = no_price if side == 'yes' else yes_price
            self.balance += price * qty
    
    def apply_settlement_payout(self, payout_per_contract_cents, position_size, fee_cents=0):
        """Apply settlement using per-contract payout and position size."""
        self.settlement_count += 1
        
        # Receive the payout
        self.balance += payout_per_contract_cents * position_size
        
        # Pay settlement fees
        self.balance -= fee_cents
        self.total_settlement_fees += fee_cents

def get_position_at_settlement(ticker, api_fills, settlement_ts):
    """Calculate position size just before settlement for a ticker."""
    # This is a simplified version - in reality we'd need to track positions properly
    # For now, let's assume we need to look at the settlement payout directly
    return 0  # Placeholder

def main():
    print("=== Balance Simulation Test ===\n")
    
    starting_capital_cents = round(510.76 * 100)
    actual_cash_cents = round(187.54 * 100)
    
    # Load API fills
    with open("kalshi_fills_complete.json", 'r') as f:
        api_fills = json.load(f)
    
    # Load settlements
    settlements = []
    with open("ledger_canonical.jsonl", 'r') as f:
        for line in f:
            event = json.loads(line)
            if event.get("type") == "SETTLEMENT":
                settlements.append(event)
    
    # Create simulator
    sim = BalanceSimulator(starting_capital_cents)
    
    print(f"Starting balance: ${starting_capital_cents/100:.2f}")
    
    # Apply all fills
    print(f"Applying {len(api_fills)} fills...")
    for fill in api_fills:
        side = fill.get("side", "").upper()
        action = fill.get("action", "").upper()
        qty = fill.get("count", 0)
        yes_price = fill.get("yes_price", 0)
        no_price = fill.get("no_price", 0)
        fee_cents = round(float(fill.get("fee_cost", 0)) * 100)
        
        sim.apply_fill(side, action, qty, yes_price, no_price, fee_cents)
    
    balance_after_fills = sim.balance
    print(f"Balance after fills: ${balance_after_fills/100:.2f}")
    print(f"Fill fees paid: ${sim.total_fill_fees/100:.2f}")
    
    # For settlements, we need to handle them differently than the FIFO engine
    # The balance_sim.py approach needs actual settlement revenue/payout data
    # Since we don't have easy access to position sizes at settlement time,
    # let's see what the gap would be without settlements
    
    print(f"\n=== Results (fills only) ===")
    print(f"Expected balance after fills: ${balance_after_fills/100:.2f}")
    print(f"Actual balance: ${actual_cash_cents/100:.2f}")
    print(f"Gap (before settlements): ${(actual_cash_cents - balance_after_fills)/100:.2f}")
    
    # The gap here should be mostly from settlement payouts
    # Settlement revenue should be around the total payout from winning positions
    
    return 0

if __name__ == "__main__":
    exit(main())