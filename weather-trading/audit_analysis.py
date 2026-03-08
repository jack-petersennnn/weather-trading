#!/usr/bin/env python3
"""
Reconciliation Gap Audit

Systematically analyze the ledger to find the root cause of the $483 gap.
"""

import json
import os
from collections import defaultdict
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LEDGER_FILE = os.path.join(BASE_DIR, "ledger.jsonl")

def parse_timestamp(ts):
    """Parse various timestamp formats"""
    if isinstance(ts, (int, float)):
        return ts
    if isinstance(ts, str):
        try:
            # ISO format
            if 'T' in ts:
                dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                return dt.timestamp()
            else:
                return float(ts)
        except:
            return 0
    return 0

def audit_events():
    """Audit all events in the ledger"""
    fills = []
    settlements = []
    
    print("Reading ledger.jsonl...")
    with open(LEDGER_FILE) as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
                ts = parse_timestamp(event.get('ts'))
                event['ts_parsed'] = ts
                event['line_no'] = line_no
                
                if event['type'] == 'FILL':
                    fills.append(event)
                elif event['type'] == 'SETTLEMENT':
                    settlements.append(event)
            except json.JSONDecodeError:
                print(f"  WARNING: Invalid JSON on line {line_no}")
    
    # Sort by timestamp
    fills.sort(key=lambda x: x['ts_parsed'])
    settlements.sort(key=lambda x: x['ts_parsed'])
    
    print(f"\nEvent Summary:")
    print(f"  Fills: {len(fills)}")
    print(f"  Settlements: {len(settlements)}")
    
    if fills:
        first_fill_ts = datetime.fromtimestamp(fills[0]['ts_parsed'], tz=timezone.utc)
        last_fill_ts = datetime.fromtimestamp(fills[-1]['ts_parsed'], tz=timezone.utc)
        print(f"  First fill: {first_fill_ts.isoformat()}")
        print(f"  Last fill: {last_fill_ts.isoformat()}")
    
    if settlements:
        first_sett_ts = datetime.fromtimestamp(settlements[0]['ts_parsed'], tz=timezone.utc)
        last_sett_ts = datetime.fromtimestamp(settlements[-1]['ts_parsed'], tz=timezone.utc)
        print(f"  First settlement: {first_sett_ts.isoformat()}")
        print(f"  Last settlement: {last_sett_ts.isoformat()}")
    
    return fills, settlements

def calculate_balance_simulation(fills, settlements):
    """Simulate balance using the simple stock model"""
    balance = 51076  # Starting balance in cents
    balance_history = [(0, 'START', 51076)]
    
    # Create chronological event list
    events = []
    for f in fills:
        events.append((f['ts_parsed'], 'FILL', f))
    for s in settlements:
        events.append((s['ts_parsed'], 'SETTLEMENT', s))
    
    events.sort(key=lambda x: x[0])
    
    total_fill_fees = 0
    total_settlement_fees = 0
    total_buy_cost = 0
    total_sell_revenue = 0
    total_settlement_revenue = 0
    
    for ts, event_type, data in events:
        if event_type == 'FILL':
            side = data['side'].upper()
            direction = data['dir'].upper()
            qty = data['qty']
            price_cents = data['price_cents']
            fee_cents = data.get('fee_cents', 0)
            
            balance -= fee_cents
            total_fill_fees += fee_cents
            
            # Simple stock model: BUY = debit side price, SELL = credit side price
            if direction == 'BUY':
                balance -= price_cents * qty
                total_buy_cost += price_cents * qty
            else:  # SELL
                balance += price_cents * qty
                total_sell_revenue += price_cents * qty
                
        elif event_type == 'SETTLEMENT':
            payout_cents = data.get('payout_cents', 0)
            if 'result' in data:
                # Try to parse settlement result
                result = data['result'].upper()
                if result == 'YES':
                    # Settlement logic is complex - just use payout if available
                    pass
            
            settle_fee_cents = data.get('settle_fee_cents', 0)
            balance += payout_cents
            balance -= settle_fee_cents
            total_settlement_revenue += payout_cents
            total_settlement_fees += settle_fee_cents
    
    return {
        'final_balance': balance,
        'total_fill_fees': total_fill_fees,
        'total_settlement_fees': total_settlement_fees,
        'total_buy_cost': total_buy_cost,
        'total_sell_revenue': total_sell_revenue,
        'total_settlement_revenue': total_settlement_revenue,
        'net_trading_flow': total_sell_revenue - total_buy_cost,
        'total_fees': total_fill_fees + total_settlement_fees,
    }

def audit_settlement_revenue(settlements):
    """Audit settlement revenue vs FIFO engine expectations"""
    total_settlement_revenue = 0
    settlement_by_ticker = defaultdict(list)
    
    for s in settlements:
        ticker = s['market_ticker']
        payout_cents = s.get('payout_cents', 0)
        result = s.get('result', 'UNKNOWN')
        fee_cents = s.get('settle_fee_cents', 0)
        
        settlement_by_ticker[ticker].append({
            'payout': payout_cents,
            'result': result,
            'fee': fee_cents,
            'ts': s['ts_parsed']
        })
        total_settlement_revenue += payout_cents
    
    print(f"\nSettlement Analysis:")
    print(f"  Total settlement revenue: {total_settlement_revenue}¢ = ${total_settlement_revenue/100:.2f}")
    print(f"  Markets settled: {len(settlement_by_ticker)}")
    
    # Find markets with unusual settlement patterns
    unusual_settlements = []
    for ticker, settlements_list in settlement_by_ticker.items():
        if len(settlements_list) > 1:
            unusual_settlements.append((ticker, len(settlements_list)))
    
    if unusual_settlements:
        print(f"  Markets with multiple settlements: {len(unusual_settlements)}")
        for ticker, count in sorted(unusual_settlements, key=lambda x: x[1], reverse=True)[:5]:
            print(f"    {ticker}: {count} settlements")
    
    return total_settlement_revenue

def main():
    print("🔍 RECONCILIATION GAP AUDIT")
    print("="*50)
    
    # Known values
    starting_balance = 51076  # $510.76
    actual_current_balance = 18754  # $187.54
    expected_pnl = actual_current_balance - starting_balance  # -$323.22
    
    print(f"Known Values:")
    print(f"  Starting balance: {starting_balance}¢ = ${starting_balance/100:.2f}")
    print(f"  Actual balance: {actual_current_balance}¢ = ${actual_current_balance/100:.2f}")
    print(f"  Expected P&L: {expected_pnl}¢ = ${expected_pnl/100:.2f}")
    
    # Audit ledger events
    fills, settlements = audit_events()
    
    # Audit settlement revenue
    settlement_revenue = audit_settlement_revenue(settlements)
    
    # Run balance simulation
    sim_results = calculate_balance_simulation(fills, settlements)
    
    print(f"\nBalance Simulation Results:")
    print(f"  Simulated final balance: {sim_results['final_balance']}¢ = ${sim_results['final_balance']/100:.2f}")
    print(f"  Gap vs actual: {sim_results['final_balance'] - actual_current_balance}¢ = ${(sim_results['final_balance'] - actual_current_balance)/100:.2f}")
    
    print(f"\nCash Flow Breakdown:")
    print(f"  Total buy cost: {sim_results['total_buy_cost']}¢ = ${sim_results['total_buy_cost']/100:.2f}")
    print(f"  Total sell revenue: {sim_results['total_sell_revenue']}¢ = ${sim_results['total_sell_revenue']/100:.2f}")
    print(f"  Net trading flow: {sim_results['net_trading_flow']}¢ = ${sim_results['net_trading_flow']/100:.2f}")
    print(f"  Settlement revenue: {sim_results['total_settlement_revenue']}¢ = ${sim_results['total_settlement_revenue']/100:.2f}")
    print(f"  Total fees: {sim_results['total_fees']}¢ = ${sim_results['total_fees']/100:.2f}")
    
    # FIFO engine comparison
    print(f"\nFIFO Engine Comparison:")
    # Rebuild FIFO to get current values
    import ledger as ledger_module
    engine = ledger_module.LotEngine()
    engine.rebuild_from_ledger()
    
    fifo_pnl = engine.total_realized_pnl()
    fifo_fees = engine.total_fees_paid()
    
    print(f"  FIFO realized P&L: {fifo_pnl}¢ = ${fifo_pnl/100:.2f}")
    print(f"  FIFO total fees: {fifo_fees}¢ = ${fifo_fees/100:.2f}")
    print(f"  FIFO vs expected P&L gap: {fifo_pnl - expected_pnl}¢ = ${(fifo_pnl - expected_pnl)/100:.2f}")
    
    # Verdict
    print(f"\n" + "="*50)
    print(f"AUDIT VERDICT:")
    
    gap_cents = abs(fifo_pnl - expected_pnl)
    if gap_cents > 500:  # $5 tolerance
        print(f"❌ SIGNIFICANT GAP: {gap_cents}¢ = ${gap_cents/100:.2f}")
        if fifo_pnl < expected_pnl:
            print(f"   FIFO engine is TOO NEGATIVE by ${gap_cents/100:.2f}")
            print(f"   Likely causes:")
            print(f"   - Missing settlement revenue")
            print(f"   - Incorrect fill processing")
            print(f"   - Missing collateral returns")
        else:
            print(f"   FIFO engine is TOO POSITIVE by ${gap_cents/100:.2f}")
            print(f"   Likely causes:")
            print(f"   - Missing fills/costs")
            print(f"   - Incorrect fee accounting")
    else:
        print(f"✅ GAP WITHIN TOLERANCE: {gap_cents}¢ = ${gap_cents/100:.2f}")
    
    print(f"\nRECOMMENDATION:")
    if abs(sim_results['final_balance'] - actual_current_balance) < gap_cents:
        print(f"   The simple stock model is more accurate than FIFO engine.")
        print(f"   FIFO engine appears to have accounting bugs.")
    else:
        print(f"   Both models have significant gaps.")
        print(f"   Missing historical events likely.")

if __name__ == "__main__":
    main()