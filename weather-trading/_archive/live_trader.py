#!/usr/bin/env python3
"""
Live Trade Engine — Real Money on Kalshi
Wraps the paper_trader logic but places REAL orders via kalshi_client.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

# Add parent to path
sys.path.insert(0, os.path.dirname(__file__))
import kalshi_client
import paper_trader

TRADES_FILE = paper_trader.TRADES_FILE
LOG_FILE = os.path.join(os.path.dirname(__file__), "live_trade_log.json")


def log_event(event):
    """Append event to live trade log."""
    event["logged_at"] = datetime.now(timezone.utc).isoformat()
    logs = []
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE) as f:
                logs = json.load(f)
        except:
            logs = []
    logs.append(event)
    with open(LOG_FILE, "w") as f:
        json.dump(logs, f, indent=2)


def get_real_balance():
    """Get actual Kalshi balance."""
    bal = kalshi_client.get_balance()
    return bal["balance"]  # cents


def place_real_order(trade_dict):
    """
    Place a real order on Kalshi based on the trade dict from paper_trader.
    Returns (success, order_response, error_msg).
    """
    ticker = trade_dict["ticker"]
    direction = trade_dict["direction"].lower()  # "yes" or "no"
    contracts = trade_dict["contracts"]
    entry_price_cents = trade_dict["entry_price_cents"]
    
    try:
        # Place limit order
        result = kalshi_client.place_order(
            ticker=ticker,
            side=direction,
            contracts=contracts,
            price_cents=entry_price_cents,
            order_type="limit"
        )
        
        log_event({
            "type": "order_placed",
            "ticker": ticker,
            "side": direction,
            "contracts": contracts,
            "price_cents": entry_price_cents,
            "cost_cents": trade_dict["cost_cents"],
            "response": result,
        })
        
        return True, result, None
        
    except Exception as e:
        error_msg = str(e)
        log_event({
            "type": "order_failed",
            "ticker": ticker,
            "side": direction,
            "contracts": contracts,
            "price_cents": entry_price_cents,
            "error": error_msg,
        })
        return False, None, error_msg


def run():
    """
    Run the live trader:
    1. Run paper_trader logic to identify trades
    2. For each new trade, place a REAL order on Kalshi
    3. Update trades.json with order IDs and real status
    """
    print("╔══════════════════════════════════════════════╗")
    print("║  KingClaw LIVE Trader v1.0 — REAL MONEY 💰   ║")
    print("╚══════════════════════════════════════════════╝")
    
    # Check real balance first
    try:
        balance_cents = get_real_balance()
        print(f"\n  💰 Kalshi Balance: ${balance_cents/100:.2f}")
    except Exception as e:
        print(f"\n  ❌ Cannot connect to Kalshi: {e}")
        print("  Aborting — not placing trades without confirmed balance.")
        return []
    
    # Load current trades
    trades_data = paper_trader.load_trades()
    existing_count = len(trades_data["trades"])
    
    # Run paper_trader logic (this identifies opportunities and appends to trades_data)
    paper_trader.run()
    
    # Reload to see new trades
    trades_data = paper_trader.load_trades()
    new_trades = trades_data["trades"][existing_count:]
    
    if not new_trades:
        print("\n  No new trades identified.")
        return []
    
    print(f"\n  📋 {len(new_trades)} new trades to place on Kalshi...")
    
    placed = []
    failed = []
    
    for trade in new_trades:
        ticker = trade["ticker"]
        direction = trade["direction"]
        contracts = trade["contracts"]
        entry = trade["entry_price_cents"]
        cost = trade["cost_cents"]
        
        # Safety check: don't exceed balance
        if cost > balance_cents:
            print(f"  ⚠️  SKIP {ticker}: cost ${cost/100:.2f} > balance ${balance_cents/100:.2f}")
            trade["status"] = "skipped"
            trade["skip_reason"] = "insufficient balance"
            failed.append(trade)
            continue
        
        print(f"\n  🔄 Placing: {direction} {ticker} @ {entry}¢ × {contracts} (${cost/100:.2f})")
        
        success, result, error = place_real_order(trade)
        
        if success:
            order = result.get("order", result)
            trade["order_id"] = order.get("order_id", "")
            trade["order_status"] = order.get("status", "")
            trade["filled_count"] = order.get("fill_count", order.get("filled_count", 0))
            trade["remaining_count"] = order.get("remaining_count", contracts)
            trade["taker_fill_cost"] = order.get("taker_fill_cost", 0)
            trade["maker_fill_cost"] = order.get("maker_fill_cost", 0)
            trade["taker_fees"] = order.get("taker_fees", 0)
            trade["mode"] = "LIVE"
            balance_cents -= cost
            placed.append(trade)
            print(f"  ✅ ORDER PLACED: {trade['order_id']} | filled={trade['filled_count']}/{contracts}")
        else:
            trade["status"] = "order_failed"
            trade["error"] = error
            trade["mode"] = "LIVE"
            failed.append(trade)
            print(f"  ❌ FAILED: {error}")
    
    # Save updated trades
    paper_trader.save_trades(trades_data)
    
    # Summary
    print(f"\n  ═══════════════════════════════")
    print(f"  💰 Orders placed: {len(placed)}")
    print(f"  ❌ Failed/skipped: {len(failed)}")
    print(f"  💵 Remaining balance: ${balance_cents/100:.2f}")
    
    return placed


def check_fills():
    """Check if any open limit orders have been filled."""
    trades_data = paper_trader.load_trades()
    updated = 0
    
    for trade in trades_data["trades"]:
        if trade.get("mode") != "LIVE":
            continue
        if trade.get("status") != "open":
            continue
        order_id = trade.get("order_id")
        if not order_id:
            continue
        
        try:
            orders = kalshi_client.get_orders(ticker=trade["ticker"])
            for o in orders.get("orders", []):
                if o.get("order_id") == order_id:
                    trade["filled_count"] = o.get("filled_count", 0)
                    trade["remaining_count"] = o.get("remaining_count", 0)
                    trade["order_status"] = o.get("status", "")
                    if o.get("status") == "canceled":
                        trade["status"] = "canceled"
                    updated += 1
                    break
        except Exception as e:
            print(f"  Error checking {trade['ticker']}: {e}")
    
    if updated:
        paper_trader.save_trades(trades_data)
        print(f"  Updated {updated} order statuses.")
    
    return updated


def get_positions_summary():
    """Get real positions from Kalshi."""
    try:
        positions = kalshi_client.get_positions()
        bal = kalshi_client.get_balance()
        print(f"\n  💰 Balance: ${bal['balance']/100:.2f}")
        print(f"  📊 Portfolio value: ${bal.get('portfolio_value', 0)/100:.2f}")
        
        market_positions = positions.get("market_positions", [])
        if market_positions:
            print(f"\n  Open positions ({len(market_positions)}):")
            for p in market_positions:
                ticker = p.get("ticker", "?")
                yes_count = p.get("total_traded", 0)
                print(f"    {ticker}: {yes_count} contracts")
        else:
            print("\n  No open positions.")
        
        return positions
    except Exception as e:
        print(f"  Error: {e}")
        return None


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "positions":
            get_positions_summary()
        elif cmd == "fills":
            check_fills()
        elif cmd == "balance":
            bal = get_real_balance()
            print(f"Balance: ${bal/100:.2f}")
        else:
            print(f"Unknown command: {cmd}")
            print("Usage: live_trader.py [run|positions|fills|balance]")
    else:
        run()


def sync_from_kalshi():
    """Sync trades.json with actual Kalshi positions, fills, and balance.
    This is the source of truth — call this to reconcile any drift."""
    import kalshi_client
    from datetime import datetime, timezone
    
    fills = kalshi_client._request('GET', '/portfolio/fills?limit=200')
    positions = kalshi_client.get_positions()
    resting_orders = kalshi_client.get_orders(status='resting')
    bal = kalshi_client.get_balance()
    
    pos_map = {p['ticker']: p for p in positions.get('market_positions', [])}
    
    city_map = {
        'KXHIGHNY': 'New York', 'KXHIGHCHI': 'Chicago',
        'KXHIGHLAX': 'Los Angeles', 'KXHIGHAUS': 'Austin',
        'KXHIGHDEN': 'Denver', 'KXHIGHHOU': 'Houston',
        'KXHIGHPHL': 'Philadelphia', 'KXHIGHSEA': 'Seattle',
    }
    
    # Group fills by ticker
    by_ticker = {}
    for f in fills.get('fills', []):
        t = f['ticker']
        if t not in by_ticker:
            by_ticker[t] = {'side': f['side'], 'total_count': 0, 'total_cost_cents': 0,
                           'fees_cents': 0, 'first_fill': f['created_time'], 'order_ids': set()}
        price = f['yes_price'] if f['side'] == 'yes' else f['no_price']
        by_ticker[t]['total_count'] += f['count']
        by_ticker[t]['total_cost_cents'] += price * f['count']
        by_ticker[t]['fees_cents'] += int(float(f.get('fee_cost', '0')) * 100)
        by_ticker[t]['order_ids'].add(f.get('order_id', ''))
        if f['created_time'] < by_ticker[t]['first_fill']:
            by_ticker[t]['first_fill'] = f['created_time']
    
    # Build resting orders map by ticker
    resting_map = {}
    for o in resting_orders.get('orders', []):
        t = o['ticker']
        if t not in resting_map:
            resting_map[t] = []
        resting_map[t].append(o)
    
    trades = []
    seen_tickers = set()
    
    # First: trades that have fills
    for ticker, data in by_ticker.items():
        seen_tickers.add(ticker)
        pos = pos_map.get(ticker, {})
        position_count = abs(pos.get('position', 0))
        series = ticker.split('-')[0]
        city = city_map.get(series, series)
        avg_price = data['total_cost_cents'] / data['total_count'] if data['total_count'] > 0 else 0
        
        status = 'open' if position_count > 0 else 'closed'
        
        # Check for resting orders on this ticker (partial fills)
        ticker_resting = resting_map.get(ticker, [])
        resting_count = sum(o.get('remaining_count', 0) for o in ticker_resting)
        resting_info = []
        for o in ticker_resting:
            resting_info.append({
                'order_id': o['order_id'],
                'side': o['side'],
                'yes_price': o.get('yes_price'),
                'no_price': o.get('no_price'),
                'remaining': o.get('remaining_count', 0),
                'created': o.get('created_time', '')
            })
        
        trade = {
            'ticker': ticker, 'series': series, 'city': city,
            'direction': data['side'].upper(),
            'entry_price_cents': round(avg_price),
            'contracts': data['total_count'],
            'cost_cents': data['total_cost_cents'],
            'fees_cents': data['fees_cents'],
            'timestamp': data['first_fill'],
            'status': status, 'mode': 'LIVE',
            'order_ids': list(data['order_ids']),
            'position_count': position_count,
            'market_exposure': pos.get('market_exposure', 0),
            'realized_pnl_cents': pos.get('realized_pnl', 0),
            'pnl_cents': None if status == 'open' else pos.get('realized_pnl', 0),
            'result': None,
            'resting_orders': resting_info,
            'resting_count': resting_count
        }
        trades.append(trade)
    
    # Second: resting orders with NO fills yet (pure limit orders waiting)
    for ticker, order_list in resting_map.items():
        if ticker in seen_tickers:
            continue
        seen_tickers.add(ticker)
        series = ticker.split('-')[0]
        city = city_map.get(series, series)
        
        for o in order_list:
            side = o['side']
            price = o['yes_price'] if side == 'yes' else o['no_price']
            remaining = o.get('remaining_count', 0)
            
            resting_info = [{
                'order_id': o['order_id'],
                'side': side,
                'yes_price': o.get('yes_price'),
                'no_price': o.get('no_price'),
                'remaining': remaining,
                'created': o.get('created_time', '')
            }]
            
            trade = {
                'ticker': ticker, 'series': series, 'city': city,
                'direction': side.upper(),
                'entry_price_cents': price,
                'contracts': 0,  # nothing filled yet
                'cost_cents': 0,
                'fees_cents': 0,
                'timestamp': o.get('created_time', ''),
                'status': 'resting', 'mode': 'LIVE',
                'order_id': o['order_id'],
                'order_ids': [o['order_id']],
                'position_count': 0,
                'market_exposure': 0,
                'realized_pnl_cents': 0,
                'pnl_cents': None,
                'result': None,
                'resting_orders': resting_info,
                'resting_count': remaining
            }
            trades.append(trade)
    
    balance = bal['balance']
    portfolio_value = bal.get('portfolio_value', 0)
    starting = 51076
    
    total_resting = sum(t.get('resting_count', 0) for t in trades)
    
    trades_data = {
        'trades': trades,
        'summary': {
            'total_trades': len(trades),
            'open': sum(1 for t in trades if t['status'] == 'open'),
            'resting': sum(1 for t in trades if t['status'] == 'resting' or t.get('resting_count', 0) > 0),
            'resting_contracts': total_resting,
            'settled': sum(1 for t in trades if t['status'] == 'closed'),
            'won': 0, 'lost': 0,
            'pnl_cents': (balance + portfolio_value) - starting,
            'realized_pnl_cents': sum(t.get('realized_pnl_cents', 0) or 0 for t in trades if t['status'] == 'closed'),
            'unrealized_pnl_cents': (balance + portfolio_value) - starting - sum(t.get('realized_pnl_cents', 0) or 0 for t in trades if t['status'] == 'closed'),
            'total_invested_cents': sum(t['cost_cents'] for t in trades),
            'portfolio_value_cents': balance + portfolio_value,
            'available_capital_cents': balance,
            'open_cost_cents': sum(t['cost_cents'] for t in trades if t['status'] == 'open'),
            'mode': 'LIVE',
            'started_at': '2026-02-19T00:28:00+00:00',
            'starting_capital_cents': starting,
            'synced_from_kalshi': datetime.now(timezone.utc).isoformat()
        }
    }
    
    with open(TRADES_FILE, 'w') as f:
        json.dump(trades_data, f, indent=2)
    
    print(f"Synced {len(trades)} trades ({total_resting} resting contracts) | P&L: ${trades_data['summary']['pnl_cents']/100:.2f} | Balance: ${balance/100:.2f}")
    return trades_data
