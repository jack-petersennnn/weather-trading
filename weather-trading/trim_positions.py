#!/usr/bin/env python3
"""One-time script to trim overlapping/over-limit Feb 20 positions."""
import json, sys, fcntl, time
sys.path.insert(0, "/home/ubuntu/.openclaw/workspace/weather-trading")
import kalshi_client

TRADES_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/trades.json"
MARKET_SELL_DISCOUNT = 2  # cents below mid for fast fill

# Tickers to sell (all contracts)
SELL_TICKERS = [
    "KXHIGHAUS-26FEB20-T72",      # 116 ctrs, forecast 68.1 = losing bet
    "KXHIGHCHI-26FEB20-B45.5",     # 8 ctrs, redundant with T45
    "KXHIGHLAX-26FEB20-B58.5",     # 14 ctrs, overlaps both neighbors
    "KXHIGHMIA-26FEB20-B80.5",     # 84 ctrs, over limit + overlap
    "KXHIGHMIA-26FEB20-B84.5",     # 14 ctrs, redundant with T85
    "KXHIGHMIA-26FEB20-T85",       # 10 ctrs, over limit
    "KXHIGHNY-26FEB20-B39.5",      # 10 ctrs, contradicts T39
]

def get_market_price(ticker):
    try:
        data = kalshi_client._request("GET", f"/markets/{ticker}")
        mkt = data.get("market", data)
        return {
            "yes_price": mkt.get("yes_ask") or mkt.get("last_price") or 50,
            "no_price": mkt.get("no_ask") or (100 - (mkt.get("yes_ask") or 50)),
        }
    except Exception as e:
        print(f"  !! Can't get price for {ticker}: {e}")
        return None

def load_trades():
    with open(TRADES_FILE) as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        data = json.load(f)
        fcntl.flock(f, fcntl.LOCK_UN)
    return data

def save_trades(data):
    with open(TRADES_FILE, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        json.dump(data, f, indent=2)
        fcntl.flock(f, fcntl.LOCK_UN)

def main():
    data = load_trades()
    trades = data.get("trades", [])
    
    results = []
    
    for ticker in SELL_TICKERS:
        # Find the trade
        trade = None
        for t in trades:
            if t.get("ticker") == ticker and t.get("status") == "open":
                trade = t
                break
        
        if not trade:
            print(f"⚠️  {ticker} — not found or not open, skipping")
            continue
        
        direction = trade["direction"].lower()
        contracts = trade["contracts"]
        entry_price = trade["entry_price_cents"]
        
        print(f"\n{'='*60}")
        print(f"SELLING: {ticker} | {direction.upper()} x{contracts} @ entry {entry_price}¢")
        
        # Get current market price
        mkt = get_market_price(ticker)
        if not mkt:
            print(f"  ✗ No market price, SKIPPING")
            results.append((ticker, "FAILED", "no market price"))
            continue
        
        yes_p = mkt["yes_price"]
        if direction == "no":
            sell_price = max(1, (100 - yes_p) - MARKET_SELL_DISCOUNT)
        else:
            sell_price = max(1, yes_p - MARKET_SELL_DISCOUNT)
        
        pnl_per = sell_price - entry_price
        total_pnl = pnl_per * contracts
        
        print(f"  Market yes_ask: {yes_p}¢ | Our sell: {sell_price}¢ | P&L: {total_pnl:+d}¢ (${total_pnl/100:+.2f})")
        
        try:
            result = kalshi_client.sell_position(
                ticker=ticker,
                side=direction,
                contracts=contracts,
                price_cents=sell_price,
            )
            order_id = result.get("order", {}).get("order_id", "?")
            status = result.get("order", {}).get("status", "?")
            print(f"  ✅ Order placed: {order_id} | status: {status}")
            
            # Update trade in trades.json
            trade["status"] = "closed"
            trade["exit_price_cents"] = sell_price
            trade["exit_reason"] = "rule_enforcement_trim"
            trade["exit_time"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            trade["pnl_cents"] = total_pnl
            trade["exit_order_id"] = order_id
            
            emoji = "💰" if total_pnl > 0 else "🔻"
            results.append((ticker, "SOLD", f"{sell_price}¢ | {emoji} ${total_pnl/100:+.2f}"))
            
        except Exception as e:
            print(f"  ✗ SELL FAILED: {e}")
            results.append((ticker, "FAILED", str(e)))
        
        time.sleep(0.5)  # Rate limit courtesy
    
    # Save updated trades
    save_trades(data)
    
    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    total_pnl_all = 0
    for ticker, status, detail in results:
        print(f"  {ticker}: {status} — {detail}")
        if status == "SOLD" and "$" in detail:
            # extract pnl
            import re
            m = re.search(r'\$([+-]?\d+\.\d+)', detail)
            if m:
                total_pnl_all += float(m.group(1))
    print(f"\n  Net P&L from trim: ${total_pnl_all:+.2f}")

if __name__ == "__main__":
    main()
