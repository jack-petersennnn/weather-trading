#!/usr/bin/env python3
"""
OKC Emergency Exit Script v3
=============================
Priority exit for YES position on KXHIGHTOKC-26FEB23-T53.

RULES (from Tucker):
- MAX 20% loss ($159.33 exposure → floor at ~$127 = sell at 16¢ minimum)
- Breakeven (19¢) or profit is the goal
- Monitor constantly, sell on any spike above breakeven
- If can't breakeven, cut before losing more than 20%

Strategy by time of day (Central):
- Overnight (<7 AM): Monitor, sell on spikes only
- Morning (7-12): Sell at breakeven+, start cutting at floor after 10 min
- Afternoon (12-8 PM): METAR-informed. If OKC hit 53°F+, dump (we lose). If cold, hold.
- Evening (8 PM+): Dump everything remaining
"""
import json
import time
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, '/home/ubuntu/.openclaw/workspace/weather-trading')
import kalshi_client

TICKER = "KXHIGHTOKC-26FEB23-T53"
CHECK_INTERVAL = 3
CENTRAL_OFFSET = -6

# Tucker's rules
MAX_LOSS_PCT = 0.20  # 20% max loss
BREAKEVEN_CENTS = 19  # avg entry ~19¢
FLOOR_CENTS = max(1, int(BREAKEVEN_CENTS * (1 - MAX_LOSS_PCT)))  # 15¢

total_sold = 0
total_revenue = 0

def get_position():
    positions = kalshi_client.get_positions()
    for p in positions.get('market_positions', []):
        if p['ticker'] == TICKER:
            return p
    return None

def get_yes_bid():
    ob = kalshi_client.get_orderbook(TICKER)
    yes_bids = ob.get('orderbook', {}).get('yes', [])
    if not yes_bids:
        return 0, 0
    best_price = 0
    best_qty = 0
    for price, qty in yes_bids:
        if price > best_price:
            best_price = price
            best_qty = qty
    return best_price, best_qty

def get_total_depth(min_price=1):
    ob = kalshi_client.get_orderbook(TICKER)
    yes_bids = ob.get('orderbook', {}).get('yes', [])
    total = 0
    for price, qty in yes_bids:
        if price >= min_price:
            total += qty
    return total

def sell_yes(contracts, price_cents):
    global total_sold, total_revenue
    
    pos = get_position()
    if not pos or pos.get('position', 0) <= 0:
        return 0
    actual = pos['position']
    if contracts > actual:
        contracts = actual
    if contracts <= 0:
        return 0
    
    try:
        result = kalshi_client.sell_position(TICKER, "yes", contracts, price_cents)
        order = result.get("order", {})
        filled = order.get("fill_count", 0)
        status = order.get("status", "?")
        order_id = order.get("order_id", "?")
        
        if status == "resting":
            time.sleep(5)
            try:
                kalshi_client.cancel_order(order_id)
                try:
                    for o in kalshi_client.get_orders(ticker=TICKER, status="canceled").get("orders", []):
                        if o.get("order_id") == order_id:
                            filled = o.get("fill_count", filled)
                except:
                    pass
            except:
                pass
        
        if filled > 0:
            revenue = filled * price_cents
            total_sold += filled
            total_revenue += revenue
            print(f"  ✅ Sold {filled} @ {price_cents}¢ = ${revenue/100:.2f} (total: {total_sold}, ${total_revenue/100:.2f})")
        
        return filled
    except Exception as e:
        print(f"  ❌ {e}")
        return 0

def get_central_hour():
    utc_now = datetime.now(timezone.utc)
    central = utc_now + timedelta(hours=CENTRAL_OFFSET)
    return central.hour, central.minute

def get_metar_temp():
    try:
        from metar_tracker import get_intraday_status
        status = get_intraday_status("Oklahoma City", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        if status:
            return status.get("current_high_f"), status.get("observations_count", 0)
    except:
        pass
    return None, 0

def main():
    pos = get_position()
    if not pos or pos.get('position', 0) <= 0:
        print("No OKC T53 YES position found!")
        return
    
    contracts = pos['position']
    exposure = pos.get('market_exposure', 0)
    
    print(f"{'=' * 60}")
    print(f"OKC PRIORITY EXIT v3 — MAX 20% LOSS")
    print(f"Position: {contracts} YES | ${exposure/100:.2f} exposed")
    print(f"Breakeven: {BREAKEVEN_CENTS}¢ | Floor: {FLOOR_CENTS}¢ | Max loss: {MAX_LOSS_PCT*100:.0f}%")
    print(f"{'=' * 60}\n")
    
    below_floor_since = None
    FLOOR_CUT_DELAY = 600  # 10 min below floor = start cutting
    
    while True:
        try:
            pos = get_position()
            if not pos or pos.get('position', 0) <= 0:
                print(f"\n🎉 Fully exited! Sold {total_sold}, Rev: ${total_revenue/100:.2f}")
                break
            
            remaining = pos['position']
            best_bid, bid_qty = get_yes_bid()
            ch, cm = get_central_hour()
            ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
            
            phase = "OVERNIGHT" if ch < 7 else "MORNING" if ch < 12 else "AFTERNOON" if ch < 20 else "EVENING"
            
            print(f"[{ts}] {remaining}x | bid:{best_bid}¢x{bid_qty} | {phase}", end="")
            
            # ═══ ALWAYS: Sell at breakeven or above ═══
            if best_bid >= BREAKEVEN_CENTS:
                depth = get_total_depth(BREAKEVEN_CENTS)
                qty = min(remaining, depth, 200)
                if qty > 0:
                    print(f" → 🚀 PROFIT {qty}x @ {best_bid}¢")
                    sell_yes(qty, best_bid)
                    below_floor_since = None
                    time.sleep(1)
                    continue
            
            # ═══ ALWAYS: Near breakeven (1¢ away) with decent depth ═══
            elif best_bid == BREAKEVEN_CENTS - 1 and bid_qty >= 5:
                qty = min(remaining, bid_qty, 100)
                print(f" → 💰 NEAR-BE {qty}x @ {best_bid}¢")
                sell_yes(qty, best_bid)
                below_floor_since = None
                time.sleep(1)
                continue
            
            # ═══ OVERNIGHT: Monitor only ═══
            if phase == "OVERNIGHT":
                # Even overnight, enforce the 20% floor with longer patience
                if best_bid <= FLOOR_CENTS and best_bid > 0:
                    if below_floor_since is None:
                        below_floor_since = time.time()
                    elapsed = time.time() - below_floor_since
                    # Overnight: 30 min patience before cutting
                    if elapsed >= 1800:
                        qty = min(remaining, bid_qty, 30)
                        if qty > 0:
                            print(f" → ✂️ FLOOR CUT {qty}x @ {best_bid}¢ (30min below floor)")
                            sell_yes(qty, best_bid)
                            time.sleep(2)
                            continue
                    else:
                        print(f" | floor in {int(1800-elapsed)}s")
                else:
                    below_floor_since = None
                    print()
                time.sleep(CHECK_INTERVAL)
                continue
            
            # ═══ MORNING+: Enforce 20% floor with 10 min patience ═══
            if best_bid <= FLOOR_CENTS and best_bid > 0:
                if below_floor_since is None:
                    below_floor_since = time.time()
                elapsed = time.time() - below_floor_since
                if elapsed >= FLOOR_CUT_DELAY:
                    qty = min(remaining, bid_qty, 50)
                    if qty > 0:
                        print(f" → ✂️ 20% FLOOR CUT {qty}x @ {best_bid}¢ ({int(elapsed)}s below)")
                        sell_yes(qty, best_bid)
                        time.sleep(2)
                        continue
                else:
                    print(f" | floor cut in {int(FLOOR_CUT_DELAY-elapsed)}s")
            else:
                below_floor_since = None
            
            # ═══ AFTERNOON: METAR check ═══
            if phase == "AFTERNOON":
                metar_temp, obs_count = get_metar_temp()
                if metar_temp is not None and obs_count >= 3:
                    if metar_temp >= 53:
                        qty = min(remaining, get_total_depth(1), 200)
                        if qty > 0:
                            print(f" → 🔥 METAR {metar_temp}°F≥53, DUMP {qty}x @ {best_bid}¢")
                            sell_yes(qty, max(1, best_bid))
                            time.sleep(1)
                            continue
                    elif metar_temp <= 48 and ch >= 14:
                        print(f" | METAR {metar_temp}°F, looks good, holding")
                    else:
                        print(f" | METAR {metar_temp}°F")
                else:
                    if best_bid > FLOOR_CENTS:
                        print()
            
            # ═══ EVENING: Dump everything ═══
            elif phase == "EVENING":
                qty = min(remaining, get_total_depth(1), 200)
                if qty > 0 and best_bid >= 1:
                    print(f" → ⏰ EOD {qty}x @ {best_bid}¢")
                    sell_yes(qty, best_bid)
                    time.sleep(1)
                    continue
                else:
                    print(f" | no depth")
            
            elif best_bid > FLOOR_CENTS:
                print()
            
            time.sleep(CHECK_INTERVAL)
            
        except KeyboardInterrupt:
            print(f"\n⛔ Stopped. Sold {total_sold}, Rev: ${total_revenue/100:.2f}")
            break
        except Exception as e:
            print(f"\n⚠ {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
