#!/usr/bin/env python3
"""
End-to-end test of the spike monitor sell flow.
Uses a real small position to test sell_position → cancel flow.
"""
import sys, json, time
sys.path.insert(0, '/home/ubuntu/.openclaw/workspace/weather-trading')
import kalshi_client

def find_test_position():
    """Find a small, expendable position to test with."""
    positions = kalshi_client.get_positions()
    for p in positions.get('market_positions', []):
        pos = p.get('position', 0)
        if abs(pos) >= 2 and abs(pos) <= 10 and 'OKC' not in p['ticker']:
            return p
    return None

def test_sell_and_cancel():
    """Place a sell_position at high price (won't fill), then cancel."""
    pos = find_test_position()
    if not pos:
        print("❌ No suitable test position found")
        return False
    
    ticker = pos['ticker']
    count = abs(pos['position'])
    side = "yes" if pos['position'] > 0 else "no"
    
    print(f"Test position: {ticker} {side.upper()} x{count}")
    
    # Step 1: Sell 1 contract at 99¢ (will rest, not fill)
    print(f"  Placing sell_position({ticker}, {side}, 1, 99)...")
    result = kalshi_client.sell_position(ticker, side, 1, 99)
    order = result.get("order", {})
    order_id = order.get("order_id")
    status = order.get("status")
    filled = order.get("fill_count", 0)
    initial = order.get("initial_count", 0)
    
    print(f"  Result: status={status}, filled={filled}, initial={initial}, id={order_id}")
    
    if status == "resting":
        # Verify position didn't change
        pos_after = None
        for p in kalshi_client.get_positions().get('market_positions', []):
            if p['ticker'] == ticker:
                pos_after = p
                break
        
        count_after = abs(pos_after['position']) if pos_after else 0
        print(f"  Position after resting order: {count_after} (was {count})")
        assert count_after == count, f"Position changed unexpectedly!"
        print(f"  ✅ Position unchanged while order resting")
        
        # Cancel
        kalshi_client.cancel_order(order_id)
        print(f"  ✅ Canceled test order")
    elif filled > 0:
        print(f"  ⚠ Somehow filled at 99¢!")
    
    # Step 2: Try to sell MORE than we hold
    print(f"\n  Attempting oversell: sell_position({ticker}, {side}, {count + 50}, 99)...")
    try:
        result2 = kalshi_client.sell_position(ticker, side, count + 50, 99)
        order2 = result2.get("order", {})
        order_id2 = order2.get("order_id")
        initial2 = order2.get("initial_count", 0)
        
        print(f"  Result: initial_count={initial2} (requested {count + 50})")
        
        # Cancel immediately
        try:
            kalshi_client.cancel_order(order_id2)
        except:
            pass
        
        if initial2 <= count:
            print(f"  ✅ Kalshi capped sell to {initial2} (held {count})")
        else:
            print(f"  ⚠ Kalshi accepted {initial2} contracts! sell_position does NOT cap!")
            print(f"  → Our code-side cap is ESSENTIAL")
            
    except Exception as e:
        if "insufficient" in str(e).lower():
            print(f"  ✅ Kalshi rejected oversell: {e}")
        else:
            print(f"  Error: {e}")
    
    # Step 3: Try to sell on WRONG side
    wrong_side = "no" if side == "yes" else "yes"
    print(f"\n  Attempting wrong-side sell: sell_position({ticker}, {wrong_side}, 1, 99)...")
    try:
        result3 = kalshi_client.sell_position(ticker, wrong_side, 1, 99)
        order3 = result3.get("order", {})
        order_id3 = order3.get("order_id")
        status3 = order3.get("status")
        
        print(f"  Result: status={status3}")
        try:
            kalshi_client.cancel_order(order_id3)
        except:
            pass
        print(f"  ⚠ Kalshi accepted wrong-side sell! This would CREATE a position!")
        
    except Exception as e:
        print(f"  ✅ Kalshi rejected wrong-side sell: {e}")
    
    return True

if __name__ == "__main__":
    print("\n🧪 END-TO-END SELL FLOW TEST\n")
    test_sell_and_cancel()
    print("\n✅ Test complete\n")
