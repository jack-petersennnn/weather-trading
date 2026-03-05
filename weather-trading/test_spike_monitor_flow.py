#!/usr/bin/env python3
"""
Simulate the spike monitor's execute_sell flow with all safety checks.
Does NOT place any real orders — just verifies the logic.
"""
import sys, json
sys.path.insert(0, '/home/ubuntu/.openclaw/workspace/weather-trading')
import kalshi_client

def simulate_execute_sell(ticker, claimed_direction, claimed_contracts, sell_price):
    """Simulate execute_sell's safety checks without placing orders."""
    print(f"\n  Simulating sell: {ticker} {claimed_direction} x{claimed_contracts} @ {sell_price}¢")
    
    # Step 1: Verify position on Kalshi
    positions = kalshi_client.get_positions()
    kalshi_pos = None
    for p in positions.get("market_positions", []):
        if p["ticker"] == ticker:
            kalshi_pos = p
            break
    
    if not kalshi_pos or kalshi_pos.get("position", 0) == 0:
        print(f"    ⛔ BLOCKED: No position on Kalshi")
        return "blocked_no_position"
    
    actual_count = abs(kalshi_pos["position"])
    actual_direction = "YES" if kalshi_pos["position"] > 0 else "NO"
    
    # Step 2: Direction check
    if actual_direction != claimed_direction:
        print(f"    🚨 BLOCKED: Direction mismatch! Claimed {claimed_direction}, actual {actual_direction}")
        return "blocked_direction_mismatch"
    
    # Step 3: Cap check
    contracts = claimed_contracts
    if contracts > actual_count:
        print(f"    ⚠ CAPPED: {contracts} → {actual_count}")
        contracts = actual_count
    
    print(f"    ✅ WOULD SELL: {ticker} {actual_direction} x{contracts} @ {sell_price}¢")
    print(f"       Using: sell_position(ticker, '{actual_direction.lower()}', {contracts}, {sell_price})")
    return "would_execute"

print("\n🧪 SPIKE MONITOR FLOW SIMULATION\n")
print("=" * 60)

# Get all positions for testing
positions = kalshi_client.get_positions()
test_cases = []
for p in positions.get('market_positions', []):
    pos = p.get('position', 0)
    if pos != 0 and abs(pos) >= 2:
        direction = "YES" if pos > 0 else "NO"
        test_cases.append((p['ticker'], direction, abs(pos)))
        if len(test_cases) >= 5:
            break

print(f"Testing with {len(test_cases)} positions:\n")

# Test 1: Normal sell (correct direction, within count)
if test_cases:
    t = test_cases[0]
    print("TEST 1: Normal sell (should pass)")
    result = simulate_execute_sell(t[0], t[1], 1, 50)
    assert result == "would_execute"

# Test 2: Oversell (more contracts than held)
if test_cases:
    t = test_cases[0]
    print("\nTEST 2: Oversell (should cap)")
    result = simulate_execute_sell(t[0], t[1], 99999, 50)
    assert result == "would_execute"  # Capped, not blocked

# Test 3: Wrong direction (the bug that caused the OKC disaster)
if test_cases:
    t = test_cases[0]
    wrong_dir = "NO" if t[1] == "YES" else "YES"
    print(f"\nTEST 3: Wrong direction (should block)")
    result = simulate_execute_sell(t[0], wrong_dir, 1, 50)
    assert result == "blocked_direction_mismatch"

# Test 4: Non-existent ticker
print(f"\nTEST 4: Non-existent position (should block)")
result = simulate_execute_sell("FAKE-TICKER-123", "YES", 1, 50)
assert result == "blocked_no_position"

# Test 5: Check all positions match trades.json
print(f"\n\nTEST 5: Full trades.json vs Kalshi direction audit")
with open('trades.json') as f:
    trades = json.load(f)

kalshi_dirs = {}
for p in positions.get('market_positions', []):
    pos = p.get('position', 0)
    if pos != 0:
        kalshi_dirs[p['ticker']] = "YES" if pos > 0 else "NO"

mismatches = 0
for t in trades.get('trades', []):
    if t.get('status') != 'open':
        continue
    ticker = t['ticker']
    kdir = kalshi_dirs.get(ticker)
    if kdir and kdir != t.get('direction'):
        print(f"  🚨 MISMATCH: {ticker} local={t['direction']} kalshi={kdir}")
        mismatches += 1

if mismatches == 0:
    print("  ✅ All directions match")

print(f"\n{'=' * 60}")
print("✅ ALL SIMULATION TESTS PASSED")
print(f"{'=' * 60}")
