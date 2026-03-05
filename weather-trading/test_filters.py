#!/usr/bin/env python3
"""
Diagnostic test: verify all rescue mode filters work correctly.
Simulates opportunities and checks they get blocked by the new filters.
Does NOT place any trades — pure filter verification.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fast_scanner import filter_opportunity, _load_trading_config

# Load actual config
cfg = _load_trading_config()

print("=" * 70)
print("RESCUE MODE FILTER DIAGNOSTIC")
print("=" * 70)
print(f"\nConfig loaded:")
for k in ["rescue_mode", "allowed_sides", "threshold_only", "no_price_band_min_cents",
           "no_price_band_max_cents", "abstain_zone_min_prob", "abstain_zone_max_prob",
           "min_net_ev_cents", "max_spread_cents", "min_depth_factor", "max_quote_age_seconds"]:
    print(f"  {k}: {cfg.get(k, 'NOT SET')}")

print("\n" + "-" * 70)
print("TEST CASES")
print("-" * 70)

tests = [
    # (name, opportunity, expected_result)
    ("Bracket market (should BLOCK)", {
        "ticker": "KXHIGHTLV-26FEB25-B80.5", "direction": "NO",
        "entry_price_cents": 75, "our_prob": 0.80, "edge": 0.15,
    }, False),
    
    ("YES entry (should BLOCK)", {
        "ticker": "KXHIGHTCHI-26FEB25-T40", "direction": "YES",
        "entry_price_cents": 20, "our_prob": 0.35, "edge": 0.15,
    }, False),
    
    ("NO at 50¢ (below 65¢ band, should BLOCK)", {
        "ticker": "KXHIGHTCHI-26FEB25-T40", "direction": "NO",
        "entry_price_cents": 50, "our_prob": 0.70, "edge": 0.20,
    }, False),
    
    ("NO at 90¢ (above 85¢ band, should BLOCK)", {
        "ticker": "KXHIGHTCHI-26FEB25-T40", "direction": "NO",
        "entry_price_cents": 90, "our_prob": 0.92, "edge": 0.02,
    }, False),
    
    ("NO at 75¢ but prob 50% (abstain zone, should BLOCK)", {
        "ticker": "KXHIGHTCHI-26FEB25-T40", "direction": "NO",
        "entry_price_cents": 75, "our_prob": 0.50, "edge": 0.15,
    }, False),
    
    ("NO at 75¢ prob 80% but low EV (should BLOCK if EV < 8¢)", {
        "ticker": "KXHIGHTCHI-26FEB25-T40", "direction": "NO",
        "entry_price_cents": 75, "our_prob": 0.78, "edge": 0.03,
        # EV = 0.78*100 - 75 = 3¢ < 8¢
    }, False),
    
    ("NO at 75¢ prob 85% strong EV (should PASS)", {
        "ticker": "KXHIGHTCHI-26FEB25-T40", "direction": "NO",
        "entry_price_cents": 75, "our_prob": 0.85, "edge": 0.10,
        # EV = 0.85*100 - 75 = 10¢ >= 8¢
    }, True),
    
    ("NO at 70¢ prob 82% (should PASS)", {
        "ticker": "KXHIGHTDEN-26FEB25-T55", "direction": "NO",
        "entry_price_cents": 70, "our_prob": 0.82, "edge": 0.12,
        # EV = 0.82*100 - 70 = 12¢ >= 8¢
    }, True),
    
    ("NO at 65¢ prob 75% (should PASS)", {
        "ticker": "KXHIGHTMIN-26FEB25-T25", "direction": "NO",
        "entry_price_cents": 65, "our_prob": 0.75, "edge": 0.10,
        # EV = 0.75*100 - 65 = 10¢ >= 8¢
    }, True),
    
    ("NO at 85¢ prob 95% (edge of band, should PASS)", {
        "ticker": "KXHIGHTPHX-26FEB25-T90", "direction": "NO",
        "entry_price_cents": 85, "our_prob": 0.95, "edge": 0.10,
        # EV = 0.95*100 - 85 = 10¢ >= 8¢
    }, True),
    
    ("NO at 64¢ (1 below band min, should BLOCK)", {
        "ticker": "KXHIGHTCHI-26FEB25-T40", "direction": "NO",
        "entry_price_cents": 64, "our_prob": 0.80, "edge": 0.16,
    }, False),
    
    ("NO at 86¢ (1 above band max, should BLOCK)", {
        "ticker": "KXHIGHTCHI-26FEB25-T40", "direction": "NO",
        "entry_price_cents": 86, "our_prob": 0.90, "edge": 0.04,
    }, False),
]

passed = 0
failed = 0
for name, opp, expected in tests:
    result, reason, details = filter_opportunity(opp, cfg, hold_to_settlement=True)
    status = "PASS" if result == expected else "FAIL"
    emoji = "✅" if status == "PASS" else "❌"
    
    if status == "PASS":
        passed += 1
    else:
        failed += 1
    
    if result:
        print(f"  {emoji} {name}: ALLOWED (expected {'allow' if expected else 'BLOCK'})")
    else:
        print(f"  {emoji} {name}: BLOCKED ({reason}) (expected {'allow' if expected else 'block'})")

print(f"\n{'='*70}")
print(f"RESULTS: {passed} passed, {failed} failed out of {len(tests)} tests")
if failed == 0:
    print("ALL FILTERS WORKING CORRECTLY ✅")
else:
    print(f"⚠ {failed} FAILURES — check filter logic")
print(f"{'='*70}")
