#!/usr/bin/env python3
"""
Test suite for FIFO lot-matching engine.
Validates all edge cases mentioned in the task requirements.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ledger

def test_simple_round_trip():
    """Test 1: Simple round trip - buy 10 @ 0.40, sell 10 @ 0.55"""
    print("=== Test 1: Simple Round Trip ===")
    
    engine = ledger.LotEngine()
    
    # Buy 10 @ 40 cents (YES side)
    pnl1 = engine.apply_fill("TEST-A", "YES", 10, 40, fee_cents=0, fill_id="fill1")
    print(f"Buy 10 @ 40¢: realized P&L = {pnl1}¢")
    assert pnl1 == 0, "Buy should not generate realized P&L"
    
    # Check open position
    pos = engine.remaining_qty("TEST-A")
    print(f"Position after buy: {pos}")
    assert pos["YES"] == 10 and pos["NO"] == 0, "Should have 10 YES contracts"
    
    # Sell 10 @ 55 cents (sell YES = buy NO @ 45¢)
    pnl2 = engine.apply_fill("TEST-A", "NO", 10, 45, fee_cents=0, fill_id="fill2")  # 100-55=45
    print(f"Sell 10 @ 55¢ (buy NO @ 45¢): realized P&L = {pnl2}¢")
    
    # Expected: YES+NO pair = 100¢. We paid 40¢+45¢=85¢, get 100¢ = +15¢ per contract × 10 = +150¢
    expected = 10 * (100 - 40 - 45)  # 10 * 15 = 150¢
    assert pnl2 == expected, f"Expected {expected}¢ realized P&L, got {pnl2}¢"
    
    # Check position is closed
    pos_after = engine.remaining_qty("TEST-A")
    print(f"Position after sell: {pos_after}")
    assert pos_after["YES"] == 0 and pos_after["NO"] == 0, "Position should be fully closed"
    
    total_pnl = engine.total_realized_pnl()
    print(f"Total realized P&L: {total_pnl}¢ (${total_pnl/100:.2f})")
    assert total_pnl == expected, f"Total P&L should be {expected}¢"
    
    print("✅ Test 1 PASSED\n")


def test_partial_sell():
    """Test 2: Partial sell - buy 10 @ 40¢, sell 4 @ 55¢"""
    print("=== Test 2: Partial Sell ===")
    
    engine = ledger.LotEngine()
    
    # Buy 10 @ 40¢
    engine.apply_fill("TEST-B", "YES", 10, 40, fee_cents=0, fill_id="fill3")
    
    # Sell 4 @ 55¢ (buy NO @ 45¢)  
    pnl = engine.apply_fill("TEST-B", "NO", 4, 45, fee_cents=0, fill_id="fill4")
    print(f"Sell 4 @ 55¢: realized P&L = {pnl}¢")
    
    # Expected: 4 contracts × (100 - 40 - 45) = 4 × 15 = 60¢
    expected = 4 * (100 - 40 - 45)
    assert pnl == expected, f"Expected {expected}¢, got {pnl}¢"
    
    # Check remaining position
    pos = engine.remaining_qty("TEST-B")
    print(f"Remaining position: {pos}")
    assert pos["YES"] == 6 and pos["NO"] == 0, "Should have 6 YES contracts remaining"
    
    print("✅ Test 2 PASSED\n")


def test_fifo_multiple_lots():
    """Test 3: Two buys, one sell across FIFO lots"""
    print("=== Test 3: FIFO Multiple Lots ===")
    
    engine = ledger.LotEngine()
    
    # Buy 10 @ 40¢ (first lot)
    engine.apply_fill("TEST-C", "YES", 10, 40, fee_cents=0, fill_id="fill5")
    
    # Buy 5 @ 60¢ (second lot) 
    engine.apply_fill("TEST-C", "YES", 5, 60, fee_cents=0, fill_id="fill6")
    
    # Check total position
    pos = engine.remaining_qty("TEST-C")
    print(f"Position after buys: {pos}")
    assert pos["YES"] == 15, "Should have 15 YES contracts total"
    
    # Sell 12 @ 70¢ (buy NO @ 30¢) - should consume FIFO order
    pnl = engine.apply_fill("TEST-C", "NO", 12, 30, fee_cents=0, fill_id="fill7")
    print(f"Sell 12 @ 70¢: realized P&L = {pnl}¢")
    
    # Expected FIFO matching:
    # - 10 contracts from first lot: (100 - 40 - 30) = 30¢ per contract × 10 = 300¢
    # - 2 contracts from second lot: (100 - 60 - 30) = 10¢ per contract × 2 = 20¢
    # - Total: 320¢
    expected = 10 * (100 - 40 - 30) + 2 * (100 - 60 - 30)
    assert pnl == expected, f"Expected {expected}¢, got {pnl}¢"
    
    # Check remaining position (3 contracts from second lot @ 60¢)
    pos_after = engine.remaining_qty("TEST-C")
    print(f"Remaining position: {pos_after}")
    assert pos_after["YES"] == 3 and pos_after["NO"] == 0, "Should have 3 YES contracts remaining"
    
    print("✅ Test 3 PASSED\n")


def test_instrument_isolation():
    """Test 4: Two instruments should not mix lots"""
    print("=== Test 4: Instrument Isolation ===")
    
    engine = ledger.LotEngine()
    
    # Buy instrument A
    engine.apply_fill("TEST-A", "YES", 5, 40, fee_cents=0, fill_id="fill8")
    
    # Buy instrument B  
    engine.apply_fill("TEST-B", "YES", 3, 50, fee_cents=0, fill_id="fill9")
    
    # Sell instrument A
    pnl_a = engine.apply_fill("TEST-A", "NO", 5, 45, fee_cents=0, fill_id="fill10")
    
    # Check that instrument B is untouched
    pos_a = engine.remaining_qty("TEST-A")
    pos_b = engine.remaining_qty("TEST-B")
    
    print(f"Position A after sell: {pos_a}")
    print(f"Position B (should be untouched): {pos_b}")
    
    assert pos_a["YES"] == 0 and pos_a["NO"] == 0, "Instrument A should be closed"
    assert pos_b["YES"] == 3 and pos_b["NO"] == 0, "Instrument B should be untouched"
    
    print("✅ Test 4 PASSED\n")


def test_oversell_prevention():
    """Test 5: Oversell should not corrupt state (engine allows it but tracks correctly)"""
    print("=== Test 5: Oversell Behavior ===")
    
    engine = ledger.LotEngine()
    
    # Buy 5 contracts
    engine.apply_fill("TEST-D", "YES", 5, 40, fee_cents=0, fill_id="fill11")
    
    # Try to sell 6 (more than we have)
    # Note: The ledger engine doesn't prevent oversells - it handles them by creating negative inventory
    # This is consistent with Kalshi mechanics where you can have net short positions
    pnl = engine.apply_fill("TEST-D", "NO", 6, 50, fee_cents=0, fill_id="fill12")
    
    pos = engine.remaining_qty("TEST-D")
    print(f"Position after oversell: {pos}")
    print(f"Realized P&L from oversell: {pnl}¢")
    
    # The engine should handle this gracefully - 5 contracts net to pairs, 1 remains as NO position
    # 5 pairs × (100 - 40 - 50) = 5 × 10 = 50¢ realized P&L
    expected_pnl = 5 * (100 - 40 - 50)
    assert pnl == expected_pnl, f"Expected {expected_pnl}¢ from netting, got {pnl}¢"
    
    # Should have 1 NO contract remaining (the excess)
    assert pos["YES"] == 0 and pos["NO"] == 1, "Should have 1 NO contract from oversell"
    
    print("✅ Test 5 PASSED (oversell handled gracefully)\n")


def test_fees_handling():
    """Test 6: Fees should be properly allocated"""
    print("=== Test 6: Fee Handling ===")
    
    engine = ledger.LotEngine()
    
    # Buy with fee
    engine.apply_fill("TEST-E", "YES", 10, 40, fee_cents=10, fill_id="fill13")
    
    # Sell with fee
    pnl = engine.apply_fill("TEST-E", "NO", 10, 45, fee_cents=5, fill_id="fill14")
    
    # Expected: 10 × (100 - 40 - 45) - fees = 10 × 15 - 15 = 135¢
    expected = 10 * (100 - 40 - 45) - 10 - 5  # Subtract both fees
    assert pnl == expected, f"Expected {expected}¢ after fees, got {pnl}¢"
    
    total_fees = engine.total_fees_paid()
    assert total_fees >= 15, f"Should track at least 15¢ in fees, got {total_fees}¢"
    
    print("✅ Test 6 PASSED\n")


def test_kalshi_mechanics():
    """Test 7: Kalshi-specific mechanics (BUY/SELL transformation)"""
    print("=== Test 7: Kalshi Mechanics ===")
    
    engine = ledger.LotEngine()
    
    # Test fill normalization
    buy_side, qty, price = ledger.normalize_fill("YES", "BUY", 10, 40)
    assert buy_side == "YES" and qty == 10 and price == 40, "BUY YES should pass through"
    
    buy_side, qty, price = ledger.normalize_fill("YES", "SELL", 10, 40)  
    assert buy_side == "NO" and qty == 10 and price == 60, "SELL YES @ 40 should = BUY NO @ 60"
    
    buy_side, qty, price = ledger.normalize_fill("NO", "SELL", 10, 40)
    assert buy_side == "YES" and qty == 10 and price == 60, "SELL NO @ 40 should = BUY YES @ 60"
    
    print("Kalshi fill transformations working correctly")
    print("✅ Test 7 PASSED\n")


def run_all_tests():
    """Run all FIFO engine tests"""
    print("🧪 FIFO Lot Engine Test Suite")
    print("=" * 50)
    
    test_simple_round_trip()
    test_partial_sell() 
    test_fifo_multiple_lots()
    test_instrument_isolation()
    test_oversell_prevention()
    test_fees_handling()
    test_kalshi_mechanics()
    
    print("🎉 All tests PASSED!")
    print("FIFO lot engine is working correctly.")


if __name__ == "__main__":
    run_all_tests()