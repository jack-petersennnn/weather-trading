# Spike Monitor Bug Analysis & Fixes — Feb 23, 2026

## Root Cause: Position Flip Bug
The spike monitor "sold" by calling `place_order(action='buy')` on the OPPOSITE side.
For a NO position, it bought YES to close. But Kalshi doesn't cap this — if you buy more YES
than your NO position, you FLIP to a net YES position. This caused an $170+ runaway on OKC.

## Key Discovery: Kalshi API Does NOT Protect You
- `sell_position(action='sell')` does NOT cap to your actual holdings
- `sell_position` on the WRONG SIDE creates a new position instead of rejecting
- Both `place_order` and `sell_position` can overshoot and flip positions
- **ALL safety must be code-side**

## Fixes Applied:
- [x] `execute_sell()` now uses `sell_position()` instead of `place_order()` on opposite side
- [x] Before ANY sell: verify actual position direction AND count from Kalshi API
- [x] Cap sell quantity to actual Kalshi position count (never sell more than held)
- [x] Circuit breaker: if Kalshi direction != trades.json direction, BLOCK sell + log alert
- [x] 60-second cooldown per ticker (no hammering same position every 0.75s)
- [x] Max 3 sells per scan cycle (prevents runaway in single loop iteration)
- [x] Both sync methods (spike_monitor + position_manager) now sync direction from Kalshi
- [x] Both sync methods now write `market_exposure` from Kalshi (accurate cap checks)
- [x] Fast scanner cap check uses `market_exposure` instead of stale `cost_cents`
- [x] Fixed 3 direction mismatches in trades.json (OKC, DC, Miami)
- [x] OKC exit script uses `sell_position` with position verification

## Test Results:
- test_sell_logic.py: All 6 tests pass (direction detection, capping, API format, oversell)
- test_spike_monitor_flow.py: All 5 simulation tests pass (normal, oversell, wrong direction, missing, audit)
- test_sell_e2e.py: Confirmed Kalshi doesn't protect against oversell or wrong-side sells

## Files Modified:
- `spike_monitor.py` — execute_sell rewrite, sync direction fix, cooldown, cycle cap
- `position_manager.py` — sync direction fix, market_exposure tracking
- `fast_scanner.py` — market_exposure-based cap check
- `kalshi_client.py` — (no changes needed, sell_position already existed)
- `okc_exit.py` — v2 rewrite with sell_position + phase-based strategy
- `trades.json` — fixed 3 direction mismatches
