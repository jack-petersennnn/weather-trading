# Weather Trading Audit - Model Responses

## Ground Truth
- Starting Kalshi balance: $510.76
- Current balance: $187.54
- **ACTUAL total P&L: -$323.22** (this is the only number that matters)
- Journal claimed P&L: +$1,757.87 (WRONG - $2,081 accounting gap)

## What the Previous Sub-Agent Got Wrong
The sub-agent claimed a "$4,168.83 unaccounted gap" between Kalshi API data and balance. This was a calculation error. It assumed all sell fills = cash received, but Kalshi short positions require collateral (sells don't credit your balance when opening shorts). The gap is a MODELING error in the reconciliation, not missing money.

## What Both External Models Got Wrong

### EXIT_BLOWN is NOT panic selling
Both models assumed EXIT_BLOWN = "fear-based exit" that destroyed $2,231 in profits.

**Code proof (position_manager.py lines 451-504):**
EXIT_BLOWN only fires when ALL of these are true:
1. It's TODAY's contract (not future dates)
2. We have REAL temperature data (from METAR/observations)
3. The temperature has ALREADY passed peak hour AND settled against our position
4. OR the actual observed temp has already crossed the threshold making the position mathematically impossible to win

Examples from journal:
- "Past peak (15:00), max only 0°F vs 45.0°F needed" - Chicago was never going to hit 45°F
- "Past peak (21:00), max only 34.5°F vs 42.0°F needed" - game was over
- "Temp 82.6°F landed IN our bracket [82-83°F]" - bracket already triggered

**Most of these positions were already dead.** However, 10 out of 49 EXIT_BLOWN positions (20%) would have actually WON at settlement if held. Total counterfactual gain from those 10: $56.70. Root causes of false positives:

**BUG 1 - Previous evening temps included in today's max (ROOT CAUSE) [FIXED]:**
Open-Meteo returns data in UTC. For US cities, early UTC hours (0-5) correspond to the PREVIOUS LOCAL EVENING (e.g., midnight UTC = 7PM EST). `get_todays_hourly()` was including all observed UTC hours in `max_so_far`, meaning yesterday's 7-11PM temps inflated today's observed max. Atlanta Feb 21: hour 0 UTC showed 72.4°F (from Feb 20 evening) but actual Feb 21 daytime max was only 63.5°F. **Fix: added daytime filter (local 8AM+) to exclude previous evening hours from max_so_far.**

**BUG 2 - Dynamic peak hour from forecast data [FIXED]:**
`get_todays_hourly()` computed peak_hour from ALL 24 forecast hours including overnight, allowing noisy overnight forecasts to claim peak at 4AM. Caller now uses hardcoded `peak_hour` from city config (14-15 for all cities) as default. The `peak_detector` module (which has its own daytime filtering) can still override when it has high-confidence data.

**BUG 3 - Open-Meteo vs NWS data source mismatch:**
Bot uses Open-Meteo API for temperature data. Kalshi settles on NWS official daily high. These can differ by several degrees, especially for boundary/rounding cases. Not fixed yet — requires cross-referencing NWS ASOS/AWOS data.

- EXIT_BLOWN is ~80% correct, ~20% false positive rate. Not the catastrophic "panic sell" the models claimed, but not perfect either. The timezone bug is the biggest contributor.

### EXIT_GRADUATED IS the forecast-shift exit
This is what both models were actually worried about:
- Fires when the forecast shifts significantly against the position
- Example: "Forecast shifted +6.8°F (38.7→45.5°F), severity 4.53x std"
- 40 entries, 281 contracts total

### The $2,231 "lost to exits" was massively inflated
The correct counterfactual numbers:
- EXIT_BLOWN false positives (10/49): **$56.70** in missed gains
- EXIT_GRADUATED (15/40 would have won): **$12.71** in missed gains
- **Combined real cost of premature exits: ~$69.41** (not $2,231)
- The other 39 EXIT_BLOWN positions were genuinely dead and exiting changed nothing

## Journal Accounting Bug (Root Cause of $2,081 Gap)

The journal's P&L tracking is fundamentally broken:

| Action | Count | Contracts | Journal P&L |
|--------|-------|-----------|-------------|
| ENTRY | 104 | - | +$904.86 |
| HEDGE | 36 | 1,180 | +$853.01 |
| EXIT_BLOWN | 49 | 2,745 | $0.00 |
| EXIT_GRADUATED | 40 | 281 | $0.00 |
| **TOTAL** | **229** | - | **+$1,757.87** |

**Multiple problems found:**

1. **EXIT records P&L = $0**: The journal doesn't track actual sell price or P&L for exit events. We don't know what we received.

2. **ENTRY records show phantom P&L**: Verified that ENTRY P&L is computed as (settlement_value - entry_price) × contracts, even for positions that were fully exited before settlement. Example: KXHIGHTDAL-26FEB25-T80 shows ENTRY P&L +$11.16 (12 contracts × 93¢ gain), but all 12 contracts were EXIT_BLOWN before settlement. The journal credits $11.16 for contracts we no longer held. **$194.68 in total phantom ENTRY P&L identified** across 26 overlapping tickers.

3. **HEDGE P&L not netted against original position**: HEDGE entries show +$853. These are RE-ENTRY positions bought after an exit. The hedge wins $853 at settlement, but the ORIGINAL position that was exited (triggering the hedge) shows $0 for its exit loss. Net effect is overstated.

4. **Short selling costs hidden**: The bot frequently buys YES AND sells NO on the same market (dual-leg entries creating leveraged exposure). The journal tracks only the YES buy price as "entry_price" but the NO sell side requires collateral that isn't captured.

5. **Remaining gap unexplained**: Phantom ENTRY P&L accounts for only $194.68 of the $2,081 gap. The remaining ~$1,886 likely comes from uncaptured short-selling costs, fee accumulation, and the dual-leg entry mechanics.

## Settlement Statistics
- 233 total settlements
- 55 YES wins (revenue: $14.00)
- 177 NO wins (revenue: $352.00)
- 1 scalar (revenue: $0.00)
- Total settlement revenue: $366.00
- Total settlement fees: $89.76
- Total fill fees: ~$90
- **Win rate by count: 23.6%** (55/233)

## Reconciliation Status: INCOMPLETE
The full fill-level reconciliation could NOT be completed because Kalshi's balance mechanics for short positions (sell NO when opening a short) are ambiguous from the API data alone. The sub-agent tried 4 different models and none reconciled within $50:

- Model A (sells = cash credit): Gap $4,079 (wrong - doesn't account for collateral)
- Model B (sells = collateral lock): Gap -$588  
- Model C (sells = collateral only): Gap -$1,049
- Model D (cashflow + settlement owed): Gap -$484

The closest was Model D (-$484 gap), suggesting there may have been a deposit or the starting balance timestamp doesn't align with the first fill.

**Bottom line: We KNOW the total is -$323.22. The per-exit-type breakdown requires understanding Kalshi's exact short position accounting, which needs their documentation or a simple test trade.**

## What They Got Right
1. Fee tracking from fills appears accurate (~$90 fill fees + ~$90 settlement fees = ~$180 total)
2. The journal vs reality gap IS real and critical
3. The system needs proper fill-level reconciliation
4. GRADUATED_EXIT is the exit type worth scrutinizing (not EXIT_BLOWN)

## Top 3 Actual Money Bleeds (Best Estimate)
1. **Overall strategy lost money**: -$323 on $511 bankroll = -63% return
2. **Journal tracking is broken**: Can't determine which subsystem (entries, exits, hedges) is the actual cost center because the accounting is wrong
3. **High-volume short selling**: 81 tickers had short positions at settlement. The bot was aggressively selling NO (shorting) to create YES-equivalent exposure, generating massive fill volume (1,766 fills for 233 settlements = 7.6 fills per settlement)

## Recommended Code Changes (Priority Order)
1. **FIX EXIT_BLOWN false positives** - Two concrete bugs identified:
   - **Timezone bug in `get_todays_hourly()`**: `now_utc.hour` used as index into local-time data. Fix: convert UTC hour to local hour using the city's timezone before slicing. One-line fix.
   - **Dynamic peak_hour from forecast**: Computed from all 24 hours including future. Fix: use the hardcoded `peak_hour` from city config (already set to 14-15 for all cities) OR only compute from observed hours.
   - Optional: cross-check Open-Meteo against NWS ASOS/AWOS data for the settlement station.
   - This is a real bug worth ~$56.70 in missed gains.
2. **FIX JOURNAL P&L TRACKING** - Record actual sell price and compute real P&L for EXIT_BLOWN and EXIT_GRADUATED events. Stop computing phantom settlement P&L for exited positions. Net hedge P&L against original position.
3. **Build fill-level reconciliation module** - Pull all fills from Kalshi API, compute actual cashflow per ticker (accounting for Kalshi's short position mechanics), compare to journal per-trade. Must reconcile to within $5 of Kalshi balance.
4. **Add client_order_id dedup** - Prevent duplicate orders.
5. **No-overclose enforcement** - The OKC runaway (889 contracts) shows the system can spiral.
6. **THEN** re-evaluate strategy with real numbers.

## Go/No-Go
**NO-GO on real money until:**
- Journal tracking is fixed and reconciles within $5 of Kalshi balance
- Per-trade P&L can be verified against fills
- At least 30 days of clean data with working accounting
