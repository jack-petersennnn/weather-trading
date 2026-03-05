# Weather Trading Bot — Full System Description

## Overview

An automated system that trades daily high temperature contracts on Kalshi (a regulated prediction market exchange). The bot predicts tomorrow's high temperature for 19 US cities, compares its forecast to Kalshi's market prices, and places trades when it detects a statistical edge. It then monitors positions in real-time and manages exits.

**Current status:** Down ~$322 from a $510.76 starting balance (~$189 remaining). 201 settled markets over ~6 days of live trading (Feb 19–24, 2026).

---

## The Market

Kalshi offers binary contracts on daily high temperatures for 19 US cities. Each city/date has multiple contracts at different strikes:

- **Threshold markets (T):** e.g., "Will NYC's high be 69°F or above?" (YES/NO)
- **Bracket markets (B):** e.g., "Will NYC's high be 67–68°F?" (YES/NO)

Each contract settles at $1.00 (YES wins) or $0.00 (NO wins). You buy at the market price (e.g., 15¢ for YES) and either get $1.00 back or lose your stake. Settlement is based on NWS Climatological Reports (official integer °F readings).

**Key market dynamics:**
- YES contracts at low prices (5–20¢) = high payout but low probability (longshots)
- NO contracts at high prices (60–90¢) = low payout but high probability (favorites)
- Market prices roughly reflect crowd consensus probability

---

## Architecture

The system has 5 independent components running on cron:

### 1. Fast Scanner (`fast_scanner.py`) — Every 20 minutes
The entry engine. For each of the 19 cities:

1. **Collects forecasts** from up to 15 weather models/sources (10 active, 5 in training mode):
   - **Active:** ECMWF, GFS, ICON, Canadian GEM, JMA, UKMO, Meteo-France Arpege, Ensemble ECMWF, Ensemble GFS, Ensemble ICON
   - **Training (collecting data but not trading on):** HRRR, MET Norway, Tomorrow.io, Visual Crossing, NWS (too high MAE)
   - Each model provides a predicted daily high temperature in °F

2. **Builds a weighted ensemble forecast:**
   - Per-city bias correction (each model has a known systematic error per city, e.g., "GFS reads 1.91°F low in Atlanta" → add 1.91°F)
   - Family-first averaging (ECMWF base + ECMWF ensemble = one "ecmwf" family vote, prevents double-counting)
   - Per-city model weights (based on 365-day backtested accuracy via ACIS historical data)
   - Models with corrected MAE > 2.0°F are disabled per-city (corrected MAE = sqrt(raw_MAE² − bias²), i.e., the unpredictable component after bias removal)
   - Requires minimum 4 independent model families to trade a city
   - Output: ensemble mean temperature + calibrated standard deviation

3. **Calculates probability for each contract:**
   - Uses normal CDF: P(temp > strike) or P(temp in bracket) based on ensemble mean/std
   - Handles skewed distributions and bimodal detection (when models cluster into two groups)
   - Caps probability at 95% to prevent overconfidence
   - Applies ±0.5°F continuity correction for integer settlement

4. **Computes edge:** `our_probability - market_price`
   - Minimum 10% edge required for threshold markets (8% for NO due to NO bias — see below)
   - Minimum 15% edge for bracket markets (13% for NO)
   - 2σ safety buffer: won't enter if forecast mean ± 2*std overlaps the strike (too uncertain)
   - Evening entries (next-day markets, UTC 22–07) require 20%+ edge and $5 max spend

5. **Position sizing:** Kelly criterion capped at 10% of bankroll
   - Further limited by: max $15/trade, 3% of tradeable capital, source agreement (tight std = bigger, wide std = smaller)
   - Max 1 position per city per date, 3°F minimum strike separation
   - Portfolio exposure cap at 60%, daily exposure cap at $80
   - $50 reserve always held back

6. **NO bias (recently added):** Historical data showed NO bets have 62% win rate vs 8% for YES bets. System now gives NO opportunities a 2% lower edge threshold and +3% virtual bonus in opportunity ranking.

### 2. Position Manager (`position_manager.py`) — Every 15 minutes
Monitors all open positions and decides whether to hold, partially sell, or fully exit.

**Exit logic is purely data-driven — only reacts to forecast shifts, not price movements:**

- **BLOWN exit (immediate):** Actual observed temperature has already made the position impossible to win (e.g., temp hit 69°F and we bet NO on "69° or above"). Uses peak detection to avoid premature calls — only declares blown after peak hour is confirmed via rate-of-change analysis.

- **Graduated exit (proportional to forecast shift severity):**
  - Compares current ensemble forecast to the forecast when we entered
  - Measures "shift severity" = |forecast_shift| / forecast_std (how many standard deviations the forecast moved against us)
  - Requires **consecutive confirmation** (2–3 scans, 20–30 min) before selling to filter noise
  - Sells proportionally: severity 0.5 → 10%, severity 1.0 → 33%, severity 1.5 → 67%, severity 2.0+ → 100%
  - Anti-cascade protection: each subsequent sell requires the severity to be meaningfully worse than the last sell
  - Recovery detection: if forecast shifts back in our favor for 2+ consecutive scans, resets severity tracking

- **Hedging/re-entry: DISABLED** — Previously, after exiting a position, the PM would scan for a new position on the same city/date at a different strike. Analysis showed this was the #1 source of losses — buying both sides of correlated outcomes guaranteed losing the spread. Now disabled via config flag.

### 3. Spike Monitor (`spike_monitor.py`) — Runs continuously (polls every 0.75s)
Detects favorable price spikes on open positions and auto-sells for profit.

- If market price jumps above our entry + minimum profit threshold (1.2x), sells
- Partial sells if win probability is still >40% (lock profit, keep upside)
- Full sell if probability dropped below 40%
- Only sells, never buys — uses `sell_position` API (capped at holdings, can't overshoot)

### 4. Trade Settler (`settle_trades.py`) — Twice daily (9AM + 11AM EST)
Checks NWS Climatological Reports for actual high temperatures and marks trades as won/lost.

### 5. Analyzer (`analyzer.py`) — Core library
Shared by all components. Contains:
- All 15 weather source API integrations
- Weighted ensemble calculation with bias correction
- Probability computation (normal, skewed, bimodal distributions)
- Weather alert detection (severe weather → skip trading or widen std)

---

## Data Pipeline

```
15 Weather APIs → Per-city bias correction → Family averaging → Weighted ensemble
    → Mean ± Std → Normal CDF → Our probability vs Market price → Edge calculation
    → Position sizing (Kelly) → Trade placement → Position monitoring → Exit management
```

### Calibration (Weekly, Sundays 2PM EST)
- Pulls 365 days of actual high temperatures from ACIS (NOAA's historical database)
- Compares each model's historical forecasts to actuals
- Computes per-model, per-city: raw MAE, bias, corrected MAE
- Updates weights (inverse MAE weighting) and bias corrections
- Disables models with corrected MAE > 2.0°F per city
- Currently: GFS is best overall (1.30°F global MAE), NWS worst (5.69–6.85°F)

### Per-City Model Configuration
Each of the 19 cities has its own:
- Model weights (how much to trust each source)
- Bias corrections (systematic offset to subtract)
- Disabled model list (unreliable sources for that specific city)
- Volatility profile (temp_std, avg_daily_change, max_daily_change)

---

## Risk Controls

1. **Circuit breaker:** Pauses trading after 5 consecutive losses or 15% daily loss
2. **Portfolio correlation limit:** If 4+ cities are all betting the same direction (warm/cold), halves sizing on additional same-direction trades
3. **Source spread gate:** If models disagree by >8°F, skip the city entirely. 5°F+ = half size.
4. **2σ buffer:** Won't enter if the strike is within 2 standard deviations of the forecast mean
5. **Probability cap:** Never assumes >95% probability regardless of model consensus
6. **$50 reserve:** Always kept in cash, never traded
7. **Conflict detection:** Won't buy YES on one strike and NO on a nearby strike for the same city
8. **Lockout:** After PM exits a position, scanner can't re-enter that ticker for 2 hours

---

## Historical Performance Analysis

From Kalshi settlement data (201 settled markets, Feb 19–24):

**By initial bet side:**
- **YES-first trades: ~8% win rate** — Almost never correct. YES bets are cheap longshots (avg entry ~18¢) but the ensemble consistently misjudges which side of the strike the temp will land on.
- **NO-first trades: ~62% win rate** — Directionally correct most of the time. But NO bets are expensive (avg entry ~65¢), so the payout per win is small.

**Biggest loss category: "Mixed" trades (both YES and NO on the same ticker)**
- 129 out of 201 settlements had both sides purchased — result of the PM's hedge/re-entry logic
- These mixed trades were overwhelmingly negative because buying both sides at different times guarantees losing the bid-ask spread
- This behavior has now been disabled

**Single biggest loss:** OKC Feb 23 T53 (−$1,087) — caused by a code bug where `sell_position` created new short positions instead of closing existing ones, snowballing from 3 contracts to 1,908 contracts. Bug has been fixed with direction verification and position cap checks.

---

## Current Configuration

```json
{
  "min_edge_threshold": 0.10,        // 10% minimum edge (8% for NO bets)
  "min_edge_bracket": 0.15,          // 15% for brackets (13% for NO)
  "kelly_fraction_max": 0.10,        // Max 10% of bankroll per trade
  "max_trade_cost_cents": 1500,      // $15 max per trade
  "max_trades_per_city": 1,          // 1 position per city per date
  "reserve_cents": 5000,             // $50 always held back
  "max_portfolio_exposure_pct": 0.60, // 60% max deployed
  "max_per_day_exposure_cents": 8000, // $80 max per day
  "hedge_enabled": false,            // NO hedging (was destroying value)
  "reentry_enabled": false,          // NO re-entry after exits
  "no_edge_discount": 0.02,          // NO bets get 2% lower threshold
  "no_selection_bonus": 0.03,        // NO bets get +3% ranking bonus
  "prob_cap": 0.95,                  // Never assume >95% probability
  "min_families": 4,                 // Need 4+ independent model families
  "circuit_breaker_consecutive_losses": 5,
  "circuit_breaker_cooldown_hours": 4
}
```

---

## Known Issues / Open Questions

1. **YES bets are terrible (8% WR)** — The ensemble is systematically wrong on YES longshots. Either the probability model is miscalibrated (too confident on tails) or the market is more efficient than we think on these.

2. **Ensemble std may be too narrow** — If the calibrated std underestimates true uncertainty, we'll overestimate our edge. The 95% probability cap helps but may not be enough.

3. **Normal distribution assumption** — Temperature highs may not follow a perfect normal distribution. The system has basic skewness and bimodal detection but may need more sophisticated distributional modeling.

4. **Settlement boundary effects** — Kalshi settles on integer °F from NWS. The system applies ±0.5°F continuity corrections but edge cases near strike boundaries are tricky.

5. **Small sample size** — Only 6 days of live data. The backtested models (365 days) look good but live trading introduces slippage, timing, and market dynamics the backtest doesn't capture.

6. **Market efficiency** — Kalshi prices may already incorporate the same weather models we use. Our "edge" might be illusory if the market has already priced in the same information.

7. **Position manager exits + scanner re-entries can conflict** — PM sells because forecast shifted, scanner may re-enter on next scan if edge still looks good. The 2-hour lockout helps but isn't perfect.

---

## File Structure

```
weather-trading/
├── fast_scanner.py          # Entry engine (every 20 min)
├── position_manager.py      # Exit management (every 15 min)
├── spike_monitor.py         # Real-time price spike selling
├── settle_trades.py         # Settlement reconciliation
├── analyzer.py              # Core: weather APIs, ensemble, probability
├── kalshi_client.py         # Kalshi API client (RSA-PSS auth)
├── trading_config.json      # All tunable parameters
├── source_weights.json      # Global model weights + MAE data
├── city_model_config.json   # Per-city weights, biases, disabled models
├── city_calibration.json    # Per-city volatility profiles
├── trades.json              # Active trade state
├── trade_journal.json       # Full trade history log
├── live_trade_log.json      # Raw Kalshi order responses
├── circuit_breaker.py       # Loss streak / daily loss protection
├── peak_detector.py         # Intraday peak hour detection
├── edge_calibration.py      # Tracks predicted vs actual edge
├── training_logger.py       # Logs training-mode model forecasts
├── city_logger.py           # Per-city decision logging
├── trade_journal.py         # Structured trade logging
├── pnl_aggregator.py        # P&L calculation
└── keys/                    # Kalshi API credentials
```
