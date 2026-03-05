# Weather Temperature Trading Bot — Full System Brief

## Purpose
Automated system that trades daily high temperature binary contracts on **Kalshi** (a US CFTC-regulated prediction market exchange). It predicts tomorrow's high temperature for up to 20 US cities, compares its forecast to market prices, and places trades when it detects a statistical edge. It then monitors positions in real-time and manages exits.

## Current Status (as of Feb 25, 2026)
- **Balance:** $176.93 cash + $15.09 in open positions
- **Started:** ~$510.76 on Feb 19, 2026 (6 days of live trading)
- **Net P&L:** Approximately **-$319** (down ~62%)
- **Trade volume:** 216 entries, 54 hedge trades, 42 add-to-position trades across 101 unique contracts
- **Exits:** 135 graduated (forecast-shift based), 70 blown (temperature already past strike)
- **Direction split:** 147 YES entries (68%) vs 69 NO entries (32%)
- **The bot is losing money and needs help becoming profitable**

---

## The Market

Kalshi offers binary contracts on the **official daily high temperature** (in integer °F, from NWS Climatological Reports) for 20 US cities. For each city/date, there are multiple contracts:

- **Threshold markets (T):** e.g., "Will NYC's high be 69°F or above?" — YES/NO
  - Can also be "less" type: "Will NYC's high be 60°F or below?" — YES/NO
- **Bracket markets (B):** e.g., "Will NYC's high be 67–68°F?" — YES/NO

Each contract settles at **$1.00** (YES wins) or **$0.00** (NO wins). You buy at market price (e.g., 15¢ for YES, 85¢ for NO). If you win, you get $1.00. If you lose, you lose your stake.

**Key dynamics:**
- YES at low prices (5–20¢) = longshots — high payout, low probability
- NO at high prices (60–90¢) = favorites — low payout per contract, high probability
- Market prices roughly reflect crowd consensus probability
- Liquidity is thin — often only a few cents of depth at best bid/ask
- Markets open ~2 days before the target date

**Cities traded:** New York, Chicago, Miami, Denver, Austin, Minneapolis, Washington DC, Atlanta, Philadelphia, Houston, Dallas, Seattle, Boston, Phoenix, Oklahoma City, Las Vegas, San Francisco, San Antonio, New Orleans. (LA excluded due to high forecast error.)

---

## Architecture Overview

The system has 5 independent components running on scheduled cron jobs:

### 1. Fast Scanner (`fast_scanner.py`) — Every 20 minutes
The entry engine. For each city:

**Step 1: Collect forecasts from up to 16 weather models/sources:**
- **Active (weighted, used for trading):** ECMWF, GFS, ICON, Ensemble ECMWF, Ensemble GFS, Ensemble ICON, Canadian GEM, JMA, UKMO, Meteo-France Arpege, Tomorrow.io, Visual Crossing
- **Training (weight=0, data collected but not used yet):** HRRR, MET Norway
- **Disabled globally (too high MAE):** NWS Forecast (5.69°F MAE), NWS Hourly (6.85°F MAE)

**Step 2: Build weighted ensemble forecast:**
- Per-city bias correction for each model (e.g., "GFS reads 1.91°F low in Atlanta" → add 1.91°F)
- Family-first averaging prevents double-counting (ECMWF base + ECMWF ensemble = one "ecmwf" family vote)
- Per-city model weights based on 365-day backtested accuracy (via ACIS/NOAA historical data)
- Models with corrected MAE > 2.0°F are auto-disabled per-city
- Requires minimum **4 independent model families** to trade a city
- Output: **ensemble mean temperature** + **calibrated standard deviation**

**Step 3: Calculate probability for each contract:**
- Uses **normal CDF** (Gaussian distribution): P(temp > strike) or P(temp in bracket)
- Applies ±0.5°F continuity correction for integer settlement
- Caps probability at **95%** to prevent overconfidence
- Has basic skewness and bimodal detection (when models cluster into two groups)

**Step 4: Compute edge:**
- `edge = our_probability - market_price`
- **Minimum 10% edge** for threshold markets (8% for NO bets due to historical NO bias)
- **Minimum 15% edge** for bracket markets (13% for NO)
- **2σ safety buffer:** Won't enter if forecast mean ± 2×std overlaps the strike
- Evening entries (next-day markets, UTC 22–07) require **20%+ edge** and $5 max spend

**Step 5: Position sizing (Kelly criterion):**
- Kelly fraction capped at **10% of bankroll**
- Further limited by: max $15/trade, 3% of tradeable capital
- Source agreement modifier: tight std (≤1.5°F) → +50% size, wide std (≥4.0°F) → -40% size
- Max 1 position per city per date, 3°F minimum strike separation between positions
- Portfolio exposure cap at 60%, daily exposure cap at $80
- $50 reserve always held back (never traded)

**Step 6: NO bias adjustment (recently added):**
- Historical data showed NO bets win 62% vs YES at 8%
- NO opportunities get 2% lower edge threshold to qualify
- NO gets +3% virtual bonus in opportunity ranking

### 2. Position Manager (`position_manager.py`) — Every 15 minutes
Monitors all open positions and decides whether to hold, sell partially, or fully exit. **Purely forecast-driven — does NOT react to price movements.**

**Exit triggers:**
- **BLOWN exit (immediate):** Actual observed temperature has already made the position impossible to win. Uses peak detection algorithm (rate-of-change analysis of hourly temps) to confirm the daily peak has passed before declaring blown.
- **Graduated exit (proportional to forecast severity):**
  - Measures "shift severity" = |forecast_shift| / forecast_std (how many standard deviations the forecast moved against us since entry)
  - Requires **2–3 consecutive confirmations** (20–30 min) before selling to filter noise
  - Sells proportionally: severity 0.5 → 10%, severity 1.0 → 33%, severity 1.5 → 67%, severity 2.0+ → 100%
  - Anti-cascade protection: each subsequent sell requires severity meaningfully worse than last sell
  - Recovery detection: if forecast shifts back in our favor for 2+ consecutive scans, resets severity tracking

**DISABLED features (were causing losses):**
- Hedging (buying opposite side after exiting)
- Re-entry after PM exits a position (was #1 source of losses — buying both sides guaranteed losing the spread)

### 3. Spike Monitor (`spike_monitor.py`) — Continuous (polls every 0.75 seconds)
Detects favorable price spikes on open positions and auto-sells for profit.
- If market price jumps above entry + minimum profit threshold (1.2x), sells
- Partial sells if win probability still >40% (lock profit, keep upside)
- Full sell if probability dropped below 40%
- **268 spike trades logged** over the live period
- Only sells, never buys

### 4. Trade Settler (`settle_trades.py`) — Twice daily (9 AM + 11 AM EST)
Checks NWS Climatological Reports for actual high temperatures and marks trades as won/lost.

### 5. Weekly Recalibration (`recalibrate_weights.py`) — Sundays 2 PM EST
- Pulls 365 days of actual temperatures from ACIS (NOAA historical database)
- Compares each model's predictions to actuals
- Computes per-model, per-city: raw MAE, bias, corrected MAE
- Updates weights (inverse MAE weighting) and bias corrections
- Disables models with corrected MAE > 2.0°F per city

---

## Data Pipeline

```
16 Weather APIs → Per-city bias correction → Family averaging → Weighted ensemble
    → Mean ± Std → Normal CDF → Our probability vs Market price → Edge calculation
    → Kelly position sizing → Trade placement → Position monitoring → Exit management
```

---

## Global Model Performance (365-day backtest)

| Model | Global MAE (°F) | Weight |
|-------|-----------------|--------|
| Ensemble ECMWF | 1.63 | 1.800 |
| ICON | 2.01 | 1.501 |
| ECMWF | 2.07 | 1.523 |
| Ensemble GFS | 2.44 | 1.247 |
| Visual Crossing | 2.46 | 1.253 |
| GFS | 2.66 | 1.171 |
| Ensemble ICON | 2.93 | 1.061 |
| Best Match | 2.97 | 1.080 |
| Tomorrow.io | 3.42 | 0.943 |
| NWS Forecast | 5.69 | 0.000 (disabled) |
| NWS Hourly | 6.85 | 0.000 (disabled) |

*(Canadian GEM, JMA, UKMO, Arpege have per-city weights only. HRRR, MET Norway in training mode.)*

---

## Risk Controls

1. **Circuit breaker:** Pauses trading after 5 consecutive losses OR 15% daily loss → 4-hour cooldown
2. **Portfolio correlation limit:** If 4+ cities all betting same direction (warm/cold), halves sizing on additional same-direction trades
3. **Source spread gate:** Models disagree by >8°F → skip city. 5°F+ → half size. 3°F+ → 75% size.
4. **2σ buffer:** Won't enter if the strike is within 2 standard deviations of the forecast mean
5. **Probability cap:** Never assumes >95% probability
6. **$50 reserve:** Always kept in cash
7. **Conflict detection:** Won't buy YES on one strike and NO on a nearby strike for same city
8. **Re-entry lockout:** After PM exits, scanner can't re-enter that ticker for 2 hours

---

## Trading Configuration

```json
{
  "min_edge_threshold": 0.10,
  "min_edge_bracket": 0.15,
  "kelly_fraction_max": 0.10,
  "max_trade_cost_cents": 1500,
  "max_trades_per_city": 1,
  "reserve_cents": 5000,
  "max_portfolio_exposure_pct": 0.60,
  "max_per_day_exposure_cents": 8000,
  "hedge_enabled": false,
  "reentry_enabled": false,
  "no_edge_discount": 0.02,
  "no_selection_bonus": 0.03,
  "prob_cap": 0.95,
  "min_families": 4,
  "min_sources": 3,
  "max_source_spread": 8.0,
  "lockout_hours": 2,
  "evening_min_edge": 0.20,
  "evening_max_spend_cents": 500,
  "circuit_breaker_consecutive_losses": 5,
  "circuit_breaker_cooldown_hours": 4
}
```

---

## What's Going Wrong — Detailed Loss Analysis

### Problem 1: YES bets are catastrophic (~8% win rate historically)
- 147 out of 216 entries (68%) were YES bets
- YES bets are cheap longshots (typically 5–20¢) but almost never hit
- The ensemble seems systematically wrong about tail probabilities — it thinks temps will exceed thresholds more often than they actually do
- **The bot is heavily biased toward YES entries despite data showing they lose**

### Problem 2: Mixed trades destroyed value (now fixed)
- 129 out of 201 early settled markets had BOTH YES and NO purchased on the same ticker
- This was caused by the Position Manager's hedge/re-entry logic: PM would exit a position, then the scanner or PM would re-enter on the opposite side
- Buying both sides at different times guarantees losing the bid-ask spread
- **Hedging and re-entry are now disabled**, but the damage was done early

### Problem 3: Catastrophic bug — OKC Feb 23 (now fixed)
- A code bug in `sell_position` created new short positions instead of closing existing ones
- A 3-contract position snowballed to 1,908 contracts, causing a **$1,087 single-trade loss**
- Bug was: Kalshi's sell API doesn't cap to holdings — you can "sell" 1000 when holding 3, creating a new short position
- Fixed with explicit direction verification and position cap checks before every sell

### Problem 4: Blown trades are very frequent
- 70 blown exits out of 216 entries (32%) — the temperature already passed the strike before expiry
- This suggests the forecast confidence intervals are too narrow, or the bot is entering positions too close to the strike despite the 2σ buffer

### Problem 5: The normal distribution assumption may be wrong
- Temperatures don't perfectly follow a Gaussian distribution
- Near extreme values (very hot or cold), the tails may be fatter or thinner than Gaussian predicts
- The 95% probability cap is a band-aid but doesn't fix the fundamental distributional mismatch

### Problem 6: Market efficiency
- Kalshi market makers likely use the same weather models (ECMWF, GFS) that we do
- The "edge" we compute may be illusory — if the market already prices in the same information, our calculated edge is noise
- The market may be more efficient on threshold markets (liquid, obvious) and less efficient on brackets (less liquid, harder to price)

### Problem 7: Too many cities, too much trading
- 20 cities × multiple contracts per city = enormous surface area
- Many cities have poor model accuracy (Atlanta: 34 entries, most of any city)
- More trades ≠ more profit when edge is uncertain — it just multiplies losses
- The scanner runs every 20 minutes and enters aggressively

### Problem 8: Forecast std may be miscalibrated
- If the ensemble's standard deviation underestimates true uncertainty, every edge calculation is inflated
- A 1°F systematic bias in std could turn a "10% edge" into no edge at all
- The 365-day backtest calculates MAE but the std calibration method may not capture real-time forecast uncertainty accurately

---

## Trade Journal Summary (Feb 19–25, 2026)

| Metric | Value |
|--------|-------|
| Total entries | 216 |
| Unique contracts | 101 |
| Graduated exits | 135 |
| Blown exits | 70 |
| Hedge entries | 54 |
| Add-to entries | 42 |
| Spike monitor sells | 268 |
| YES entries | 147 (68%) |
| NO entries | 69 (32%) |

**Top cities by entry count:**
Atlanta (34), Minneapolis (24), Phoenix (17), San Antonio (13), Oklahoma City (13), Chicago (12), Washington DC (11), Boston (10)

---

## What I Want Help With

1. **Why is this system losing money?** Is the fundamental approach (weather model ensemble → probability → binary contract trading) viable at all on Kalshi?
2. **What changes would make it profitable?** Specific, actionable changes to the algorithm, risk management, model selection, or trading strategy.
3. **Should I focus on NO-only strategies?** The historical NO win rate (62%) suggests the edge is in favorites, not longshots.
4. **Is the normal distribution the wrong model?** Should I use empirical distributions, quantile regression, or something else for probability estimation?
5. **Am I trading too often?** Would reducing to 3–5 highest-conviction cities and only trading when edge is massive (25%+) improve results?
6. **Position management:** Is the graduated exit approach sound, or should I just hold to settlement and accept the binary outcome?
7. **Market microstructure:** Am I getting adversely selected? Are market makers seeing my orders and adjusting?
8. **Any other blind spots** in the system design that could explain the losses?

---

## Technical Details for Reference

**Settlement:** Kalshi uses integer °F from NWS Climatological Reports (CLI). These come from specific airport weather stations (e.g., Central Park for NYC, Midway for Chicago, KMIA for Miami). The bot uses the correct NWS station coordinates for each city's forecasts.

**Continuity correction:** Since settlement is in integers but forecasts are continuous, the system applies ±0.5°F corrections. For threshold "greater" markets: P(temp ≥ strike+1) = P(temp > strike+0.5). For brackets: P(floor-0.5 < temp < cap+0.5).

**API authentication:** Kalshi uses RSA-PSS signed API requests. Weather data comes from Open-Meteo (free, 10k calls/day), Tomorrow.io (free tier), Visual Crossing (free tier), and NWS (unlimited, US government).

**Infrastructure:** Runs on an AWS EC2 instance (arm64, Linux). Python scripts with no ML framework — pure statistics (normal CDF, weighted averaging, bias correction). Cron-scheduled via OpenClaw (AI assistant platform).
