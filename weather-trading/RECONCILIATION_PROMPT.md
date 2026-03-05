# Kalshi Ledger Reconciliation — Full Context for Analysis

## The Problem
We have a weather trading bot on Kalshi (prediction market exchange). The FIFO lot engine that tracks P&L doesn't match reality. We need to build a balance simulator that, given all fills and settlements, reproduces the exact ending balance.

**Known values:**
- Starting balance: $510.76 (51,076¢)
- Ending balance: $187.54 (18,754¢) — confirmed via API
- Actual P&L: -$323.22
- No deposits or withdrawals occurred
- Zero open positions currently

## What Kalshi Is (Correction from Account Owner)
Prediction markets work exactly like stocks with a floor of $0 and ceiling of $1:
- You BUY a contract (YES or NO) at some price
- You wait for the price to move
- You SELL the contract at the current market price
- **When you sell, you receive the SELL PRICE of the contract you're selling — NOT the other side's price**
- YES + NO always = $1.00 (they're complementary)
- At settlement: winning side pays $1, losing side pays $0

**Example:** Buy NO @ 28¢, price rises to 40¢, sell NO @ 40¢. You receive 40¢. Profit = 12¢. Simple as that.

## Data We Have

### Fills (1,766 total from `/portfolio/fills`)
Each fill has these fields:
```json
{
  "action": "buy" or "sell",
  "side": "yes" or "no",
  "count": 12,              // number of contracts
  "yes_price": 2,           // YES price in cents (integer)
  "no_price": 98,           // NO price in cents (integer)
  "price": 0.02,            // price in dollars (float) — always equals yes_price/100
  "fee_cost": "0.0200",     // fee in dollars (string)
  "is_taker": true,
  "market_ticker": "KXHIGHTDAL-26FEB25-T80",
  "ts": 1771995633          // unix timestamp
}
```

**Important:** `yes_price + no_price = 100` for ALL 1,766 fills (verified, zero exceptions).

**Fill breakdown by type:**
- (yes, buy): 526 fills, 1,019 contracts
- (yes, sell): 255 fills
- (no, buy): 428 fills
- (no, sell): 557 fills, 954 contracts

**Important discovery:** Kalshi can FLIP the side between order and fill. An order placed as "sell yes" can appear in fills as "sell no". The fill data reflects what actually happened on the exchange. Always use fill data, not order data.

### Settlements (233 total from `/portfolio/settlements`)
```json
{
  "ticker": "KXHIGHTLV-26FEB24-B71.5",
  "market_result": "yes" or "no" or "scalar",
  "revenue": 0,             // cents — what Kalshi paid us
  "value": 100,             // settlement value in cents
  "yes_count": 1,           // total YES contracts involved
  "no_count": 1,            // total NO contracts involved  
  "yes_total_cost": 5,      // sum of yes_price * qty for all YES fills (volume metric, NOT cost basis)
  "no_total_cost": 94,      // sum of no_price * qty for all NO fills (volume metric)
  "fee_cost": "0.0100"      // settlement fee in dollars
}
```

### Settlement Revenue Formula (VERIFIED — 0 mismatches across all 233)
```
If YES wins: revenue = max(yes_count - no_count, 0) × 100
If NO wins:  revenue = max(no_count - yes_count, 0) × 100
```

This means Kalshi NETS YES and NO positions before settlement. If you hold equal YES and NO contracts, revenue = 0 (they cancel out).

### Orders (1,146 executed)
Orders have `taker_fill_cost` and `maker_fill_cost` fields:
- 108 orders have maker_fill_cost > 0, totaling $523.20
- Total taker_fill_cost across all orders: $5,919.40

### Fees
- Fill fees total: $89.76 (8,976¢)
- Settlement fees total: $89.76 (8,976¢)  
- Grand total fees: $179.52 (17,952¢)
- No rounding issues (raw sum matches rounded sum exactly)

### Historical Data
- Historical cutoff: March 2025 (before this account existed)
- `/historical/fills` returns empty — all data is in live endpoints
- No missing data

## Key Balance Equation
```
ending_balance = starting_balance - net_cost_of_positions + settlement_revenue - all_fees
18,754 = 51,076 - net_cost + 36,600 - 17,952
net_cost = 50,970¢ = $509.70
```

So whatever model we build, the NET cost of all position entries and exits must equal $509.70.

## What We've Tried and Why It Failed

### Model 1: BUY = pay side_price, SELL = receive side_price
```
buy yes: balance -= yes_price × qty
sell yes: balance += yes_price × qty
buy no: balance -= no_price × qty  
sell no: balance += no_price × qty
```
**Result: $4,266.61 ending balance. Gap = +$4,079.07**
Way too much money. Sell volume ($5,269) dwarfs buy volume ($1,700), so the model gives back too much cash. But this is how Tucker says it should work (like stocks).

### Model 2: BUY = pay side_price, SELL = receive OTHER side's price
```
buy yes: balance -= yes_price × qty
sell yes: balance += no_price × qty  
buy no: balance -= no_price × qty
sell no: balance += yes_price × qty
```
**Result: $217.71 ending balance. Gap = +$30.17**
Closest model (99.4% accurate), never goes negative. But Tucker (the account owner) says this is wrong — when you sell NO @ 40¢, you get 40¢, not the YES price.

### Model 3: Every fill is a debit (no credits from sells)
Based on: Kalshi article says "sell NO = buy YES" and orders show `taker_fill_cost` for sells too.
```
buy yes / sell no: balance -= yes_price × qty
buy no / sell yes: balance -= no_price × qty
collateral return when yes_held > 0 AND no_held > 0: return min(yes,no) × 100
```
**Result: -$1,697.39. Gap = -$1,884.93**
Collateral return of $527 is way too low.

### Model 4: Position-aware (sell closing = credit, sell opening = debit)
Track inventory per ticker. Selling owned contracts returns side_price. Selling unowned posts collateral.
**Result: Various, -$1,846 to -$1,895 range**
Goes negative (impossible on Kalshi which is fully collateralized).

### YES-Normalization FIFO Engine (original ledger.py)
Normalizes everything to YES exposure. SELL NO becomes BUY YES at (100-no_price).
**Result: -$615.90. Gap = -$292.68**
624 overclose events where engine tried to close more than it held.

### With Short Lot Support Added
Overclosures open short YES lots instead of being dropped.
**Result: -$806.34 to -$840.42. Gap got WORSE (-$483 to -$517)**

## The Paradox
- The simple stock model (buy/sell at face value) gives +$4,079 gap — way too positive
- The "other side" model gives +$30 gap — almost perfect but conceptually wrong per the account owner  
- Every other model gives large negative gaps

**The +$4,079 gap means the simple model thinks we received $4,079 MORE than we actually did.** Since sell volume >> buy volume, the model is crediting too much on sells. Something is reducing the effective credit from sells.

## Possible Explanations We Haven't Fully Explored
1. **Collateral mechanics on sells**: When you sell contracts you don't own (short sell), Kalshi might NOT credit you the sell price — instead you post collateral. But we can't distinguish "sell to close" from "sell to open" in the fill data.

2. **Collateral return**: Kalshi has a feature where holding both YES and NO on the same market returns excess collateral. We confirmed revenue = max(net) × 100 at settlement, which means netted positions return $0 revenue. The collateral must have been returned DURING trading.

3. **The fill side-flipping**: Orders say "sell yes" but fills say "sell no". This means what appears as "sell no" in fills might actually be a "sell yes" that Kalshi converted. The balance impact might be different from what the fill's side field suggests.

4. **Event-level netting**: Weather markets have multiple brackets (B71.5, B73.5, etc.) under one event. Kalshi might do cross-bracket collateral return.

## What We Need
A balance model that:
1. Produces ending balance of $187.54 (±$5 tolerance)  
2. Never goes negative during simulation
3. Uses only the data available in fills + settlements
4. Is conceptually consistent with how Kalshi works

## Available for Testing
- Full Kalshi API access (can query balance, positions, orders, fills, settlements)
- Can place a small test trade when markets open to empirically verify balance deltas
- All 1,766 fills and 233 settlements cached as JSON

## Questions
1. Given that the simple stock model (Model 1) is how prediction markets are SUPPOSED to work but gives a $4,079 gap, what's actually happening? Where is $4,079 going?
2. Why does the "other side" model (Model 2) work so well numerically despite being conceptually wrong?
3. Is there a hybrid model that accounts for position-dependent behavior (closing vs opening) that would close the gap?
4. Should we focus on the empirical test trade first, or is there enough information here to solve it analytically?
