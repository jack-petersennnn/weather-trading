# Rescue Mode Analysis — Why Zero Trades

**Generated:** 2026-03-01 19:48 UTC

## Skip Reason Histogram

From `decision_log.jsonl` (100 entries, 15 unique tickers):

| Reason | Count | % |
|--------|-------|---|
| `bracket_disabled_rescue` | 71 | 71% |
| `side_disabled_rescue` | 29 | 29% |

**Every single skip is from rescue-mode filters.** No opportunities passed the first two filters.

## Root Cause

The rescue config has two contradictory constraints:

1. **`threshold_only: true`** → blocks ALL bracket markets (`-B` tickers) → 71% of opportunities killed
2. **`allowed_sides: ["NO"]`** → blocks all YES trades → 29% of opportunities killed

Since Kalshi weather markets are **almost entirely bracket markets** (e.g., `KXHIGHDEN-26FEB25-B69.5`), `threshold_only: true` blocks nearly everything. The few threshold (`-T`) markets that survive are then filtered by `allowed_sides: ["NO"]`, which kills YES-side threshold trades.

**Result: 100% of opportunities are blocked. Zero trades possible.**

## Closest-to-Pass Candidates (Highest Net EV)

| Ticker | Dir | Price | Prob | Edge | Net EV | Reason |
|--------|-----|-------|------|------|--------|--------|
| KXHIGHTMIN-26FEB26-T40 | YES | 12¢ | 0.536 | 41.6% | 41.6¢ | side_disabled_rescue |
| KXHIGHTMIN-26FEB26-T40 | YES | 7¢ | 0.456 | 38.6% | 38.6¢ | side_disabled_rescue |
| KXHIGHTMIN-26FEB26-B42.5 | NO | 62¢ | 0.950 | 33.0% | 33.0¢ | bracket_disabled_rescue |
| KXHIGHTLV-26FEB27-B83.5 | NO | 58¢ | 0.899 | 31.9% | 31.9¢ | bracket_disabled_rescue |
| KXHIGHTLV-26FEB27-B83.5 | NO | 61¢ | 0.926 | 31.6% | 31.6¢ | bracket_disabled_rescue |

These are **strong** opportunities (20-40¢ net EV). The filters are blocking good trades, not bad ones.

## Assessment

**The filters are misconfigured, not working as intended.**

- `threshold_only: true` makes sense if you only want threshold markets, but Kalshi weather is ~70% brackets
- `allowed_sides: ["NO"]` is reasonable caution, but combined with threshold_only it creates a complete blockade
- The filter chain never reaches the quality filters (net EV, spread, abstain zone) because rescue filters kill everything first

## Recommendation

To actually trade in rescue mode, either:
1. **Set `threshold_only: false`** — allow bracket markets (they're the majority of liquidity)
2. **Or set `allowed_sides: ["YES", "NO"]`** — allow YES on threshold markets
3. Keep the quality filters (min_net_ev: 8¢, max_spread: 4¢, abstain zone) as the actual guard rails

The quality filters downstream are well-designed. The problem is the rescue-mode blanket bans never let candidates reach them.

## Cities in Decision Log

All 5 allowed cities appear: Chicago, Denver, Las Vegas, Minneapolis, Phoenix.
Denver dominates (most logged skips) — but this is moot now that Denver is disabled.
