# Rescue Mode Exit Criteria
Created: 2026-03-08
Status: NOT MET

This is a mechanical checklist. Rescue mode comes off when ALL items are checked. No vibes-based decisions.

## Required Before Resuming Live Trading

### Accounting (HARD BLOCKER)
- [x] FIFO P&L engine rewritten and tested (2026-03-08)
- [x] Portfolio values reconcile via dual accounting: balance_sim canonical + FIFO attribution (2026-03-08)
- [x] Complete historical fills pulled from Kalshi API (1,766 fills) (2026-03-08)
- [x] Ledger cleaned: 7,500 duplicate fills + 932 duplicate settlements removed (2026-03-08)
- [x] Balance simulator gap = $30.17 (MECNET collateral residual, 99.4% accuracy) — ACCEPTED (2026-03-08)
- NOTE: $30.17 residual is from unmodeled MECNET cross-bracket collateral netting (76 multi-bracket events). Not a bug.

### Sigma Calibration
- [ ] All 19 cities: 1σ coverage between 55-82%
- [ ] No city below 50% 1σ coverage
- [ ] Boston currently 33.3% → needs to reach 55%+ after multiplier fix
- [ ] Oklahoma City currently 44.4% → needs to reach 55%+ after multiplier fix
- [ ] Seattle currently 100% → needs to tighten below 82%
- [ ] Minimum 30 days of clean forecast/actual data (currently at 14 days)
- [ ] Isotonic calibration completed (waiting for data)

### Model Stack
- [ ] Training models (HRRR, MET Norway, NWS) promoted to active (need 14 days, currently at ~11)
- [ ] MIN_FAMILIES ≥ 4 for all tradeable cities
- [ ] No model with cMAE > 2.0°F in active ensemble

### Risk Controls
- [ ] kelly_fraction_max > 0 (currently 0.0)
- [ ] Edge persistence requirement validated (2 consecutive scans)
- [ ] Price drift cancellation threshold reviewed (currently 20%)
- [ ] Max position size per market defined and enforced

### Data Quality
- [ ] 7d vs 14d bias tracker operational
- [ ] No city showing regime shift (delta > 0.5°F) at time of re-enable
- [ ] Station mappings verified for all cities (Houston KHOU, Dallas KDFW, etc.)

### Capital
- [ ] Available capital: $93.76 (sufficient for NO-only, threshold-only trades)
- [ ] Maximum loss per trade defined (suggest 5% of available = ~$4.70)

## Nice-to-Have (Not Blocking)
- [ ] Shadow-live period (paper trade for 1 week with FIFO engine)
- [ ] Weekly recalibration includes bias drift section
- [ ] Blown exit counterfactual tracking operational

## Decision Process
When all REQUIRED items are checked:
1. Set kelly_fraction_max to 0.02 (conservative start)
2. Limit to 2-4 best-calibrated cities only
3. NO-only, threshold-only for first 2 weeks
4. Review after 10 settled trades

## Last Reviewed: 2026-03-08
## Reviewed By: Tucker + KingClaw
