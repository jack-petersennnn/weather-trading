# Graduated 2σ Buffer — Gate Criteria

## Context
Hard 2σ veto is currently active for ALL trade types (rescue mode).
A graduated/EV-aware 2σ approach was built and tested (Mar 1, 2026) but reverted
because sigma multipliers are unvalidated. This document defines when to revisit.

## Gate Conditions (ALL must be met)

1. **30+ days of clean logged data** (training_forecast_log.json)
   - Target: late March 2026
   
2. **Stable 1σ coverage per city** for at least 3 of 4 active cities
   - Threshold: 60-75% coverage sustained over 2+ consecutive weekly reports
   - Cities: Chicago, Minneapolis, Phoenix, Las Vegas
   
3. **Isotonic calibration map built from own logs** (not historical data)
   - Must use only post-rescue-mode logged forecasts
   
4. **Trade-level counterfactual shows improvement vs baseline**
   - Compare: raw model vs sigma-corrected vs sigma+graduated
   - Must show net positive EV on hypothetical trades

## If conditions are NOT met
Hard 2σ veto stays. Re-evaluate at next monthly checkpoint.

## Graduated approach (ready to deploy when gate passes)
- Strike inside 2σ band → require ≥12¢ net EV (vs normal 8¢)
- Size haircut: `clamp(z_score / 2.0, 0.25, 1.0)`
- Edge persistence still required (2 consecutive scans)
- Code was tested and works — just revert the revert in find_opportunities()

## Data being collected for this decision
- Idle proof logs: top 5 candidates per city per scan (with z-score + EV)
- 1σ coverage tracking per city in weekly reports
- training_forecast_log.json growing daily
