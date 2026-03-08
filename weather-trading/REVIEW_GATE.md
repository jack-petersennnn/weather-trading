# Sub-Agent Review Gate
Created: 2026-03-08

## FIFO Review Checklist
- [ ] Does it actually use FIFO lots, not average cost in disguise?
- [ ] What exact files changed?
- [ ] Is there a backup/migration for legacy portfolio state?
- [ ] Does oversell hard fail?
- [ ] Do tests cover:
  - [ ] Single round trip
  - [ ] Partial sell
  - [ ] Multi-lot sell across FIFO
  - [ ] Multi-instrument isolation
  - [ ] Oversell rejection
- [ ] Do realized + unrealized + cash + open cost basis reconcile logically?
- [ ] Any hidden schema changes?
- [ ] Any unrelated files touched?
- [ ] Does it track YES and NO separately per market (Kalshi-specific)?

### Manual Reconciliation Check
```
buy 10 @ 0.40
buy 5 @ 0.60
sell 12 @ 0.70

Expected FIFO:
  10 from lot 1 → (0.70 - 0.40) × 10 = +3.00
  2 from lot 2  → (0.70 - 0.60) × 2  = +0.20
  Total realized P&L = +3.20
  Remaining: 3 @ 0.60
```

### Merge Rule
Only merge if:
- Scope stayed narrow (no unrelated refactors)
- All tests passed
- Output is understandable
- No weird side effects
- No hand-wavy "should work" language
- Legacy state backed up before any migration

---

## Bias Tracker Review Checklist
- [ ] Is bias definition reused from existing code (sigma_validator.py)?
- [ ] Are both 7d and 14d windows actually computed?
- [ ] Is delta = 7d - 14d?
- [ ] Are flags correct?
  - [ ] stable
  - [ ] warming_shift (delta > 0.5)
  - [ ] cooling_shift (delta < -0.5)
  - [ ] insufficient_data (< 4 samples)
- [ ] Is threshold really 0.5°F?
- [ ] Does the report output look clean and readable?
- [ ] Any unrelated report refactors?
- [ ] Does bias_drift_state.json save correctly?
