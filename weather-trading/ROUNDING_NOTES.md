# Temperature Rounding & Settlement Notes

## How Kalshi Settles
- **Source**: NWS Daily Climate Report (CLI) — the ONLY official source
- **Values**: Whole integer °F (no decimals)
- CLI is quality-controlled and may differ from real-time Time Series data

## ASOS Rounding Chain (creates ambiguity in real-time data)
1. ASOS records 1-minute average (OMO) → rounded to whole °F
2. °F → °C conversion
3. °C rounded to nearest whole °C  
4. °C → °F conversion (displayed on Time Series graph)
5. Rounded to nearest °F (displayed on Time Series list)

**Impact**: A displayed 70°F could actually be 69-71°F due to C↔F rounding.
This affects our METAR intraday tracking — the temperature we see may be ±1°F off.

## Hourly vs 5-Minute Data
- **Hourly (METAR, :51-:54)**: Higher precision, less rounding ambiguity
- **5-Minute**: Subject to full rounding chain, use with caution
- **OMO (1-min)**: Most accurate but not publicly available
- **SPECI**: Exact readings, triggered by weather changes

## Our Probability Calculations (Continuity Correction)
Since settlement is on whole integers:
- **Bracket "67° to 68°"**: P(66.5 < temp < 68.5) — includes both 67 and 68
- **Threshold "69° or above"**: P(temp > 68.5) — temp rounds to ≥69
- **Threshold "60° or below"**: P(temp < 60.5) — temp rounds to ≤60

## Key Insight for Edge Cases
When our forecast puts temp right at a boundary (e.g., 68.5°F for "69° or above"):
- The NWS rounding chain means the ACTUAL sensor reading could be 67.5-69.5°F 
  and still display as 69°F on the CLI
- This creates ~1°F of additional uncertainty at boundaries
- Our std already captures most of this, but edge bets are riskier than they appear
