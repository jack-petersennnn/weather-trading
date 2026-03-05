"""
Data-driven peak temperature detection.

Uses observed hourly temps (past hours) combined with forecast (future hours)
to determine if today's high temperature has peaked.

Signals used:
  - Observed max vs forecast max
  - Rate of change (°F/hr over recent hours)
  - Acceleration (is the rate itself declining?)
  - Consecutive declining hours after observed max

Peak confidence levels:
  - "observed": 2+ consecutive declines after max, or 3+ hrs of sustained drop
  - "forecast+declining": past forecast peak AND rate is negative
  - "forecast": relying on forecast alone (early in the day)
  - "none": not enough data
"""

from datetime import datetime, timezone


def detect_peak(hourly_temps, utc_hour, utc_offset_hours=0, forecast_std=None):
    """
    Analyze hourly temps (24-element list, index=UTC hour) to detect peak.
    
    Args:
        hourly_temps: 24-element list of temps (index = UTC hour), from Open-Meteo
        utc_hour: Current UTC hour (0-23)
        utc_offset_hours: City's UTC offset (e.g., -5 for EST, -6 for CST)
        forecast_std: Standard deviation of forecast ensemble (°F). Used to judge
            whether observed vs forecast gap is within normal model uncertainty
            or indicates a real weather event (storm, clearing, etc.)
    
    Returns dict with peak analysis.
    """
    result = {
        "past_peak": False,
        "peak_hour": 15,
        "confidence": "none",
        "observed_max": None,
        "forecast_max": None,
        "rate_of_change": 0.0,
        "acceleration": 0.0,
        "consecutive_declines": 0,
        "local_hour": (utc_hour + utc_offset_hours) % 24,
    }
    
    if not hourly_temps or len(hourly_temps) < 24:
        return result
    
    clean_all = [(i, t) for i, t in enumerate(hourly_temps) if t is not None]
    if not clean_all:
        return result
    
    local_hour = (utc_hour + utc_offset_hours) % 24
    result["local_hour"] = local_hour
    
    # Forecast max (all 24 hours)
    forecast_max_val = max(t for _, t in clean_all)
    forecast_max_hour = [i for i, t in clean_all if t == forecast_max_val][-1]
    forecast_max_local = (forecast_max_hour + utc_offset_hours) % 24
    result["forecast_max"] = forecast_max_val
    result["peak_hour"] = forecast_max_hour
    result["confidence"] = "forecast"
    
    # EARLY EXIT: If it's before 10 AM local, daily peak hasn't happened yet.
    # Nighttime cooling is NOT a peak signal.
    if local_hour < 10:
        return result
    
    # Split into observed (past) vs forecast (future)
    observed = [(i, hourly_temps[i]) for i in range(min(utc_hour + 1, 24))
                if hourly_temps[i] is not None]
    
    if len(observed) < 3:
        return result
    
    # Only consider observed hours during daytime (local 8AM+) for peak detection
    # Nighttime cooling before sunrise is noise
    daytime_start_utc = (8 - utc_offset_hours) % 24  # 8AM local in UTC
    daytime_observed = [(i, t) for i, t in observed if i >= daytime_start_utc]
    
    if len(daytime_observed) < 2:
        # Not enough daytime hours yet
        return result
    
    # Observed max (daytime only)
    obs_max_val = max(t for _, t in daytime_observed)
    obs_max_hour = [i for i, t in daytime_observed if t == obs_max_val][-1]
    result["observed_max"] = obs_max_val
    
    hours_after_max = utc_hour - obs_max_hour
    
    # Rate of change: hourly deltas for daytime observed hours
    day_temps = [t for _, t in daytime_observed]
    deltas = [day_temps[i] - day_temps[i-1] for i in range(1, len(day_temps))]
    
    # Recent rate (last 3 hours)
    recent_deltas = deltas[-3:] if len(deltas) >= 3 else deltas
    avg_rate = sum(recent_deltas) / len(recent_deltas) if recent_deltas else 0
    result["rate_of_change"] = round(avg_rate, 2)
    
    # Acceleration (change in rate)
    if len(deltas) >= 4:
        accel = [deltas[i] - deltas[i-1] for i in range(1, len(deltas))]
        recent_accel = accel[-3:] if len(accel) >= 3 else accel
        avg_accel = sum(recent_accel) / len(recent_accel) if recent_accel else 0
        result["acceleration"] = round(avg_accel, 2)
    
    # Count consecutive declining hours from end of daytime observed data
    consecutive_declines = 0
    for i in range(len(day_temps) - 1, 0, -1):
        if day_temps[i] < day_temps[i-1]:
            consecutive_declines += 1
        else:
            break
    result["consecutive_declines"] = consecutive_declines
    
    # === STORM / TEMPORARY DIP GUARD (data-driven) ===
    # If observed max is significantly below forecast max, we likely haven't hit
    # the real peak yet — could be a storm, cloud cover, or cold front passing.
    # But "significantly" depends on how confident our forecast is (std).
    #
    # Use forecast_std to set the threshold dynamically:
    # - Low std (models agree, e.g. 1.5°F) → tight threshold, small gap = suspicious
    # - High std (models disagree, e.g. 4°F) → wider threshold, bigger gap is normal
    # 
    # Also consider: observed could EXCEED forecast (clearing, etc.)
    # In that case gap is negative = observed ran hotter than expected. That's fine.
    forecast_gap = forecast_max_val - obs_max_val  # positive = obs below forecast
    result["forecast_gap"] = round(forecast_gap, 1)
    
    # Dynamic threshold: 1.0x forecast_std (or 3°F floor if no std provided)
    # If models say ±2°F, then observed being 2°F below forecast is within noise
    # If models say ±4°F, then 4°F below is still plausible
    gap_threshold = max(3.0, forecast_std * 1.0) if forecast_std else 3.0
    result["gap_threshold"] = round(gap_threshold, 1)
    
    # Also look at whether temp is still TRYING to recover (rate trending back up)
    # A storm dip shows: sharp drop then rate starts flattening or reversing
    # A real peak shows: gradual sustained decline
    recovering = False
    if len(deltas) >= 2:
        # If the most recent delta is less negative (or positive) compared to prior,
        # temp might be bouncing back
        latest_delta = deltas[-1]
        prev_delta = deltas[-2] if len(deltas) >= 2 else latest_delta
        recovering = latest_delta > prev_delta + 0.5  # rate improving by > 0.5°F/hr
    result["recovering"] = recovering
    
    # Peak is "close to forecast" if:
    # 1) Observed is within gap_threshold of forecast (normal uncertainty), OR
    # 2) Observed EXCEEDED forecast (gap is negative — ran hotter than expected)
    close_to_forecast = (forecast_gap <= gap_threshold) or (forecast_gap < 0)
    
    # Even if close to forecast, if temp is actively recovering, don't confirm
    if recovering and forecast_gap > 1.0:
        close_to_forecast = False
        result["confidence"] = "recovering"
    
    # === PEAK DETECTION RULES (ordered by confidence) ===
    
    # RULE 1: 2+ consecutive daytime declines after daytime max, local >= 12,
    # AND observed max is close to forecast (not a storm dip)
    if consecutive_declines >= 2 and hours_after_max >= 2 and local_hour >= 12:
        if close_to_forecast:
            result["past_peak"] = True
            result["confidence"] = "observed"
            result["peak_hour"] = obs_max_hour
            return result
        else:
            # Looks like a dip, not a real peak — forecast says higher temps coming
            result["confidence"] = "possible_dip"
    
    # RULE 2: 3+ hours after daytime max with sustained drop, local >= 13,
    # AND observed max is close to forecast
    if hours_after_max >= 3 and avg_rate < -0.5 and local_hour >= 13:
        if close_to_forecast:
            result["past_peak"] = True
            result["confidence"] = "observed"
            result["peak_hour"] = obs_max_hour
            return result
        else:
            result["confidence"] = "possible_dip"
    
    # RULE 3: Past forecast peak AND declining AND local >= 14
    # This rule doesn't need the storm guard because forecast peak hour has passed
    if utc_hour >= forecast_max_hour and avg_rate < 0 and local_hour >= 14:
        result["past_peak"] = True
        result["confidence"] = "forecast+declining"
        result["peak_hour"] = forecast_max_hour
        return result
    
    # RULE 4: Late in the day (local >= 17/5PM) — even if obs < forecast, 
    # the sun is going down. Peak is peak regardless of forecast miss.
    if local_hour >= 17 and consecutive_declines >= 2:
        result["past_peak"] = True
        result["confidence"] = "end_of_day"
        result["peak_hour"] = obs_max_hour
        return result
    
    return result
