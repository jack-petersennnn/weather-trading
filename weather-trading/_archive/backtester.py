#!/usr/bin/env python3
"""Backtester v3.1 — Per-city strategies, dual strategy comparison, equity tracking."""

import json
import math
import os
import random
import statistics
import urllib.request
from datetime import datetime, timezone, timedelta

CITIES = {
    "New York":    {"lat": 40.7831, "lon": -73.9712, "tz": "America/New_York"},
    "Chicago":     {"lat": 41.8781, "lon": -87.6298, "tz": "America/Chicago"},
    "Miami":       {"lat": 25.7617, "lon": -80.1918, "tz": "America/New_York"},
    "Denver":      {"lat": 39.7392, "lon": -104.9903, "tz": "America/Denver"},
    "Los Angeles": {"lat": 34.0522, "lon": -118.2437, "tz": "America/Los_Angeles"},
    "Austin":      {"lat": 30.2672, "lon": -97.7431, "tz": "America/Chicago"},
}

OUTPUT = "/home/ubuntu/.openclaw/workspace/weather-trading/backtest-results.json"
HIST_OUTPUT = "/home/ubuntu/.openclaw/workspace/weather-trading/historical-temps.json"
STRATEGIES_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/city_strategies.json"

SOURCE_ERROR_STD = {
    "NWS Forecast": 2.0, "NWS Hourly": 1.8, "ECMWF": 2.5, "GFS": 3.0,
    "Best Match": 2.2, "Ensemble ICON": 2.8, "Ensemble GFS": 3.2, "Ensemble ECMWF": 2.6,
}

SOURCE_WEIGHTS = {
    "NWS Hourly": 1.5, "ECMWF": 1.4, "NWS Forecast": 1.2,
    "Best Match": 1.1, "GFS": 1.0, "Ensemble ICON": 0.9,
    "Ensemble GFS": 0.9, "Ensemble ECMWF": 0.9,
}

CITY_CALIBRATION = {
    "New York": 1.3, "Chicago": 1.2, "Miami": 0.85,
    "Denver": 1.4, "Los Angeles": 0.8, "Austin": 1.1,
}


def load_strategies():
    try:
        if os.path.exists(STRATEGIES_FILE):
            with open(STRATEGIES_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return None


def get_city_strategy(strategies, city):
    if not strategies:
        return {"style": "balanced", "edge_threshold_base": 0.12, "max_trades_per_day": 3,
                "kelly_multiplier": 0.25, "capital_allocation_pct": 8.0,
                "min_sources_required": 7, "max_source_spread_f": 6.0, "skip_if_bimodal": False}
    return strategies.get("cities", {}).get(city, strategies.get("defaults", {}))


def fetch_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "KingClaw-Backtester/3.1"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"    ✗ {e}")
        return None


def get_historical_temps(lat, lon, tz, start_date, end_date):
    tz_encoded = tz.replace("/", "%2F")
    url = (f"https://archive-api.open-meteo.com/v1/archive?"
           f"latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}"
           f"&daily=temperature_2m_max&temperature_unit=fahrenheit&timezone={tz_encoded}")
    data = fetch_json(url)
    if not data or "daily" not in data:
        return {}
    dates = data["daily"].get("time", [])
    temps = data["daily"].get("temperature_2m_max", [])
    return {d: t for d, t in zip(dates, temps) if t is not None}


def norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def compute_probability(mean, std, floor, cap, is_tail=False):
    if std <= 0:
        std = 3.0
    if floor is not None and cap is not None:
        prob = norm_cdf((cap - mean) / std) - norm_cdf((floor - mean) / std)
    elif floor is not None:
        prob = 1 - norm_cdf((floor - mean) / std)
    elif cap is not None:
        prob = norm_cdf((cap - mean) / std)
    else:
        prob = 0.5
    if is_tail:
        prob *= 0.85
    return max(0.001, min(0.999, prob))


def is_tail_bracket(floor, cap, mean, std):
    if floor is not None and cap is None:
        return floor > mean + 2 * std
    if cap is not None and floor is None:
        return cap < mean - 2 * std
    if floor is not None and cap is not None:
        mid = (floor + cap) / 2
        return abs(mid - mean) > 2.5 * std
    return False


def compute_edge_threshold_strategy(city, std, sources_agreeing, days_out, strategy):
    """Use per-city strategy base threshold with dynamic adjustments."""
    base = strategy.get("edge_threshold_base", 0.12)
    threshold = base
    if sources_agreeing >= 8:
        threshold = max(threshold - 0.04, 0.06)
    elif sources_agreeing >= 6:
        threshold = max(threshold - 0.02, 0.06)
    if days_out == 0:
        threshold = max(threshold - 0.03, 0.04)
    elif days_out >= 2:
        threshold += 0.03
    if std < 2.0:
        threshold = max(threshold - 0.02, 0.04)
    return max(0.04, min(0.30, threshold))


def compute_edge_threshold_flat(city, std, sources_agreeing, days_out=0):
    """Old-style flat threshold."""
    return 0.15


def simulate_day(actual_temp):
    forecasts = {}
    for source, err_std in SOURCE_ERROR_STD.items():
        if random.random() < 0.10:
            continue
        noise = random.gauss(0, err_std)
        forecasts[source] = round(actual_temp + noise, 1)
    if not forecasts:
        return None, None
    temps = list(forecasts.values())
    names = list(forecasts.keys())
    weights = [SOURCE_WEIGHTS.get(n, 1.0) for n in names]
    total_w = sum(weights)
    w_mean = sum(t * w for t, w in zip(temps, weights)) / total_w
    if len(temps) > 1:
        variance = sum(w * (t - w_mean) ** 2 for t, w in zip(temps, weights)) / total_w
        w_std = math.sqrt(variance)
    else:
        w_std = 3.0
    if w_std < 1.0:
        w_std = 1.5
    sources_agreeing = sum(1 for t in temps if abs(t - w_mean) <= 3.0)
    source_spread = max(temps) - min(temps)
    return forecasts, {
        "mean": round(w_mean, 1), "std": round(w_std, 2),
        "sources_agreeing": sources_agreeing, "n_sources": len(temps),
        "source_spread": round(source_spread, 1),
    }


def generate_strikes(actual_temp):
    center = round(actual_temp / 2) * 2
    brackets = []
    for offset in range(-10, 12, 2):
        floor = center + offset
        cap = floor + 2
        brackets.append((floor, cap))
    return brackets


def simulate_market_price(actual_temp, floor, cap):
    true_prob = compute_probability(actual_temp, 2.0, floor, cap)
    noise = random.gauss(0, 0.08)
    return round(max(0.02, min(0.98, true_prob + noise)), 3)


def compute_stats(trades):
    """Compute comprehensive stats for a list of trades."""
    if not trades:
        return {"total_trades": 0, "win_rate": 0, "pnl": 0, "max_drawdown": 0, "sharpe": 0,
                "longest_win_streak": 0, "longest_lose_streak": 0}

    total = len(trades)
    wins = sum(1 for t in trades if t["won"])
    win_rate = round(wins / total * 100, 1) if total else 0
    pnl = sum(t["pnl_cents"] for t in trades)

    # Max drawdown
    equity = 0
    peak = 0
    max_dd = 0
    for t in trades:
        equity += t["pnl_cents"]
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    # Streaks
    win_streak = 0
    lose_streak = 0
    max_win_streak = 0
    max_lose_streak = 0
    for t in trades:
        if t["won"]:
            win_streak += 1
            lose_streak = 0
        else:
            lose_streak += 1
            win_streak = 0
        max_win_streak = max(max_win_streak, win_streak)
        max_lose_streak = max(max_lose_streak, lose_streak)

    # Sharpe-like (daily P&L based)
    daily_pnl = {}
    for t in trades:
        d = t.get("date", "")
        daily_pnl[d] = daily_pnl.get(d, 0) + t["pnl_cents"]
    daily_returns = list(daily_pnl.values())
    if len(daily_returns) > 1:
        mean_r = statistics.mean(daily_returns)
        std_r = statistics.pstdev(daily_returns)
        sharpe = round(mean_r / std_r, 3) if std_r > 0 else 0
    else:
        sharpe = 0

    return {
        "total_trades": total, "win_rate": win_rate,
        "pnl": pnl, "pnl_dollars": round(pnl / 100, 2),
        "max_drawdown": max_dd, "max_drawdown_dollars": round(max_dd / 100, 2),
        "sharpe": sharpe,
        "longest_win_streak": max_win_streak, "longest_lose_streak": max_lose_streak,
        "avg_edge": round(statistics.mean([t.get("edge", 0) for t in trades]), 4) if trades else 0,
    }


def run():
    print("╔══════════════════════════════════════════════╗")
    print("║  KingClaw Backtester v3.1 (Per-City Strategy) ║")
    print("╚══════════════════════════════════════════════╝")
    now = datetime.now(timezone.utc)
    print(f"Time: {now.strftime('%Y-%m-%d %H:%M UTC')}\n")

    random.seed(42)

    strategies = load_strategies()
    portfolio_cfg = strategies.get("portfolio", {}) if strategies else {}
    starting_capital = portfolio_cfg.get("starting_capital_cents", 50000)
    reserve_pct = portfolio_cfg.get("reserve_pct", 0.0)
    max_single_pct = portfolio_cfg.get("max_single_trade_pct", 3.0)
    max_city_exposure_pct = portfolio_cfg.get("max_city_exposure_pct", 15.0)

    if strategies:
        print(f"  Strategies loaded: v{strategies.get('version', '?')}")
        print(f"  Starting capital: ${starting_capital/100:.0f} | Reserve: {reserve_pct}%")
    print()

    end_date = (now - timedelta(days=2)).strftime("%Y-%m-%d")
    start_date = (now - timedelta(days=31)).strftime("%Y-%m-%d")

    historical_data = {"generated": now.isoformat(), "period": {"start": start_date, "end": end_date}, "cities": {}}

    # Two strategy simulations
    flat_trades = []
    percity_trades = []
    all_errors = []
    city_results = {}

    # Per-city portfolio tracking for per_city strategy
    percity_equity = starting_capital
    percity_equity_curve = []  # (date, equity)
    flat_equity = starting_capital

    for city, cfg in CITIES.items():
        print(f"\n{'─'*50}")
        print(f"  {city}")
        print(f"{'─'*50}")

        actuals = get_historical_temps(cfg["lat"], cfg["lon"], cfg["tz"], start_date, end_date)
        print(f"  Historical days: {len(actuals)}")

        if not actuals:
            print(f"  ⚠ No data, skipping")
            city_results[city] = {"error": "no historical data"}
            continue

        historical_data["cities"][city] = {"lat": cfg["lat"], "lon": cfg["lon"], "temps": actuals}

        city_multiplier = CITY_CALIBRATION.get(city, 1.0)
        strategy = get_city_strategy(strategies, city)
        city_flat_trades = []
        city_percity_trades = []
        city_errors = []

        for date, actual_temp in actuals.items():
            forecasts, stats = simulate_day(actual_temp)
            if not stats:
                continue

            error = abs(stats["mean"] - actual_temp)
            city_errors.append(error)
            all_errors.append(error)

            raw_std = stats["std"]
            calibrated_std = raw_std * city_multiplier
            sources_agreeing = stats["sources_agreeing"]
            n_sources = stats["n_sources"]
            source_spread = stats["source_spread"]

            brackets = generate_strikes(actual_temp)

            # ─── FLAT STRATEGY ───
            flat_day_count = 0
            for floor, cap in brackets:
                old_prob = compute_probability(stats["mean"], raw_std, floor, cap)
                market_price = simulate_market_price(actual_temp, floor, cap)
                old_gap = abs(old_prob - market_price)

                if old_gap > 0.15 and flat_day_count < 3:
                    direction = "YES" if old_prob > market_price else "NO"
                    entry = int(market_price * 100) if direction == "YES" else int((1 - market_price) * 100)
                    entry = max(1, min(99, entry))
                    actual_in = (floor <= actual_temp < cap)
                    won = (actual_in if direction == "YES" else not actual_in)
                    pnl = (100 - entry) * 10 if won else -entry * 10
                    flat_trades.append({"date": date, "city": city, "won": won, "pnl_cents": pnl,
                                        "edge": round(old_gap, 4), "contracts": 10})
                    city_flat_trades.append({"won": won, "pnl_cents": pnl, "date": date})
                    flat_day_count += 1

            # ─── PER-CITY STRATEGY ───
            # Check source requirements
            min_sources = strategy.get("min_sources_required", 7)
            max_spread = strategy.get("max_source_spread_f", 6.0)
            skip_bimodal = strategy.get("skip_if_bimodal", False)

            if n_sources < min_sources:
                continue
            if source_spread > max_spread:
                continue

            edge_threshold = compute_edge_threshold_strategy(city, calibrated_std, sources_agreeing, 0, strategy)
            max_trades = strategy.get("max_trades_per_day", 3)
            kelly_mult = strategy.get("kelly_multiplier", 0.25)
            city_alloc = strategy.get("capital_allocation_pct", 8.0)
            percity_day_count = 0

            available = percity_equity * (1.0 - reserve_pct / 100.0)

            for floor, cap in brackets:
                if percity_day_count >= max_trades:
                    break

                tail = is_tail_bracket(floor, cap, stats["mean"], calibrated_std)
                new_prob = compute_probability(stats["mean"], calibrated_std, floor, cap, is_tail=tail)
                market_price = simulate_market_price(actual_temp, floor, cap)
                new_gap = abs(new_prob - market_price)

                if new_gap <= edge_threshold:
                    continue

                direction = "YES" if new_prob > market_price else "NO"

                # Kelly sizing
                if direction == "YES":
                    p = new_prob
                    b = (1.0 - market_price) / market_price if market_price > 0 else 0
                    entry = max(1, min(99, int(market_price * 100)))
                else:
                    p = 1.0 - new_prob
                    b = market_price / (1.0 - market_price) if market_price < 1 else 0
                    entry = max(1, min(99, int((1 - market_price) * 100)))

                if b <= 0:
                    continue
                kelly_raw = (b * p - (1 - p)) / b
                if kelly_raw <= 0:
                    continue

                kelly_adj = kelly_raw * kelly_mult
                size = available * kelly_adj * (city_alloc / 100.0)
                max_size = percity_equity * (max_single_pct / 100.0)
                size = max(200, min(size, max_size))
                contracts = max(1, int(size / entry))

                # Skip extreme prices
                if market_price <= 0.02 or market_price >= 0.98:
                    if direction == "YES" and market_price <= 0.02 and new_prob < 0.15:
                        continue
                    if direction == "NO" and market_price >= 0.98 and new_prob > 0.85:
                        continue

                actual_in = (floor <= actual_temp < cap)
                won = (actual_in if direction == "YES" else not actual_in)
                pnl = (100 - entry) * contracts if won else -entry * contracts

                percity_trades.append({
                    "date": date, "city": city, "won": won, "pnl_cents": pnl,
                    "edge": round(new_gap, 4), "contracts": contracts,
                    "kelly": round(kelly_raw, 4), "is_tail": tail,
                    "strategy_style": strategy.get("style", "balanced"),
                })
                city_percity_trades.append({"won": won, "pnl_cents": pnl, "date": date})
                percity_day_count += 1
                percity_equity += pnl

            percity_equity_curve.append({"date": date, "equity": percity_equity})

        # City results
        city_flat_stats = compute_stats([{"won": t["won"], "pnl_cents": t["pnl_cents"], "date": t["date"], "edge": 0.15} for t in city_flat_trades])
        city_percity_stats = compute_stats([{"won": t["won"], "pnl_cents": t["pnl_cents"], "date": t["date"], "edge": 0} for t in city_percity_trades])
        mae = statistics.mean(city_errors) if city_errors else 0

        city_results[city] = {
            "days_analyzed": len(actuals),
            "actual_mean": round(statistics.mean(list(actuals.values())), 1),
            "mae": round(mae, 2),
            "strategy_style": strategy.get("style", "balanced"),
            "flat_strategy": city_flat_stats,
            "per_city_strategy": city_percity_stats,
        }

        print(f"  MAE: {mae:.2f}°F | Style: {strategy.get('style', 'balanced')}")
        print(f"  FLAT:     {city_flat_stats['total_trades']:>3} trades | Win: {city_flat_stats['win_rate']:>5.1f}% | P&L: ${city_flat_stats['pnl_dollars']:>8.2f}")
        print(f"  PER-CITY: {city_percity_stats['total_trades']:>3} trades | Win: {city_percity_stats['win_rate']:>5.1f}% | P&L: ${city_percity_stats['pnl_dollars']:>8.2f}")

    # Overall stats
    flat_stats = compute_stats(flat_trades)
    percity_stats = compute_stats(percity_trades)
    overall_mae = statistics.mean(all_errors) if all_errors else 0

    # Daily equity curve (deduplicated by date)
    daily_equity = []
    seen_dates = set()
    for pt in percity_equity_curve:
        if pt["date"] not in seen_dates:
            seen_dates.add(pt["date"])
            daily_equity.append(pt)

    print(f"\n{'═'*65}")
    print(f"  BACKTEST COMPARISON: FLAT vs PER-CITY STRATEGY")
    print(f"{'═'*65}")
    print(f"  Period: {start_date} → {end_date}")
    print(f"  Overall MAE: {overall_mae:.2f}°F")
    print(f"  Starting capital: ${starting_capital/100:.0f}")
    print(f"")
    print(f"  {'Metric':<25} {'FLAT ($10)':>14} {'PER-CITY (%)':>14} {'Delta':>10}")
    print(f"  {'─'*65}")
    print(f"  {'Total trades':<25} {flat_stats['total_trades']:>14} {percity_stats['total_trades']:>14} {percity_stats['total_trades']-flat_stats['total_trades']:>+10}")
    print(f"  {'Win rate':<25} {flat_stats['win_rate']:>13.1f}% {percity_stats['win_rate']:>13.1f}% {percity_stats['win_rate']-flat_stats['win_rate']:>+9.1f}%")
    print(f"  {'P&L':<25} ${flat_stats['pnl_dollars']:>12.2f} ${percity_stats['pnl_dollars']:>12.2f} ${percity_stats['pnl_dollars']-flat_stats['pnl_dollars']:>+9.2f}")
    print(f"  {'Max drawdown':<25} ${flat_stats['max_drawdown_dollars']:>12.2f} ${percity_stats['max_drawdown_dollars']:>12.2f}")
    print(f"  {'Sharpe ratio':<25} {flat_stats['sharpe']:>14.3f} {percity_stats['sharpe']:>14.3f}")
    print(f"  {'Win streak':<25} {flat_stats['longest_win_streak']:>14} {percity_stats['longest_win_streak']:>14}")
    print(f"  {'Lose streak':<25} {flat_stats['longest_lose_streak']:>14} {percity_stats['longest_lose_streak']:>14}")
    print(f"  {'Final equity':<25} {'N/A':>14} ${percity_equity/100:>12.2f}")
    print(f"{'═'*65}")

    results = {
        "generated": now.isoformat(),
        "backtester_version": "3.1-per-city",
        "period": {"start": start_date, "end": end_date},
        "strategies": {
            "flat": flat_stats,
            "per_city": percity_stats,
        },
        "per_city_breakdown": city_results,
        "daily_equity": daily_equity,
        # Legacy fields for backward compat
        "comparison": {
            "old_v2": {
                "total_trades": flat_stats["total_trades"],
                "win_rate": flat_stats["win_rate"],
                "avg_edge": flat_stats["avg_edge"],
                "pnl_cents": flat_stats["pnl"],
                "pnl_dollars": flat_stats["pnl_dollars"],
            },
            "new_v3": {
                "total_trades": percity_stats["total_trades"],
                "win_rate": percity_stats["win_rate"],
                "avg_edge": percity_stats["avg_edge"],
                "pnl_cents": percity_stats["pnl"],
                "pnl_dollars": percity_stats["pnl_dollars"],
            },
        },
        "summary": {
            "total_trades": percity_stats["total_trades"],
            "win_rate": percity_stats["win_rate"],
            "avg_edge": percity_stats["avg_edge"],
            "mae": round(overall_mae, 2),
            "pnl_cents": percity_stats["pnl"],
            "pnl_dollars": percity_stats["pnl_dollars"],
        },
        "cities": city_results,
        "sample_trades": percity_trades[:50],
    }

    with open(OUTPUT, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved backtest results to {OUTPUT}")

    with open(HIST_OUTPUT, "w") as f:
        json.dump(historical_data, f, indent=2)
    print(f"Saved historical temps to {HIST_OUTPUT}")

    return results


if __name__ == "__main__":
    run()
