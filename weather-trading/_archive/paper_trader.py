#!/usr/bin/env python3
"""Paper Trade Engine v3.1 — Per-city strategies, percentage-based capital allocation, Kelly sizing."""

import json
import os
import re
from datetime import datetime, timezone, timedelta

ANALYSIS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/analysis.json"
TRADES_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/trades.json"
MARKETS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/active-markets.json"
STRATEGIES_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/city_strategies.json"

# Trade filter parameters
MIN_VOLUME = 5
MIN_OPEN_INTEREST = 10
MAX_SPREAD_CENTS = 15
MIN_HOURS_BEFORE_SETTLEMENT = 2


def load_trades():
    if os.path.exists(TRADES_FILE):
        with open(TRADES_FILE) as f:
            return json.load(f)
    return {"trades": [], "summary": {"total_trades": 0, "settled": 0, "won": 0, "lost": 0, "pnl_cents": 0}}


def save_trades(data):
    with open(TRADES_FILE, "w") as f:
        json.dump(data, f, indent=2)


def load_strategies():
    """Load per-city strategy configuration."""
    try:
        if os.path.exists(STRATEGIES_FILE):
            with open(STRATEGIES_FILE) as f:
                return json.load(f)
    except Exception as e:
        print(f"  Warning: Could not load city_strategies.json: {e}")
    return None


def get_city_strategy(strategies, city):
    """Get strategy config for a city, falling back to defaults."""
    if not strategies:
        return {
            "style": "balanced", "edge_threshold_base": 0.12,
            "max_trades_per_day": 3, "kelly_multiplier": 0.25,
            "capital_allocation_pct": 8.0, "min_sources_required": 7,
            "max_source_spread_f": 6.0, "skip_if_bimodal": False,
        }
    cities = strategies.get("cities", {})
    defaults = strategies.get("defaults", {})
    return cities.get(city, defaults)


def compute_portfolio_value(trades_data, strategies):
    """Compute current portfolio value from trades and starting capital."""
    portfolio = strategies.get("portfolio", {}) if strategies else {}
    starting_capital = portfolio.get("starting_capital_cents", 50000)

    realized_pnl = 0
    open_cost = 0
    for t in trades_data.get("trades", []):
        if t.get("pnl_cents") is not None:
            realized_pnl += t["pnl_cents"]
        if t.get("status") in ("open", "pending_settlement"):
            open_cost += t.get("cost_cents", 0)

    portfolio_value = starting_capital + realized_pnl
    return portfolio_value, realized_pnl, open_cost


def compute_kelly_size_pct(our_prob, market_price, direction, strategy, portfolio_value, available_capital, portfolio_cfg):
    """Compute position size using Kelly criterion with percentage-based capital allocation.
    Returns (size_cents, contracts, kelly_raw, kelly_adjusted, sizing_info)."""
    if direction == "YES":
        p = our_prob
        b = (1.0 - market_price) / market_price if market_price > 0 else 0
        entry_price_cents = max(1, min(99, int(market_price * 100)))
    else:
        p = 1.0 - our_prob
        b = market_price / (1.0 - market_price) if market_price < 1 else 0
        entry_price_cents = max(1, min(99, int((1 - market_price) * 100)))

    q = 1.0 - p

    if b <= 0:
        return 0, 0, 0.0, 0.0, "b<=0, no edge"

    kelly_raw = (b * p - q) / b
    if kelly_raw <= 0:
        return 0, 0, kelly_raw, 0.0, "negative Kelly — no edge"

    # Apply city's Kelly multiplier
    kelly_multiplier = strategy.get("kelly_multiplier", 0.25)
    kelly_adjusted = kelly_raw * kelly_multiplier

    # City capital allocation
    city_alloc_pct = strategy.get("capital_allocation_pct", 8.0)

    # Size = available_capital * kelly_adjusted * city_allocation_pct / 100
    size_cents = available_capital * kelly_adjusted * (city_alloc_pct / 100.0)

    # Cap at max_single_trade_pct of portfolio
    max_single_pct = portfolio_cfg.get("max_single_trade_pct", 3.0)
    max_size_cents = portfolio_value * (max_single_pct / 100.0)
    size_cents = min(size_cents, max_size_cents)

    # Floor at $2 (200 cents)
    size_cents = max(200, size_cents)

    # Convert to contracts
    contracts = max(1, int(size_cents / entry_price_cents))

    # Recompute actual cost
    actual_cost = contracts * entry_price_cents

    info = (f"kelly_raw={kelly_raw:.3f} kelly_adj={kelly_adjusted:.3f} "
            f"city_alloc={city_alloc_pct}% size={size_cents:.0f}¢ "
            f"contracts={contracts} entry={entry_price_cents}¢")

    return actual_cost, contracts, kelly_raw, kelly_adjusted, info


def check_trade_filters(opp, city, target_date, today_str, city_trade_counts, event_directions, strategy):
    """Apply smart trade filters. Returns (pass, reason)."""

    # Filter 1: Liquidity — volume
    volume = opp.get("volume", 0)
    if volume is not None and volume < MIN_VOLUME:
        return False, f"low volume ({volume})"

    # Filter 2: Liquidity — open interest
    oi = opp.get("open_interest", 0)
    if oi is not None and oi < MIN_OPEN_INTEREST:
        return False, f"low open interest ({oi})"

    # Filter 3: Bid-ask spread
    yes_bid = opp.get("yes_bid", 0) or 0
    yes_ask = opp.get("yes_ask", 0) or 0
    if yes_bid > 0 and yes_ask > 0:
        spread = yes_ask - yes_bid
        if spread > MAX_SPREAD_CENTS:
            return False, f"wide spread ({spread}¢)"

    # Filter 4: Max trades per city per day (from strategy)
    max_trades = strategy.get("max_trades_per_day", 3)
    key = f"{city}:{target_date}"
    if city_trade_counts.get(key, 0) >= max_trades:
        return False, f"max {max_trades} trades/city/day ({strategy.get('style', 'balanced')})"

    # Filter 5: Don't take opposing trades in same event
    event_key = opp.get("_event_ticker", "")
    direction = opp["direction"]
    if event_key in event_directions:
        existing = event_directions[event_key]
        if direction in existing and len(existing[direction]) >= 2:
            return False, "too many same-direction trades in event"

    # Filter 6: Skip stale extreme prices
    mp = opp.get("market_price", 0)
    if mp <= 0.02 or mp >= 0.98:
        if direction == "YES" and mp <= 0.02 and opp.get("our_prob", 0) < 0.15:
            return False, f"stale extreme price ({mp})"
        if direction == "NO" and mp >= 0.98 and opp.get("our_prob", 1) > 0.85:
            return False, f"stale extreme price ({mp})"

    return True, "passed"


def run():
    print("╔══════════════════════════════════════════════╗")
    print("║  KingClaw Trade Engine v4.0 (LIVE 💰)        ║")
    print("╚══════════════════════════════════════════════╝")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

    try:
        with open(ANALYSIS_FILE) as f:
            analysis = json.load(f)
    except FileNotFoundError:
        print("No analysis.json found. Run analyzer first.")
        return None

    strategies = load_strategies()
    portfolio_cfg = strategies.get("portfolio", {}) if strategies else {}
    reserve_pct = portfolio_cfg.get("reserve_pct", 0.0)
    max_city_exposure_pct = portfolio_cfg.get("max_city_exposure_pct", 15.0)
    max_daily_risk_pct = portfolio_cfg.get("max_daily_risk_pct", 8.0)

    trades_data = load_trades()
    existing_tickers = {t["ticker"] for t in trades_data["trades"]}
    new_trades = 0
    filtered_count = 0
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Compute portfolio value
    portfolio_value, realized_pnl, open_cost = compute_portfolio_value(trades_data, strategies)
    available_capital = portfolio_value * (1.0 - reserve_pct / 100.0)

    print(f"\n  Portfolio: ${portfolio_value/100:.2f} (starting={portfolio_cfg.get('starting_capital_cents', 50000)/100:.0f} + pnl={realized_pnl/100:.2f})")
    print(f"  Available capital: ${available_capital/100:.2f} (reserve={reserve_pct}%)")
    print(f"  Open trade cost: ${open_cost/100:.2f}\n")

    # Build settled market lookup
    try:
        with open(MARKETS_FILE) as f:
            markets_raw = json.load(f)
    except:
        markets_raw = {}

    settled_tickers = set()
    for s, sdata in markets_raw.get("series", {}).items():
        for ev in sdata.get("events", []):
            for mkt in ev.get("markets", []):
                if mkt.get("result", "") != "":
                    settled_tickers.add(mkt["ticker"])

    # Track trade counts and city exposure
    city_trade_counts = {}
    event_directions = {}
    city_exposure = {}  # city -> total open cost
    daily_new_cost = 0  # total cost of new trades today

    # Count existing trades
    for t in trades_data["trades"]:
        city = t.get("city", "")
        if t["status"] in ("open", "pending_settlement"):
            et = t.get("event_ticker", "")
            d = t.get("direction", "")
            key = f"{city}:{t.get('_target_date', today_str)}"
            city_trade_counts[key] = city_trade_counts.get(key, 0) + 1
            city_exposure[city] = city_exposure.get(city, 0) + t.get("cost_cents", 0)
            if et not in event_directions:
                event_directions[et] = {}
            if d not in event_directions[et]:
                event_directions[et][d] = []
            event_directions[et][d].append(t["ticker"])

    for series, city_data in analysis.get("cities", {}).items():
        city = city_data["city"]
        strategy = get_city_strategy(strategies, city)

        for event in city_data.get("events", []):
            target_date = event.get("target_date", "")
            event_ticker = event.get("event_ticker", "?")

            # Skip past events
            if target_date and target_date < today_str:
                print(f"  SKIP (past): {event_ticker} date={target_date}")
                continue

            # Skip low confidence events
            if event.get("low_confidence", False):
                print(f"  SKIP (low confidence): {event_ticker} — {', '.join(event.get('low_confidence_reasons', []))}")
                continue

            opps = sorted(event.get("opportunities", []), key=lambda x: x.get("edge", 0), reverse=True)

            for opp in opps:
                if opp["ticker"] in settled_tickers:
                    print(f"  SKIP (settled): {opp['ticker']}")
                    continue
                if opp["ticker"] in existing_tickers:
                    continue

                opp["_event_ticker"] = event_ticker

                # Apply trade filters
                passed, reason = check_trade_filters(
                    opp, city, target_date, today_str,
                    city_trade_counts, event_directions, strategy
                )
                if not passed:
                    print(f"  FILTER: {opp['ticker']} — {reason}")
                    filtered_count += 1
                    continue

                # Per-city exposure check
                current_city_exposure = city_exposure.get(city, 0)
                max_city_cost = portfolio_value * (max_city_exposure_pct / 100.0)
                if current_city_exposure >= max_city_cost:
                    print(f"  FILTER: {opp['ticker']} — city exposure ${current_city_exposure/100:.2f} >= max ${max_city_cost/100:.2f}")
                    filtered_count += 1
                    continue

                # Daily risk check
                max_daily_cost = portfolio_value * (max_daily_risk_pct / 100.0)
                if daily_new_cost >= max_daily_cost:
                    print(f"  FILTER: {opp['ticker']} — daily risk ${daily_new_cost/100:.2f} >= max ${max_daily_cost/100:.2f}")
                    filtered_count += 1
                    continue

                # Kelly + percentage-based sizing
                direction = opp["direction"]
                market_price = opp["market_price"]
                our_prob = opp["our_prob"]

                cost, contracts, kelly_raw, kelly_adj, sizing_info = compute_kelly_size_pct(
                    our_prob, market_price, direction, strategy,
                    portfolio_value, available_capital, portfolio_cfg
                )

                if contracts <= 0:
                    print(f"  SKIP (no Kelly edge): {opp['ticker']} — {sizing_info}")
                    filtered_count += 1
                    continue

                # Entry price
                if direction == "YES":
                    entry_price_cents = max(1, min(99, int(market_price * 100)))
                else:
                    entry_price_cents = max(1, min(99, int((1 - market_price) * 100)))

                cost = entry_price_cents * contracts

                # Check remaining city exposure room
                room = max_city_cost - current_city_exposure
                if cost > room:
                    contracts = max(1, int(room / entry_price_cents))
                    cost = entry_price_cents * contracts

                # Check remaining daily risk room
                daily_room = max_daily_cost - daily_new_cost
                if cost > daily_room:
                    contracts = max(1, int(daily_room / entry_price_cents))
                    cost = entry_price_cents * contracts

                trade = {
                    "ticker": opp["ticker"],
                    "series": series,
                    "city": city,
                    "event_ticker": event_ticker,
                    "_target_date": target_date,
                    "direction": direction,
                    "entry_price_cents": entry_price_cents,
                    "contracts": contracts,
                    "cost_cents": cost,
                    "our_prob": our_prob,
                    "market_price_at_entry": market_price,
                    "edge": opp["edge"],
                    "entry_forecast_high": event.get("ensemble_mean"),
                    "entry_forecast_std": event.get("calibrated_std"),
                    "entry_source_spread": event.get("source_spread_f"),
                    "kelly_fraction": round(kelly_raw, 4),
                    "kelly_adjusted": round(kelly_adj, 4),
                    "sizing_reason": sizing_info,
                    "strategy_style": strategy.get("style", "balanced"),
                    "is_tail": opp.get("is_tail", False),
                    "edge_threshold": opp.get("edge_threshold", 0.12),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "status": "open",
                    "result": None,
                    "pnl_cents": None
                }
                trades_data["trades"].append(trade)
                existing_tickers.add(opp["ticker"])
                city_exposure[city] = city_exposure.get(city, 0) + cost
                daily_new_cost += cost
                new_trades += 1

                # Update tracking
                key = f"{city}:{target_date}"
                city_trade_counts[key] = city_trade_counts.get(key, 0) + 1
                if event_ticker not in event_directions:
                    event_directions[event_ticker] = {}
                if direction not in event_directions[event_ticker]:
                    event_directions[event_ticker][direction] = []
                event_directions[event_ticker][direction].append(opp["ticker"])

                print(f"  NEW TRADE: {direction} {opp['ticker']} @ {entry_price_cents}¢ × {contracts} "
                      f"| edge={opp['edge']:.1%} | kelly={kelly_raw:.3f}→{kelly_adj:.3f} "
                      f"| {city} [{strategy.get('style', '?')}]")

    # Check for settlements
    try:
        with open(MARKETS_FILE) as f:
            markets = json.load(f)
    except:
        markets = {}

    result_lookup = {}
    for series, sdata in markets.get("series", {}).items():
        for event in sdata.get("events", []):
            for mkt in event.get("markets", []):
                if mkt.get("result"):
                    result_lookup[mkt["ticker"]] = mkt["result"]

    settled = 0
    for trade in trades_data["trades"]:
        if trade["status"] != "open":
            continue
        result = result_lookup.get(trade["ticker"])
        if not result:
            event_ticker = trade.get("event_ticker", "")
            trade_target = None
            for s, sdata in markets_raw.get("series", {}).items():
                for ev in sdata.get("events", []):
                    if ev.get("event_ticker") == event_ticker:
                        trade_target = ev.get("end_date", "")[:10]
                        break
            if trade_target and trade_target < today_str:
                if trade["status"] != "pending_settlement":
                    trade["status"] = "pending_settlement"
                    print(f"  PENDING: {trade['ticker']} — target {trade_target} passed")
            continue

        trade["status"] = "settled"
        trade["result"] = result
        settled += 1

        if (trade["direction"] == "YES" and result == "yes") or \
           (trade["direction"] == "NO" and result == "no"):
            trade["pnl_cents"] = (100 - trade["entry_price_cents"]) * trade["contracts"]
            trade["status"] = "won"
        else:
            trade["pnl_cents"] = -trade["cost_cents"]
            trade["status"] = "lost"

        print(f"  SETTLED: {trade['ticker']} → {trade['status']} | P&L: {trade['pnl_cents']}¢")

    # Update summary
    all_trades = trades_data["trades"]
    portfolio_value_final, _, open_cost_final = compute_portfolio_value(trades_data, strategies)

    trades_data["summary"] = {
        "total_trades": len(all_trades),
        "open": sum(1 for t in all_trades if t["status"] in ("open", "pending_settlement")),
        "pending_settlement": sum(1 for t in all_trades if t["status"] == "pending_settlement"),
        "settled": sum(1 for t in all_trades if t["status"] in ("won", "lost")),
        "won": sum(1 for t in all_trades if t["status"] == "won"),
        "lost": sum(1 for t in all_trades if t["status"] == "lost"),
        "pnl_cents": sum(t["pnl_cents"] or 0 for t in all_trades),
        "total_invested_cents": sum(t["cost_cents"] for t in all_trades),
        "portfolio_value_cents": portfolio_value_final,
        "available_capital_cents": int(portfolio_value_final * (1.0 - reserve_pct / 100.0)),
        "open_cost_cents": open_cost_final,
        "avg_kelly": round(
            sum(t.get("kelly_fraction", 0) for t in all_trades if t.get("kelly_fraction")) /
            max(1, sum(1 for t in all_trades if t.get("kelly_fraction"))), 4
        ),
    }

    save_trades(trades_data)
    print(f"\n  New trades: {new_trades} | Filtered: {filtered_count} | Settled: {settled}")
    print(f"  Portfolio: ${portfolio_value_final/100:.2f} | Open cost: ${open_cost_final/100:.2f} | P&L: {trades_data['summary']['pnl_cents']}¢")
    print(f"Saved to {TRADES_FILE}")
    return trades_data


if __name__ == "__main__":
    run()
