#!/usr/bin/env python3
"""Dashboard Updater v3.1 — Per-city strategies, portfolio allocation, strategy comparison."""

import json
import os
import statistics
from datetime import datetime, timezone

MARKETS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/active-markets.json"
ANALYSIS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/analysis.json"
TRADES_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/trades.json"
ACCURACY_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/accuracy.json"
BACKTEST_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/backtest-results.json"
STRATEGIES_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/city_strategies.json"
OUTPUT = "/home/ubuntu/.openclaw/workspace/dashboard/data.json"
DASHBOARD_FILE = OUTPUT  # alias for loading existing data
START_TIME = "2026-02-13T23:00:00+00:00"


def safe_load(path):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def format_uptime():
    start = datetime.fromisoformat(START_TIME)
    now = datetime.now(timezone.utc)
    delta = now - start
    hours = int(delta.total_seconds() // 3600)
    mins = int((delta.total_seconds() % 3600) // 60)
    return f"{hours}h {mins}m"


def compute_source_reliability(analysis):
    source_stats = {}
    for series, cdata in analysis.get("cities", {}).items():
        city = cdata.get("city", "?")
        for event in cdata.get("events", []):
            forecasts = event.get("source_forecasts", {})
            mean = event.get("ensemble_mean")
            if not forecasts or mean is None:
                continue
            for src, temp in forecasts.items():
                if src not in source_stats:
                    source_stats[src] = {"deviations": [], "cities": set()}
                source_stats[src]["deviations"].append(abs(temp - mean))
                source_stats[src]["cities"].add(city)
    result = {}
    for src, info in source_stats.items():
        result[src] = {
            "cities_count": len(info["cities"]),
            "data_points": len(info["deviations"]),
            "avg_deviation": round(statistics.mean(info["deviations"]), 2) if info["deviations"] else 0,
        }
    return result


def compute_portfolio_info(trades_data, strategies):
    """Compute portfolio and per-city exposure info."""
    portfolio_cfg = strategies.get("portfolio", {}) if strategies else {}
    starting_capital = portfolio_cfg.get("starting_capital_cents", 50000)
    reserve_pct = portfolio_cfg.get("reserve_pct", 0.0)

    trade_list = trades_data.get("trades", [])
    realized_pnl = sum(t.get("pnl_cents", 0) or 0 for t in trade_list)
    portfolio_value = starting_capital + realized_pnl
    available_capital = int(portfolio_value * (1.0 - reserve_pct / 100.0))
    reserve = portfolio_value - available_capital

    # Per-city exposure
    city_exposure = {}
    city_trades_today = {}
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for t in trade_list:
        city = t.get("city", "Unknown")
        if t.get("status") in ("open", "pending_settlement"):
            city_exposure[city] = city_exposure.get(city, 0) + t.get("cost_cents", 0)
        ts = t.get("timestamp", "")[:10]
        if ts == today_str:
            city_trades_today[city] = city_trades_today.get(city, 0) + 1

    open_cost = sum(t.get("cost_cents", 0) for t in trade_list if t.get("status") in ("open", "pending_settlement"))
    info = {
        "portfolio_value_cents": portfolio_value,
        "portfolio_value_dollars": round(portfolio_value / 100, 2),
        "available_capital_cents": available_capital,
        "available_capital_dollars": round(available_capital / 100, 2),
        "reserve_cents": reserve,
        "reserve_dollars": round(reserve / 100, 2),
        "reserve_pct": reserve_pct,
        "realized_pnl_cents": realized_pnl,
        "open_cost_cents": open_cost,
        "open_cost_dollars": round(open_cost / 100, 2),
        "city_exposure": {city: {"cost_cents": cost, "cost_dollars": round(cost / 100, 2),
                                  "pct_of_portfolio": round(cost / max(1, portfolio_value) * 100, 1)}
                          for city, cost in city_exposure.items()},
        "city_trades_today": city_trades_today,
    }

    # For LIVE mode, use Kalshi-synced values as source of truth
    summary = trades_data.get("summary", {})
    if summary.get("mode") == "LIVE":
        if "portfolio_value_cents" in summary:
            info["portfolio_value_cents"] = summary["portfolio_value_cents"]
            info["portfolio_value_dollars"] = round(summary["portfolio_value_cents"] / 100, 2)
        if "available_capital_cents" in summary:
            info["available_capital_cents"] = summary["available_capital_cents"]
            info["available_capital_dollars"] = round(summary["available_capital_cents"] / 100, 2)
        if "pnl_cents" in summary:
            info["realized_pnl_cents"] = summary["pnl_cents"]

    return info


def compute_city_strategies_summary(strategies, portfolio_info):
    """Build city strategies summary for dashboard."""
    if not strategies:
        return {}
    result = {}
    cities = strategies.get("cities", {})
    exposure = portfolio_info.get("city_exposure", {})
    trades_today = portfolio_info.get("city_trades_today", {})
    for city, cfg in cities.items():
        exp = exposure.get(city, {})
        result[city] = {
            "style": cfg.get("style", "balanced"),
            "allocation_pct": cfg.get("capital_allocation_pct", 8.0),
            "current_exposure_pct": exp.get("pct_of_portfolio", 0),
            "current_exposure_dollars": exp.get("cost_dollars", 0),
            "trades_today": trades_today.get(city, 0),
            "max_trades_per_day": cfg.get("max_trades_per_day", 3),
            "kelly_multiplier": cfg.get("kelly_multiplier", 0.25),
            "edge_threshold_base": cfg.get("edge_threshold_base", 0.12),
        }
    return result


def run():
    print("=== KingClaw Dashboard Updater v3.1 ===")
    now = datetime.now(timezone.utc)

    analysis = safe_load(ANALYSIS_FILE)
    trades_data = safe_load(TRADES_FILE)
    strategies = safe_load(STRATEGIES_FILE)
    trade_list = trades_data.get("trades", [])

    # Portfolio info
    portfolio_info = compute_portfolio_info(trades_data, strategies)
    city_strategies_summary = compute_city_strategies_summary(strategies, portfolio_info)

    # Calculate P&L (only from settled trades)
    total_pnl = 0
    won = 0
    lost = 0
    open_count = 0
    today_str = now.strftime("%Y-%m-%d")
    today_pnl = 0
    settled_today = 0
    for t in trade_list:
        if t.get("status") == "open":
            open_count += 1
            continue
        if t.get("pnl_cents") is not None:
            total_pnl += t["pnl_cents"]
            if t["pnl_cents"] > 0:
                won += 1
            elif t["pnl_cents"] < 0:
                lost += 1
            # Check if settled today
            if t.get("settled_at", "")[:10] == today_str:
                today_pnl += t["pnl_cents"]
                settled_today += 1
    win_rate = f"{won}/{won+lost} ({won/(won+lost)*100:.0f}%)" if (won + lost) > 0 else "N/A"

    # For LIVE trading, use realized P&L only (not unrealized mark-to-market)
    summary = trades_data.get("summary", {}) if isinstance(trades_data, dict) else {}
    if summary.get("mode") == "LIVE" and "realized_pnl_cents" in summary:
        total_pnl = summary["realized_pnl_cents"]

    # Markets
    markets = []
    for series, cdata in analysis.get("cities", {}).items():
        for event in cdata.get("events", []):
            target = event.get("target_date", "")
            mean = event.get("ensemble_mean")
            std = event.get("ensemble_std")
            if mean is None:
                continue
            markets.append({
                "market": f"{cdata['city']} ({target})",
                "contract": f"High Temp",
                "price": round(mean, 1),
                "change": f"±{std:.1f}°F" if std else "+0.0"
            })
            for opp in event.get("opportunities", [])[:2]:
                direction = opp.get("direction", "?")
                ticker = opp.get("ticker", "")
                short_ticker = ticker.split("-")[-1] if "-" in ticker else ticker
                edge = opp.get("edge", 0)
                mkt_price = opp.get("market_price", 0)
                markets.append({
                    "market": f"  └ {cdata['city']}",
                    "contract": f"{direction} {short_ticker}",
                    "price": round(mkt_price * 100) / 100,
                    "change": f"+{edge:.0%} edge"
                })

    # Activity feed
    recent = sorted(trade_list, key=lambda t: t.get("timestamp", ""), reverse=True)[:8]
    activity = []
    for t in recent:
        ts = t.get("timestamp", "")
        try:
            dt = datetime.fromisoformat(ts)
            time_str = dt.strftime("%H:%M")
        except:
            time_str = "??:??"
        emoji = "📈" if t["direction"] == "YES" else "📉"
        style_tag = f" [{t.get('strategy_style', '')}]" if t.get("strategy_style") else ""
        activity.append({
            "time": time_str,
            "event": f"{emoji} {t['direction']} {t.get('ticker','?')} @ {t['entry_price_cents']}¢ ({t.get('city','')}){style_tag}",
            "type": f"{t.get('edge',0):.0%} edge" if t.get("edge") else "trade"
        })

    for series, cdata in analysis.get("cities", {}).items():
        for event in cdata.get("events", []):
            forecasts = event.get("source_forecasts", {})
            if not forecasts:
                continue
            n_sources = len(forecasts)
            temps = list(forecasts.values())
            if not temps:
                continue
            lo, hi = min(temps), max(temps)
            spread = hi - lo
            city = cdata.get("city", "?")
            target = event.get("target_date", "")
            if event.get("low_confidence"):
                activity.append({
                    "time": now.strftime("%H:%M"),
                    "event": f"⚠️ {city} LOW CONFIDENCE: {', '.join(event.get('low_confidence_reasons', []))}",
                    "type": "warning"
                })
            elif spread <= 6:
                activity.append({
                    "time": now.strftime("%H:%M"),
                    "event": f"✅ {n_sources}/{n_sources} sources agree: {city} high {lo:.0f}-{hi:.0f}°F ({target})",
                    "type": "consensus"
                })
            else:
                std = statistics.pstdev(temps)
                activity.append({
                    "time": now.strftime("%H:%M"),
                    "event": f"⚠️ {city} spread {spread:.0f}°F ({lo:.0f}-{hi:.0f}°F) — high uncertainty (σ={std:.1f}°F)",
                    "type": "warning"
                })

    activity.insert(0, {
        "time": now.strftime("%H:%M"),
        "event": f"🔄 Dashboard updated — {len(trade_list)} trades, {len(markets)} signals",
        "type": "system"
    })

    # Agents — preserve existing from data.json, only update weather desc
    existing_data = safe_load(DASHBOARD_FILE)
    agents = existing_data.get("agents", [
        {"icon": "🌤️", "name": "Weather", "desc": "Analyzer + Trader • 6 cities", "status": "Active"},
        {"icon": "🏦", "name": "Econ Trader", "desc": "CPI • Fed • GDP • NFP", "status": "Active"},
        {"icon": "📱", "name": "Mobile App Dev", "desc": "Pocket Mechanic", "status": "Active"},
        {"icon": "💼", "name": "Freelance", "desc": "Hunter + Delivery Bot • Browser scanning", "status": "Active"},
    ])
    # Update weather agent desc with live trade count
    for a in agents:
        if 'Weather' in a['name']:
            a['desc'] = f"Analyzer + Trader • {len(analysis.get('cities',{}))} cities • {len(trade_list)} LIVE trades"

    # Accuracy
    accuracy = safe_load(ACCURACY_FILE)
    accuracy_summary = {
        "mae": 0.0, "direction_accuracy": 0, "best_city": "N/A", "worst_city": "N/A",
        "total_predictions": 0, "best_city_mae": "N/A", "worst_city_mae": "N/A",
        "city_stats": {}
    }
    accuracy_details = {}
    if accuracy.get("overall"):
        ov = accuracy["overall"]
        recent_comps = sorted(accuracy.get("comparisons", []), key=lambda c: c.get("target_date", ""), reverse=True)[:10]
        accuracy_summary = {
            "mae": ov.get("mae"),
            "direction_accuracy": ov.get("direction_accuracy"),
            "total_predictions": ov.get("total_predictions", 0),
            "best_city": ov.get("best_city"),
            "best_city_mae": ov.get("best_city_mae"),
            "worst_city": ov.get("worst_city"),
            "worst_city_mae": ov.get("worst_city_mae"),
            "city_stats": accuracy.get("city_stats", {}),
            "recent_comparisons": [
                {"city": c["city"], "date": c["target_date"],
                 "predicted": c["predicted"], "actual": c["actual"], "error": c["error"]}
                for c in recent_comps
            ],
        }
        accuracy_details = {
            "city_stats": accuracy.get("city_stats", {}),
            "all_comparisons": [
                {"city": c["city"], "date": c["target_date"],
                 "predicted": c["predicted"], "actual": c["actual"],
                 "error": c["error"], "direction_correct": c.get("direction_correct")}
                for c in accuracy.get("comparisons", [])
            ],
        }

    source_reliability = compute_source_reliability(analysis)

    # Backtest with strategy comparison
    backtest_raw = safe_load(BACKTEST_FILE)
    backtest = None
    if backtest_raw.get("summary"):
        backtest = {
            "summary": backtest_raw["summary"],
            "cities": backtest_raw.get("cities", backtest_raw.get("per_city_breakdown", {})),
            "period": backtest_raw.get("period", {}),
            "strategies": backtest_raw.get("strategies"),
            "daily_equity": backtest_raw.get("daily_equity", []),
        }

    analyzer_cities = analysis.get("cities", {})

    dashboard = {
        "status": "online",
        "model": "claude-opus-4-6",
        "uptime": format_uptime(),
        "pnl": {
            "total": round(total_pnl / 100, 2),
            "today": round(today_pnl / 100, 2),
            "trades": len(trade_list),
            "settled_today": settled_today,
            "winRate": win_rate,
            "totalWon": won,
            "totalLost": lost,
            "open_trades": open_count,
        },
        "portfolio": portfolio_info,
        "city_strategies": city_strategies_summary,
        "agents": agents,
        "markets": markets,
        "activity": activity,
        "accuracy": accuracy_summary,
        "trades_list": [{**t, "edge": t.get("edge", 0), "our_prob": t.get("our_prob", 0), "market_price_at_entry": t.get("market_price_at_entry", 0)} for t in trade_list],
        "source_reliability": source_reliability,
        "backtest": backtest,
        "accuracy_details": accuracy_details,
        "analyzer_cities": analyzer_cities,
    }

    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(dashboard, f, indent=2)

    print(f"  Markets/signals: {len(markets)} rows")
    print(f"  Trades: {len(trade_list)} (P&L: ${total_pnl/100:.2f})")
    print(f"  Portfolio: ${portfolio_info['portfolio_value_dollars']:.2f} (avail: ${portfolio_info['available_capital_dollars']:.2f})")
    print(f"  City strategies: {len(city_strategies_summary)} cities")
    print(f"  Activity items: {len(activity)}")
    print(f"  Backtest: {'yes (with strategy comparison)' if backtest and backtest.get('strategies') else 'yes' if backtest else 'no'}")
    print(f"Saved dashboard to {OUTPUT}")
    return dashboard


if __name__ == "__main__":
    run()
