#!/usr/bin/env python3
"""
P&L Aggregator — reads trade_journal.json and prints a summary.

Matches ENTRY/ADD actions to EXIT actions by ticker to compute realized P&L.
Run standalone: python3 pnl_aggregator.py
"""

import json
import os
import sys
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JOURNAL_FILE = os.path.join(BASE_DIR, "trade_journal.json")


def load_journal():
    if not os.path.exists(JOURNAL_FILE):
        print(f"No journal file found at {JOURNAL_FILE}")
        sys.exit(1)
    with open(JOURNAL_FILE) as f:
        return json.load(f)


def compute_trades(entries):
    """Match entries to exits by ticker, compute realized P&L per trade."""
    # Group by ticker
    by_ticker = defaultdict(list)
    for e in entries:
        by_ticker[e.get("ticker", "?")].append(e)

    trades = []
    for ticker, actions in by_ticker.items():
        # Sum up entry cost and exit revenue
        entry_contracts = 0
        entry_cost_cents = 0
        exit_contracts = 0
        exit_revenue_cents = 0
        city = None
        direction = None
        series = None

        for a in actions:
            act = a.get("action", "")
            c = a.get("contracts", 0)
            p = a.get("price_cents", 0)

            if act in ("ENTRY", "ADD", "RE_ENTRY", "HEDGE"):
                entry_contracts += c
                entry_cost_cents += c * p
                city = city or a.get("city")
                direction = direction or a.get("direction")
                series = series or a.get("series")
            elif act.startswith("EXIT") or act == "SETTLE":
                exit_contracts += c
                exit_revenue_cents += c * p

        # Determine market type from ticker
        parts = ticker.split("-")
        market_type = "?"
        if len(parts) == 3:
            strike_str = parts[2]
            if strike_str.startswith("B"):
                market_type = "B"
            elif strike_str.startswith("T"):
                market_type = "T"

        realized_pnl = exit_revenue_cents - entry_cost_cents if exit_contracts > 0 else None
        is_settled = any(a.get("action") == "SETTLE" for a in actions)
        is_exited = any(a.get("action", "").startswith("EXIT") for a in actions)

        trades.append({
            "ticker": ticker,
            "city": city or "?",
            "direction": direction or "?",
            "market_type": market_type,
            "entry_contracts": entry_contracts,
            "entry_cost_cents": entry_cost_cents,
            "exit_contracts": exit_contracts,
            "exit_revenue_cents": exit_revenue_cents,
            "realized_pnl": realized_pnl,
            "is_open": not is_settled and not is_exited,
            "is_win": realized_pnl is not None and realized_pnl > 0,
            "is_loss": realized_pnl is not None and realized_pnl < 0,
        })

    return trades


def print_summary(trades):
    closed = [t for t in trades if t["realized_pnl"] is not None]
    open_trades = [t for t in trades if t["is_open"]]

    print("=" * 60)
    print("  P&L AGGREGATOR — Trade Journal Summary")
    print("=" * 60)
    print(f"\nTotal tickers: {len(trades)} | Closed: {len(closed)} | Open: {len(open_trades)}")

    if not closed:
        print("\nNo closed trades to analyze.")
        return

    total_pnl = sum(t["realized_pnl"] for t in closed)
    wins = [t for t in closed if t["is_win"]]
    losses = [t for t in closed if t["is_loss"]]
    print(f"Net P&L: {total_pnl:+d}¢ (${total_pnl/100:+.2f})")
    print(f"Wins: {len(wins)} | Losses: {len(losses)} | "
          f"Win rate: {len(wins)/len(closed)*100:.0f}%")

    # By city
    print(f"\n{'─'*60}")
    print("BY CITY:")
    print(f"  {'City':<18} {'Trades':>6} {'Wins':>5} {'Losses':>6} {'Net P&L':>10}")
    city_stats = defaultdict(lambda: {"trades": 0, "wins": 0, "losses": 0, "pnl": 0})
    for t in closed:
        c = city_stats[t["city"]]
        c["trades"] += 1
        c["wins"] += 1 if t["is_win"] else 0
        c["losses"] += 1 if t["is_loss"] else 0
        c["pnl"] += t["realized_pnl"]
    for city in sorted(city_stats, key=lambda c: city_stats[c]["pnl"], reverse=True):
        s = city_stats[city]
        print(f"  {city:<18} {s['trades']:>6} {s['wins']:>5} {s['losses']:>6} {s['pnl']:>+8d}¢")

    # By direction
    print(f"\n{'─'*60}")
    print("BY DIRECTION:")
    print(f"  {'Dir':<6} {'Trades':>6} {'Win%':>6} {'Net P&L':>10}")
    for d in ["YES", "NO"]:
        dt = [t for t in closed if t["direction"] == d]
        if dt:
            w = sum(1 for t in dt if t["is_win"])
            pnl = sum(t["realized_pnl"] for t in dt)
            print(f"  {d:<6} {len(dt):>6} {w/len(dt)*100:>5.0f}% {pnl:>+8d}¢")

    # By market type
    print(f"\n{'─'*60}")
    print("BY MARKET TYPE:")
    print(f"  {'Type':<10} {'Trades':>6} {'Win%':>6} {'Net P&L':>10}")
    for mt, label in [("B", "Bracket"), ("T", "Above")]:
        mt_trades = [t for t in closed if t["market_type"] == mt]
        if mt_trades:
            w = sum(1 for t in mt_trades if t["is_win"])
            pnl = sum(t["realized_pnl"] for t in mt_trades)
            print(f"  {label:<10} {len(mt_trades):>6} {w/len(mt_trades)*100:>5.0f}% {pnl:>+8d}¢")

    # Top 5 best and worst
    sorted_by_pnl = sorted(closed, key=lambda t: t["realized_pnl"], reverse=True)
    print(f"\n{'─'*60}")
    print("TOP 5 BEST TRADES:")
    for t in sorted_by_pnl[:5]:
        print(f"  {t['ticker']:<35} {t['direction']:<3} {t['realized_pnl']:>+6d}¢ ({t['city']})")

    print(f"\nTOP 5 WORST TRADES:")
    for t in sorted_by_pnl[-5:]:
        print(f"  {t['ticker']:<35} {t['direction']:<3} {t['realized_pnl']:>+6d}¢ ({t['city']})")

    print(f"\n{'='*60}")


if __name__ == "__main__":
    journal = load_journal()
    trades = compute_trades(journal)
    print_summary(trades)
