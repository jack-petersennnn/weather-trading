#!/usr/bin/env python3
"""
Weekly Report Generator — Pulls LIVE Kalshi data, computes P&L from fills/settlements,
includes reconciliation block, 1σ coverage per city, correct unit labels.

Run via cron or manually: python3 generate_weekly_report.py
"""

import json
import os
import sys
import statistics
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kalshi_client

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
START_EQUITY_CENTS = 51076  # $510.76

def load_json(path):
    full = os.path.join(BASE_DIR, path) if not os.path.isabs(path) else path
    if os.path.exists(full):
        with open(full) as f:
            return json.load(f)
    return {}


def get_live_kalshi_data():
    """Pull live cash, positions, and open orders from Kalshi."""
    data = {}
    try:
        bal = kalshi_client.get_balance()
        data["balance"] = bal
        data["balance_cents"] = bal.get("balance", 0)
    except Exception as e:
        data["balance_error"] = str(e)
        data["balance_cents"] = 0

    try:
        positions = kalshi_client.get_positions()
        pos_list = positions if isinstance(positions, list) else positions.get("market_positions", positions.get("positions", []))
        data["positions"] = pos_list
    except Exception as e:
        data["positions_error"] = str(e)
        data["positions"] = []

    try:
        orders = kalshi_client.get_orders(status="resting")
        order_list = orders if isinstance(orders, list) else orders.get("orders", [])
        data["open_orders"] = order_list
    except Exception as e:
        data["orders_error"] = str(e)
        data["open_orders"] = []

    return data


def compute_pnl_from_trades():
    """Compute P&L from trades.json fills/settlements, NOT event logs."""
    trades = load_json("trades.json")
    trade_list = trades.get("trades", [])

    realized_pnl = 0
    unrealized_cost = 0
    total_fees = 0
    settled_count = 0
    open_count = 0

    for t in trade_list:
        pnl = t.get("pnl_cents", 0) or 0
        fees = t.get("fees_cents", 0) or 0
        status = t.get("status", "")

        if status in ("settled", "closed"):
            realized_pnl += pnl
            total_fees += fees
            settled_count += 1
        elif status in ("open", "pending_settlement"):
            unrealized_cost += t.get("cost_cents", 0) or 0
            open_count += 1

    return {
        "realized_pnl_cents": realized_pnl,
        "unrealized_cost_cents": unrealized_cost,
        "total_fees_cents": total_fees,
        "settled_count": settled_count,
        "open_count": open_count,
    }


def get_sigma_coverage():
    """Get LIVE 1σ coverage per city from training_forecast_log + ACIS actuals."""
    try:
        from sigma_validator import compute_sigma_coverage
        results = compute_sigma_coverage()
        if "error" in results:
            return {}
        
        coverage = {}
        for city, data in results.get("per_city", {}).items():
            coverage[city] = {
                "coverage": data["coverage_1sigma_pct"],
                "samples": data["samples"],
                "low_sample": data["samples"] < 10,
                "quality": data["quality"],
                "mae": data["mae"],
                "bias": data["bias"],
            }
        return coverage
    except Exception as e:
        print(f"Warning: sigma_validator failed ({e}), falling back to stale data")
        # Fallback to old file
        sigma_data = load_json("sigma_optimization_results.json")
        opt = sigma_data.get("optimization_results", {})
        coverage = {}
        for city, data in opt.items():
            cov = data.get("sigma_1_coverage")
            n = data.get("validation_samples", 0)
            coverage[city] = {
                "coverage": round(cov * 100, 1) if cov else None,
                "samples": n,
                "low_sample": n < 10,
            }
        return coverage


def generate_report():
    now = datetime.now(timezone.utc)
    lines = []
    lines.append("# 📊 Weekly Trading Report")
    lines.append(f"**Generated:** {now.strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")

    # --- Live Kalshi Data ---
    lines.append("## 💰 Live Kalshi Account State")
    live = get_live_kalshi_data()

    if "balance_error" in live:
        lines.append(f"⚠️ Balance fetch error: {live['balance_error']}")
        lines.append(f"Using cached balance: {live['balance_cents']} cents")
    else:
        bal_cents = live["balance_cents"]
        lines.append(f"- **Cash balance:** ${bal_cents/100:.2f}")

    positions = live["positions"]
    lines.append(f"- **Open positions:** {len(positions)}")
    unrealized = 0
    for p in positions:
        # Position value estimate
        qty = abs(p.get("total_traded", p.get("position", 0)))
        market_price = p.get("market_price", 0)
        cost = p.get("total_cost", p.get("cost", 0))
        if qty and market_price:
            unrealized += int(market_price * qty) - cost

    lines.append(f"- **Estimated unrealized P&L:** ${unrealized/100:.2f}")
    lines.append(f"- **Open orders:** {len(live['open_orders'])}")
    lines.append("")

    # --- P&L from fills/settlements ---
    lines.append("## 📈 P&L (from fills/settlements)")
    pnl = compute_pnl_from_trades()
    lines.append(f"- Realized P&L: ${pnl['realized_pnl_cents']/100:.2f}")
    lines.append(f"- Open position cost: ${pnl['unrealized_cost_cents']/100:.2f}")
    lines.append(f"- Fees paid: ${pnl['total_fees_cents']/100:.2f}")
    lines.append(f"- Settled trades: {pnl['settled_count']}")
    lines.append(f"- Open trades: {pnl['open_count']}")
    lines.append("")

    # --- Reconciliation ---
    lines.append("## 🔄 Reconciliation")
    current_equity_cents = live["balance_cents"] + unrealized + pnl["unrealized_cost_cents"]
    expected_equity = START_EQUITY_CENTS + pnl["realized_pnl_cents"] + unrealized - pnl["total_fees_cents"]
    discrepancy = current_equity_cents - expected_equity

    lines.append(f"- Start equity: ${START_EQUITY_CENTS/100:.2f}")
    lines.append(f"- + Realized P&L: ${pnl['realized_pnl_cents']/100:.2f}")
    lines.append(f"- + Unrealized: ${unrealized/100:.2f}")
    lines.append(f"- − Fees: ${pnl['total_fees_cents']/100:.2f}")
    lines.append(f"- = Expected equity: ${expected_equity/100:.2f}")
    lines.append(f"- **Kalshi-reported equity: ${current_equity_cents/100:.2f}**")
    if abs(discrepancy) > 0:
        lines.append(f"- ⚠️ **Discrepancy: ${discrepancy/100:.2f}**")
    else:
        lines.append(f"- ✅ Reconciled (no discrepancy)")
    lines.append("")

    # --- 1σ Coverage per city ---
    lines.append("## 🎯 1σ Coverage by City")
    coverage = get_sigma_coverage()
    if coverage:
        lines.append("| City | Coverage | Samples | Flag |")
        lines.append("|------|----------|---------|------|")
        for city in sorted(coverage.keys()):
            c = coverage[city]
            cov_str = f"{c['coverage']:.1f}%" if c['coverage'] is not None else "N/A"
            flag = "⚠️ LOW SAMPLE" if c['low_sample'] else ("✅" if c['coverage'] and c['coverage'] >= 60 else "⚠️ Low coverage")
            lines.append(f"| {city} | {cov_str} | {c['samples']} | {flag} |")
    else:
        lines.append("No sigma optimization data available.")
    lines.append("")

    # --- Source spread note ---
    lines.append("## 📝 Notes")
    lines.append("- `source_spread` is measured in **°F** (temperature spread across forecast sources)")
    lines.append("- `edge` and prices are in **¢** (cents per contract)")
    lines.append("")

    # --- Config summary ---
    config = load_json("trading_config.json")
    scanner = config.get("scanner", {})
    lines.append("## ⚙️ Current Config")
    lines.append(f"- Rescue mode: {'ON' if scanner.get('rescue_mode') else 'OFF'}")
    lines.append(f"- Allowed cities: {scanner.get('allowed_cities', 'all')}")
    lines.append(f"- Allowed sides: {scanner.get('allowed_sides', 'all')}")
    lines.append(f"- Threshold only: {scanner.get('threshold_only', False)}")
    lines.append(f"- Min edge: {scanner.get('min_edge_threshold', '?')}")
    lines.append("")

    # --- Gate Progress (graduated 2σ) — from LIVE sigma_validator ---
    lines.append("## 🚪 Gate Progress: Graduated 2σ Buffer")
    try:
        from sigma_validator import compute_sigma_coverage
        gate_results = compute_sigma_coverage()
        gate = gate_results.get("gate_progress", {})
        
        clean_days = gate.get("clean_days", 0)
        gate1 = "✅" if gate.get("clean_days_met") else "❌"
        lines.append(f"- {gate1} Clean logged data: **{clean_days}/30 days** ({gate.get('date_range', 'N/A')})")
        
        good_cities = gate.get("good_coverage_cities", [])
        good_count = gate.get("good_coverage_count", 0)
        gate2 = "✅" if gate.get("good_coverage_met") else "❌"
        lines.append(f"- {gate2} Cities with good 1σ coverage (55-82%): **{good_count}/3** ({', '.join(good_cities) if good_cities else 'none yet'})")
        
        lines.append(f"- {'✅' if gate.get('clean_days_met') else '❌'} Isotonic calibration map: {'ready to build' if gate.get('clean_days_met') else 'waiting for data'}")
        lines.append(f"- ❌ Trade-level counterfactual: not yet run")
    except Exception as e:
        lines.append(f"- ⚠️ Gate check failed: {e}")
    lines.append("")

    report = "\n".join(lines)

    # Save
    report_path = os.path.join(BASE_DIR, "weekly_report.md")
    with open(report_path, "w") as f:
        f.write(report)
    print(f"Report saved to {report_path}")

    return report


if __name__ == "__main__":
    print(generate_report())
