#!/usr/bin/env python3
"""KingClaw Weather Trading System v3.0 — Orchestrator."""

import sys
import os
import json
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import scanner
import calibration
import analyzer
import paper_trader
import live_trader
import accuracy_tracker

# Optional: dashboard may not exist yet
try:
    import update_dashboard
    HAS_DASHBOARD = True
except ImportError:
    HAS_DASHBOARD = False

METADATA_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/pipeline_metadata.json"


def main():
    start = time.time()
    print("╔══════════════════════════════════════════════╗")
    print("║   KingClaw Weather Trading System v4.0 LIVE   ║")
    print("║   Weighted Ensemble + Kelly + Calibration     ║")
    print("╚══════════════════════════════════════════════╝")
    print(f"Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n")

    results = {}

    # Step 0: Sync with Kalshi (source of truth)
    print("─" * 44)
    print("STEP 0/7: Syncing with Kalshi")
    print("─" * 44)
    try:
        sync_data = live_trader.sync_from_kalshi()
        results["kalshi_sync"] = "ok"
        results["balance"] = sync_data.get("summary", {}).get("available_capital_cents", 0)
    except Exception as e:
        print(f"Kalshi sync error: {e}")
        results["kalshi_sync"] = f"error: {e}"
    print()

    # Step 1: Scan markets
    print("─" * 44)
    print("STEP 1/7: Scanning Markets")
    print("─" * 44)
    try:
        scanner.run()
    except Exception as e:
        print(f"Scanner error: {e}")
    print()

    # Step 2: City Calibration
    print("─" * 44)
    print("STEP 2/7: City Calibration")
    print("─" * 44)
    try:
        cal_result = calibration.run()
        results["calibration"] = "ok"
    except Exception as e:
        print(f"Calibration error: {e}")
        results["calibration"] = f"error: {e}"
    print()

    # Step 3: Analyze weather vs odds (with weights + calibration)
    print("─" * 44)
    print("STEP 3/7: Analyzing Weather & Odds (v3)")
    print("─" * 44)
    try:
        analysis = analyzer.run()
        if analysis:
            results["opportunities"] = analysis.get("total_opportunities", 0)
            results["analyzer_version"] = analysis.get("analyzer_version", "?")
    except Exception as e:
        print(f"Analyzer error: {e}")
        results["analyzer"] = f"error: {e}"
    print()

    # Step 4: LIVE trading (Kelly + Filters + Real Kalshi Orders)
    print("─" * 44)
    print("STEP 4/7: LIVE Trading (Real Money 💰)")
    print("─" * 44)
    try:
        placed = live_trader.run()
        trades_data = paper_trader.load_trades()
        results["trades_summary"] = trades_data.get("summary", {})
        results["new_orders"] = len(placed) if placed else 0
        results["mode"] = "LIVE"
    except Exception as e:
        print(f"Live trader error: {e}")
        results["trader"] = f"error: {e}"
    print()

    # Step 5: Accuracy tracking (+ auto-calibrate weights)
    print("─" * 44)
    print("STEP 5/7: Tracking Forecast Accuracy")
    print("─" * 44)
    try:
        acc = accuracy_tracker.run()
        if acc:
            results["accuracy"] = acc.get("overall", {})
    except Exception as e:
        print(f"Accuracy tracker error: {e}")
        results["accuracy_tracker"] = f"error: {e}"
    print()

    # Step 6: Update dashboard
    print("─" * 44)
    print("STEP 6/7: Updating Dashboard")
    print("─" * 44)
    if HAS_DASHBOARD:
        try:
            update_dashboard.run()
        except Exception as e:
            print(f"Dashboard error: {e}")
    else:
        print("  Dashboard module not found, skipping")
    print()

    # Step 7: Save pipeline metadata
    print("─" * 44)
    print("STEP 7/7: Saving Pipeline Metadata")
    print("─" * 44)
    elapsed = time.time() - start
    metadata = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "3.0-calibrated",
        "elapsed_seconds": round(elapsed, 1),
        "results": results,
    }
    try:
        with open(METADATA_FILE, "w") as f:
            json.dump(metadata, f, indent=2)
        print(f"  Saved pipeline metadata to {METADATA_FILE}")
    except Exception as e:
        print(f"  Error saving metadata: {e}")

    print()
    print("═" * 44)
    print(f"✓ Complete in {elapsed:.1f}s")
    print(f"  Version: 3.0-calibrated")
    if "opportunities" in results:
        print(f"  Opportunities found: {results['opportunities']}")
    if "trades_summary" in results:
        s = results["trades_summary"]
        print(f"  Trades: {s.get('total_trades', '?')} total, P&L: {s.get('pnl_cents', 0)}¢")
    print("═" * 44)


if __name__ == "__main__":
    main()
