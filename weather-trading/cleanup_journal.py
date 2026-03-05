#!/usr/bin/env python3
"""
Journal Dedup Cleanup — Rolls duplicate ADD entries into single position lifecycle records.

Before: KXHIGHMIA-26FEB21-B85.5 appears 35 times (34 ADDs + 1 EXIT)
After:  KXHIGHMIA-26FEB21-B85.5 appears 1-2 times (1 ENTRY/consolidated + 1 settlement)

Also creates a backup before modifying.
"""

import json
import os
import shutil
from datetime import datetime, timezone
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JOURNAL_FILE = os.path.join(BASE_DIR, "trade_journal.json")


def cleanup():
    if not os.path.exists(JOURNAL_FILE):
        print("No journal file found.")
        return

    with open(JOURNAL_FILE) as f:
        entries = json.load(f)

    print(f"BEFORE: {len(entries)} total entries")

    # Count dupes before
    from collections import Counter
    before_counts = Counter(e.get("ticker", "?") for e in entries)
    top_dupes = before_counts.most_common(5)
    print(f"Top dupes before: {top_dupes}")

    # Backup
    backup_path = JOURNAL_FILE + f".backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    shutil.copy2(JOURNAL_FILE, backup_path)
    print(f"Backup saved to {backup_path}")

    # Group by ticker
    by_ticker = defaultdict(list)
    for e in entries:
        by_ticker[e.get("ticker", "unknown")].append(e)

    # Consolidate: for each ticker, produce:
    #   1. One consolidated ENTRY record (merging all ADDs into the first entry)
    #   2. One settlement/exit record if it exists
    consolidated = []
    for ticker, group in by_ticker.items():
        # Separate by action type
        adds = [e for e in group if e.get("action") in ("ADD", "ENTRY")]
        exits = [e for e in group if e.get("action") not in ("ADD", "ENTRY")]

        if adds:
            # Use first entry as base, accumulate contracts and cost
            base = dict(adds[0])
            total_contracts = sum(e.get("contracts", 0) or 0 for e in adds)
            total_cost = sum((e.get("contracts", 0) or 0) * (e.get("price_cents", 0) or 0) for e in adds)
            avg_price = round(total_cost / total_contracts) if total_contracts else base.get("price_cents", 0)

            base["action"] = "ENTRY"
            base["contracts"] = total_contracts
            base["price_cents"] = avg_price
            base["add_count"] = len(adds)
            if len(adds) > 1:
                base["first_add"] = adds[0].get("ts")
                base["last_add"] = adds[-1].get("ts")
                base["reasoning"] = (base.get("reasoning", "") or "") + f" [consolidated from {len(adds)} scans]"

            consolidated.append(base)

        # Keep only the LAST exit/settlement record per ticker (dedup repeated exit scans)
        if exits:
            # Group exits by action type, keep last of each type
            exit_by_action = {}
            for ex in exits:
                action = ex.get("action", "EXIT")
                exit_by_action[action] = ex  # last one wins
            # If there's both EXIT_BLOWN and EXIT_GRADUATED, keep whichever came last
            last_exit = max(exits, key=lambda e: e.get("ts", ""))
            last_exit["exit_scan_count"] = len(exits)
            consolidated.append(last_exit)

    # Sort by timestamp
    consolidated.sort(key=lambda e: e.get("ts", ""))

    print(f"AFTER: {len(consolidated)} total entries")
    after_counts = Counter(e.get("ticker", "?") for e in consolidated)

    # Show before/after for the worst offender
    for ticker, before_count in top_dupes[:3]:
        after_count = after_counts.get(ticker, 0)
        print(f"  {ticker}: {before_count} → {after_count}")

    with open(JOURNAL_FILE, "w") as f:
        json.dump(consolidated, f, indent=2)

    print(f"\n✅ Cleanup complete. {len(entries)} → {len(consolidated)} entries.")
    return {
        "before": len(entries),
        "after": len(consolidated),
        "backup": backup_path,
        "examples": {t: {"before": c, "after": after_counts.get(t, 0)} for t, c in top_dupes[:5]}
    }


if __name__ == "__main__":
    result = cleanup()
    print(json.dumps(result, indent=2))
