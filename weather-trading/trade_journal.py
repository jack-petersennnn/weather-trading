#!/usr/bin/env python3
"""
Trade Journal — Logs the WHY behind every trade action.

Every entry, exit, add, hedge, and re-entry gets a structured record with:
- Full forecast snapshot (all sources + ensemble stats)
- The reasoning chain that led to the decision
- Market conditions at time of action
- Position context (what we already held, limits, etc.)

File: trade_journal.json (append-only, one record per action)
"""

import json
import os
import fcntl
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JOURNAL_FILE = os.path.join(BASE_DIR, "trade_journal.json")


ARCHIVE_DIR = os.path.join(BASE_DIR, "journal_archive")
MAX_ENTRIES = 2000  # Per file — when hit, rotate to archive


def _load_journal():
    if not os.path.exists(JOURNAL_FILE):
        return []
    try:
        with open(JOURNAL_FILE) as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            data = json.load(f)
            fcntl.flock(f, fcntl.LOCK_UN)
            return data
    except:
        return []


def _save_journal(entries):
    # When we hit the cap, archive the current file and start fresh
    if len(entries) > MAX_ENTRIES:
        _rotate_journal(entries)
        return
    with open(JOURNAL_FILE, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        json.dump(entries, f, indent=2)
        f.flush()
        fcntl.flock(f, fcntl.LOCK_UN)


def _rotate_journal(entries):
    """Archive current journal and start a new one."""
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    
    # Name archive by date range: first entry → last entry
    first_ts = entries[0].get("ts", "unknown")[:10]
    last_ts = entries[-1].get("ts", "unknown")[:10]
    archive_name = f"journal_{first_ts}_to_{last_ts}.json"
    archive_path = os.path.join(ARCHIVE_DIR, archive_name)
    
    # If file exists (same date range), append a counter
    counter = 1
    while os.path.exists(archive_path):
        archive_name = f"journal_{first_ts}_to_{last_ts}_{counter}.json"
        archive_path = os.path.join(ARCHIVE_DIR, archive_name)
        counter += 1
    
    # Write full journal to archive
    with open(archive_path, "w") as f:
        json.dump(entries, f, indent=2)
    
    # Start fresh
    with open(JOURNAL_FILE, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        json.dump([], f)
        f.flush()
        fcntl.flock(f, fcntl.LOCK_UN)
    
    print(f"📓 Journal rotated → {archive_name} ({len(entries)} entries archived)")


def log_action(action, ticker, direction, contracts, price_cents, 
               city=None, series=None, reasoning=None, forecast_snapshot=None,
               ensemble_mean=None, ensemble_std=None, source_spread=None,
               edge=None, our_prob=None, market_price=None,
               entry_forecast=None, current_temp=None, max_so_far=None,
               severity=None, prob_drop=None, forecast_shift=None,
               peak_info=None, conviction=None, position_context=None,
               extra=None):
    """
    Log a trade action with full context.
    
    action: "ENTRY" | "ADD" | "EXIT_BLOWN" | "EXIT_GRADUATED" | "HEDGE" | "RE_ENTRY"
    reasoning: Human-readable string explaining WHY this action was taken
    forecast_snapshot: dict of {source_name: forecast_value} at time of action
    position_context: dict with info like existing positions, city/date counts, etc.
    extra: any additional context worth recording
    """
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "ticker": ticker,
        "city": city,
        "series": series,
        "direction": direction,
        "contracts": contracts,
        "price_cents": price_cents,
        "reasoning": reasoning,
        
        # Forecast data at time of action
        "forecast_snapshot": forecast_snapshot,
        "ensemble_mean": round(ensemble_mean, 2) if ensemble_mean else None,
        "ensemble_std": round(ensemble_std, 2) if ensemble_std else None,
        "source_spread": round(source_spread, 2) if source_spread else None,
        
        # Trade metrics
        "edge": round(edge, 4) if edge else None,
        "our_prob": round(our_prob, 4) if our_prob else None,
        "market_price": round(market_price, 4) if market_price else None,
        
        # For exits: what changed
        "entry_forecast": round(entry_forecast, 2) if entry_forecast else None,
        "forecast_shift": round(forecast_shift, 2) if forecast_shift is not None else None,
        "severity": round(severity, 2) if severity else None,
        "prob_drop": round(prob_drop, 4) if prob_drop else None,
        
        # Current conditions
        "current_temp": current_temp,
        "max_so_far": max_so_far,
        
        # Peak detection state
        "peak_info": peak_info,
        
        # Sizing reasoning
        "conviction": conviction,
        
        # Position context
        "position_context": position_context,
        
        # Anything else
        "extra": extra,
    }
    
    # Strip None values to keep journal compact
    entry = {k: v for k, v in entry.items() if v is not None}
    
    journal = _load_journal()
    
    # Dedup: For ADD/ENTRY actions, check if this ticker already has an entry today.
    # Roll into existing position instead of creating a new record.
    if action in ("ADD", "ENTRY"):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        existing = None
        for i, e in enumerate(journal):
            if (e.get("ticker") == ticker and 
                e.get("action") in ("ADD", "ENTRY") and
                e.get("ts", "").startswith(today)):
                existing = i
                break
        if existing is not None:
            # Roll into existing: update contracts, avg price, bump add_count
            old = journal[existing]
            old_contracts = old.get("contracts", 0) or 0
            old_price = old.get("price_cents", 0) or 0
            new_contracts = (contracts or 0)
            total = old_contracts + new_contracts
            if total > 0:
                avg_price = round((old_contracts * old_price + new_contracts * price_cents) / total)
            else:
                avg_price = price_cents
            old["contracts"] = total
            old["price_cents"] = avg_price
            old["add_count"] = old.get("add_count", 1) + 1
            old["last_add_ts"] = entry["ts"]
            _save_journal(journal)
            return old
    
    # Dedup: For EXIT actions, check if this ticker already has an exit today
    if action in ("EXIT_BLOWN", "EXIT_GRADUATED", "HEDGE"):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for e in journal:
            if (e.get("ticker") == ticker and
                e.get("action") == action and
                e.get("ts", "").startswith(today)):
                # Already logged this exit today, skip
                return e
    
    journal.append(entry)
    _save_journal(journal)
    
    return entry


def _load_all_journals():
    """Load current journal + all archives for full history searches."""
    all_entries = []
    
    # Load archives first (oldest first)
    if os.path.exists(ARCHIVE_DIR):
        for fname in sorted(os.listdir(ARCHIVE_DIR)):
            if fname.endswith(".json"):
                try:
                    with open(os.path.join(ARCHIVE_DIR, fname)) as f:
                        all_entries.extend(json.load(f))
                except:
                    pass
    
    # Then current journal
    all_entries.extend(_load_journal())
    return all_entries


def get_daily_journal(date_str=None, include_archives=True):
    """Get all journal entries for a specific date (YYYY-MM-DD). Defaults to today."""
    if not date_str:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    journal = _load_all_journals() if include_archives else _load_journal()
    return [e for e in journal if e.get("ts", "").startswith(date_str)]


def get_ticker_history(ticker):
    """Get all journal entries for a specific ticker (searches archives too)."""
    journal = _load_all_journals()
    return [e for e in journal if e.get("ticker") == ticker]


def get_city_history(city, limit=100):
    """Get recent journal entries for a city (searches archives too)."""
    journal = _load_all_journals()
    matches = [e for e in journal if e.get("city") == city]
    return matches[-limit:]


def list_archives():
    """List all archived journal files with entry counts."""
    if not os.path.exists(ARCHIVE_DIR):
        return []
    archives = []
    for fname in sorted(os.listdir(ARCHIVE_DIR)):
        if fname.endswith(".json"):
            path = os.path.join(ARCHIVE_DIR, fname)
            try:
                with open(path) as f:
                    count = len(json.load(f))
                archives.append({"file": fname, "entries": count})
            except:
                archives.append({"file": fname, "entries": "?"})
    return archives


def summary_for_date(date_str=None):
    """Generate a human-readable summary of a day's trading actions."""
    entries = get_daily_journal(date_str)
    if not entries:
        return "No trades logged for this date."
    
    lines = []
    by_action = {}
    for e in entries:
        a = e.get("action", "UNKNOWN")
        by_action.setdefault(a, []).append(e)
    
    lines.append(f"📓 Trade Journal — {date_str or 'today'}")
    lines.append(f"Total actions: {len(entries)}")
    lines.append("")
    
    for action_type in ["ENTRY", "ADD", "EXIT_BLOWN", "EXIT_GRADUATED", "HEDGE", "RE_ENTRY"]:
        acts = by_action.get(action_type, [])
        if not acts:
            continue
        lines.append(f"{'─'*40}")
        lines.append(f"**{action_type}** ({len(acts)})")
        for e in acts:
            t = e.get("ts", "")[11:16]
            ticker = e.get("ticker", "?")
            direction = e.get("direction", "?")
            contracts = e.get("contracts", "?")
            reason = e.get("reasoning", "no reason logged")
            lines.append(f"  {t} | {ticker} {direction} x{contracts}")
            lines.append(f"         {reason}")
            if e.get("forecast_snapshot"):
                sources = ", ".join(f"{k}={v:.1f}" for k, v in e["forecast_snapshot"].items())
                lines.append(f"         Sources: {sources}")
            if e.get("ensemble_mean"):
                lines.append(f"         Ensemble: {e['ensemble_mean']}°F ± {e.get('ensemble_std', '?')}°F")
            lines.append("")
    
    return "\n".join(lines)
