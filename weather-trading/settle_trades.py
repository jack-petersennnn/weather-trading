#!/usr/bin/env python3
"""
Settle trades using NWS Climatological Reports (same source Kalshi uses).
Falls back to Open-Meteo archive if NWS data unavailable.

Usage:
    python3 settle_trades.py          # Settle all eligible trades
    python3 settle_trades.py --dry    # Dry run, don't write
"""
import json, os, sys, re
from datetime import datetime, timedelta
import pytz

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import circuit_breaker
import edge_calibration
import trade_archiver

TRADES_FILE = os.path.join(os.path.dirname(__file__), "trades.json")
DASHBOARD_DATA = "/home/ubuntu/.openclaw/workspace/dashboard/data.json"

# ACIS station IDs (RCC-ACIS — must match Kalshi's NWS Climatological Report stations!)
ACIS_STATIONS = {
    "New York":       "KNYC",   # Central Park
    "Chicago":        "KMDW",   # Midway — NOT O'Hare (KORD)!
    "Miami":          "KMIA",   # Miami Intl
    "Denver":         "KDEN",   # Denver Intl
    # "Los Angeles":  "KLAX",   # LAX — excluded from trading (not in fast_scanner)
    "Austin":         "KAUS",   # Bergstrom
    "Minneapolis":    "KMSP",   # MSP
    "Washington DC":  "KDCA",   # Reagan National
    "Atlanta":        "KATL",   # Hartsfield
    "Philadelphia":   "KPHL",   # PHL
    "Houston":        "KHOU",   # Hobby Airport (Kalshi uses CLIHOU, NOT Intercontinental)
    "Dallas":         "KDFW",   # DFW
    "Seattle":        "KSEA",   # Sea-Tac
    "Boston":         "KBOS",   # Logan
    "Phoenix":        "KPHX",   # Sky Harbor
    "Oklahoma City":  "KOKC",   # Will Rogers
    "Las Vegas":      "KLAS",   # Harry Reid
    "San Francisco":  "KSFO",   # SFO
    "San Antonio":    "KSAT",   # SAT
    "New Orleans":    "KMSY",   # Louis Armstrong
}

SERIES_TO_CITY = {
    "KXHIGHNY":    "New York",
    "KXHIGHCHI":   "Chicago",
    "KXHIGHMIA":   "Miami",
    "KXHIGHDEN":   "Denver",
    # "KXHIGHLAX": "Los Angeles",  # Excluded from trading
    "KXHIGHAUS":   "Austin",
    "KXHIGHTMIN":  "Minneapolis",
    "KXHIGHTDC":   "Washington DC",
    "KXHIGHTATL":  "Atlanta",
    "KXHIGHPHIL":  "Philadelphia",
    "KXHIGHTHOU":  "Houston",
    "KXHIGHTDAL":  "Dallas",
    "KXHIGHTSEA":  "Seattle",
    "KXHIGHTBOS":  "Boston",
    "KXHIGHTPHX":  "Phoenix",
    "KXHIGHTOKC":  "Oklahoma City",
    "KXHIGHTLV":   "Las Vegas",
    "KXHIGHTSFO":  "San Francisco",
    "KXHIGHTSATX": "San Antonio",
    "KXHIGHTNOLA": "New Orleans",
}

def fetch_json(url):
    from urllib.request import urlopen, Request
    try:
        req = Request(url, headers={"User-Agent": "weather-settler/1.0"})
        with urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"  ⚠️  Fetch failed: {e}")
        return None

def get_actual_high(city, date_str):
    """Get actual high temp from RCC-ACIS (same backend database as NWS CLI reports).
    Returns integer °F, or None if data not yet available (settler retries next run)."""
    sid = ACIS_STATIONS.get(city)
    if not sid:
        print(f"  ⚠️  No ACIS station for {city}")
        return None
    
    try:
        import urllib.request
        body = json.dumps({
            "sid": sid, "sdate": date_str, "edate": date_str,
            "elems": [{"name": "maxt"}]
        }).encode()
        req = urllib.request.Request(
            "https://data.rcc-acis.org/StnData",
            data=body,
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode())
        
        rows = data.get("data", [])
        if not rows:
            print(f"  ⏳ ACIS: no data for {city} on {date_str}, will retry")
            return None
        
        val = rows[0][1]
        if val in ("M", "T", "S", ""):
            print(f"  ⏳ ACIS: {city} {date_str} = '{val}' (not yet available), will retry")
            return None
        
        temp = int(round(float(val)))
        print(f"  📡 ACIS ({sid}): {temp}°F for {date_str}")
        return temp
        
    except Exception as e:
        print(f"  ⚠️  ACIS fetch error for {city}: {e}")
        return None

def parse_event_date(event_ticker):
    """Parse date from event ticker like KXHIGHNY-26FEB14 → 2026-02-14"""
    m = re.search(r'-(\d{2})([A-Z]{3})(\d{1,2})$', event_ticker)
    if not m:
        return None
    year = 2000 + int(m.group(1))
    months = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
              "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
    month = months.get(m.group(2))
    day = int(m.group(3))
    if not month:
        return None
    return f"{year}-{month:02d}-{day:02d}"

def parse_strike(ticker):
    """Parse strike from ticker like KXHIGHNY-26FEB14-B44.5 or -T40
    B = below (above strike), T = at or above (threshold)
    """
    m = re.search(r'-(B|T)([\d.]+)$', ticker)
    if not m:
        return None, None
    kind = m.group(1)  # B=below, T=threshold/at-or-above
    strike = float(m.group(2))
    return kind, strike

def _log_settlement_to_archive(trade, timestamp):
    """IMPROVED: Log settled trade to archive immediately for proper P&L tracking."""
    try:
        archive_file = os.path.join(os.path.dirname(__file__), "trades_archive.json")
        archive_data = {"settled_trades": [], "metadata": {"last_updated": timestamp.isoformat()}}
        
        if os.path.exists(archive_file):
            with open(archive_file) as f:
                archive_data = json.load(f)
        
        # Ensure settled_trades list exists
        if "settled_trades" not in archive_data:
            archive_data["settled_trades"] = []
        
        # Add this settlement
        settlement_entry = {
            "ticker": trade.get("ticker"),
            "city": trade.get("city"),
            "direction": trade.get("direction"),
            "entry_date": trade.get("timestamp", ""),
            "settled_date": timestamp.isoformat(),
            "entry_price_cents": trade.get("entry_price_cents"),
            "contracts": trade.get("contracts"),
            "cost_cents": trade.get("cost_cents"),
            "pnl_cents": trade.get("pnl_cents"),
            "result": trade.get("result"),
            "actual_temp": trade.get("actual_temp"),
            "settlement_details": trade.get("settlement_details", {}),
            "source": trade.get("source", "unknown"),
        }
        
        archive_data["settled_trades"].append(settlement_entry)
        archive_data["metadata"]["last_updated"] = timestamp.isoformat()
        archive_data["metadata"]["total_entries"] = len(archive_data["settled_trades"])
        
        with open(archive_file, "w") as f:
            json.dump(archive_data, f, indent=2)
            
    except Exception as e:
        print(f"  ⚠️  Archive logging failed: {e}")


def resolve_trade(trade, actual_temp):
    """Determine if trade won or lost given actual temp."""
    kind, strike = parse_strike(trade["ticker"])
    if kind is None or actual_temp is None:
        return None
    
    direction = trade["direction"]  # YES or NO
    
    # B (Bracket): B67.5 = "67° to 68°" → YES if actual is 67 or 68
    # T (Threshold): T68 = "69° or above" → YES if actual >= 69
    #                T61 = "60° or below" → YES if actual <= 60
    # NWS settles on integer °F, so all comparisons are integer
    if kind == "B":
        bracket_low = int(strike - 0.5)   # B67.5 → 67
        bracket_high = int(strike + 0.5)  # B67.5 → 68
        market_yes = bracket_low <= actual_temp <= bracket_high
    else:  # T - threshold
        # T68 "69° or above": floor_strike=68, YES if actual >= 69 (i.e. > strike)
        # T61 "60° or below": cap_strike=61, YES if actual <= 60 (i.e. < strike)
        # We don't have strike_type in the ticker, but we can infer:
        # If strike is near the bottom brackets, it's likely "less" (below)
        # But safer: check trade metadata for strike_type if available
        strike_type = trade.get("strike_type")
        if not strike_type:
            # Try to look up from Kalshi API (market may be expired but try)
            try:
                import kalshi_client
                mkt = kalshi_client.get_market(trade["ticker"])
                strike_type = mkt.get("strike_type", "greater")
            except:
                strike_type = "greater"  # Default assumption for T-markets
        if strike_type == "less":
            # "X° or below" → YES if actual <= strike - 1
            market_yes = actual_temp <= strike - 1
        else:
            # "X+1° or above" → YES if actual >= strike + 1
            market_yes = actual_temp >= strike + 1
    
    if direction == "YES":
        won = market_yes
    else:
        won = not market_yes
    
    return won

def settle():
    dry = "--dry" in sys.argv
    
    with open(TRADES_FILE) as f:
        raw = json.load(f)
    trades = raw.get("trades", raw) if isinstance(raw, dict) else raw
    
    now = datetime.now(pytz.UTC)
    settled_count = 0
    total_pnl = 0
    wins = 0
    losses = 0
    
    # Cache actual temps to avoid duplicate API calls
    temp_cache = {}
    
    for trade in trades:
        # Settle any trade that hasn't been settled yet (open, exited, spike_sold, etc.)
        if trade.get("status") in ("won", "lost", "settled"):
            continue
        
        # Try event_ticker first, then extract from ticker
        event = trade.get("event_ticker") or ""
        if not event:
            # Extract event from ticker: KXHIGHNY-26FEB19-B40.5 -> KXHIGHNY-26FEB19
            parts = trade.get("ticker", "").rsplit("-", 1)
            event = parts[0] if len(parts) == 2 else ""
        date_str = parse_event_date(event)
        if not date_str:
            continue
        
        # Settle if the date has passed (no buffer — check as soon as day ends)
        trade_date = datetime.strptime(date_str, "%Y-%m-%d")
        if trade_date.date() >= now.date():
            continue
        
        city = trade.get("city", "")
        # Normalize: if city field contains a series ticker, resolve it
        if city.startswith("KX"):
            city = SERIES_TO_CITY.get(city, "")
        if not city:
            series = trade.get("series", "")
            city = SERIES_TO_CITY.get(series, "")
        if not city:
            continue
        
        # Get actual temp (cached)
        cache_key = f"{city}_{date_str}"
        if cache_key not in temp_cache:
            temp_cache[cache_key] = get_actual_high(city, date_str)
        actual = temp_cache[cache_key]
        
        if actual is None:
            print(f"  ⚠️  No actual temp for {city} on {date_str}, skipping")
            continue
        
        won = resolve_trade(trade, actual)
        if won is None:
            continue
        
        # Calculate P&L
        cost_cents = trade["cost_cents"]
        contracts = trade["contracts"]
        if won:
            pnl_cents = (100 * contracts) - cost_cents  # Win pays $1/contract
            wins += 1
        else:
            pnl_cents = -cost_cents
            losses += 1
        
        trade["status"] = "won" if won else "lost"
        trade["result"] = "won" if won else "lost"
        trade["pnl_cents"] = pnl_cents
        trade["actual_temp"] = actual
        trade["settled_at"] = now.isoformat()
        
        # IMPROVED: Enhanced settlement tracking
        _kind, _strike = parse_strike(trade["ticker"])
        trade["settlement_details"] = {
            "strike_type": _kind,
            "strike_value": _strike,
            "actual_temp": actual,
            "won": won,
            "pnl_per_contract": pnl_cents / contracts,
            "total_cost": cost_cents,
            "roi_pct": (pnl_cents / cost_cents) * 100 if cost_cents > 0 else 0,
            "hold_days": (now - datetime.fromisoformat(trade.get("timestamp", now.isoformat()))).days,
            "entry_edge": trade.get("edge", 0),
            "entry_prob": trade.get("our_prob", 0.5),
        }
        
        # Circuit breaker: record result
        try:
            circuit_breaker.record_result("win" if won else "loss", ticker=trade.get("ticker", ""), pnl_cents=pnl_cents)
        except:
            pass
        
        # Edge calibration: record outcome
        try:
            edge_calibration.record_outcome(trade.get("ticker", ""), trade.get("direction", ""), won)
        except:
            pass
        
        # === PHASE B: Blown exit settlement logging ===
        # If this trade was blown-exited, append settlement outcome + counterfactual
        if trade.get("blown_phase_a"):
            phase_a = trade["blown_phase_a"]
            # Counterfactual: what would have happened if we held to settlement?
            hold_pnl = pnl_cents  # This IS what settlement says (won/lost)
            # But we already exited, so our actual realized P&L was from the exit sell
            # The blown exit P&L was roughly: (exit_sell_price - entry_price) * contracts
            # We log both so we can compare
            trade["blown_phase_b"] = {
                "settlement_temp": actual,
                "settlement_won": won,
                "hold_counterfactual_pnl_cents": (100 * contracts - cost_cents) if won else -cost_cents,
                "net_benefit_of_exit": "computed_externally",  # needs exit fill price from Phase A
                "settled_at": now.isoformat(),
            }
        
        total_pnl += pnl_cents
        settled_count += 1
        
        # IMPROVED: Log settlement to separate archive immediately
        _log_settlement_to_archive(trade, now)
        
        kind, strike = parse_strike(trade["ticker"])
        print(f"  {'✅' if won else '❌'} {trade['ticker']} | {city} {date_str} | "
              f"Actual: {actual}°F | Strike: {kind}{strike} | Dir: {trade['direction']} | "
              f"P&L: {'+'if pnl_cents>=0 else ''}{pnl_cents/100:.2f}")
    
    print(f"\n{'='*60}")
    print(f"  SETTLEMENT SUMMARY")
    print(f"  Settled: {settled_count} trades")
    print(f"  Wins: {wins} | Losses: {losses}")
    if wins + losses > 0:
        print(f"  Win Rate: {wins/(wins+losses)*100:.1f}%")
    print(f"  P&L: {'+'if total_pnl>=0 else ''}{total_pnl/100:.2f}")
    print(f"{'='*60}")
    
    if not dry and settled_count > 0:
        # Preserve original file structure
        if isinstance(raw, dict):
            raw["trades"] = trades
            save_data = raw
        else:
            save_data = trades
        with open(TRADES_FILE, "w") as f:
            json.dump(save_data, f, indent=2)
        print(f"\n  💾 Saved {settled_count} settlements to trades.json")
        
        # Update dashboard data.json
        update_dashboard(trades)
        
        # Archive closed trades to keep trades.json lean
        try:
            trade_archiver.archive()
        except Exception as e:
            print(f"  ⚠️  Archive failed: {e}")
    elif dry:
        print("\n  🔍 Dry run — no changes written")

def update_dashboard(trades):
    """Update dashboard data.json with latest P&L."""
    try:
        with open(DASHBOARD_DATA) as f:
            dash = json.load(f)
    except:
        return
    
    open_trades = [t for t in trades if t["status"] == "open"]
    settled = [t for t in trades if t["status"] in ("won", "lost")]
    won = [t for t in settled if t["status"] == "won"]
    
    total_pnl_cents = sum(t.get("pnl_cents", 0) for t in settled)
    deployed_cents = sum(t.get("cost_cents", 0) for t in open_trades)
    
    # Settle today
    today = datetime.now(pytz.timezone("US/Eastern")).strftime("%Y-%m-%d")
    settled_today = [t for t in settled if t.get("settled_at", "")[:10] == today]
    today_pnl = sum(t.get("pnl_cents", 0) for t in settled_today)
    
    dash["pnl"]["total"] = total_pnl_cents / 100
    dash["pnl"]["today"] = today_pnl / 100
    dash["pnl"]["trades"] = len(open_trades)
    dash["pnl"]["settled_today"] = len(settled_today)
    dash["pnl"]["winRate"] = f"{len(won)/len(settled)*100:.0f}%" if settled else "—"
    dash["portfolio"]["portfolio_value_dollars"] = deployed_cents / 100
    dash["portfolio"]["open_trades"] = len(open_trades)
    
    with open(DASHBOARD_DATA, "w") as f:
        json.dump(dash, f, indent=2)
    print("  📊 Dashboard updated with P&L")

def settle_journal(temp_cache):
    """Also settle trade_journal.json entries using the same ACIS data."""
    journal_file = os.path.join(os.path.dirname(__file__), "trade_journal.json")
    if not os.path.exists(journal_file):
        return
    
    try:
        with open(journal_file) as f:
            journal = json.load(f)
    except:
        return
    
    settled = 0
    for entry in journal:
        if entry.get("status") not in (None, "?", "open"):
            continue
        # Only settle ENTRY/BUY actions — EXIT entries are already closed
        action = entry.get("action", "").upper()
        if "EXIT" in action or "SELL" in action:
            continue
        
        ticker = entry.get("ticker", "")
        if not ticker:
            continue
        
        # Parse date from ticker: KXHIGHNY-26FEB21-B40.5
        parts = ticker.split("-")
        if len(parts) < 2:
            continue
        event = f"{parts[0]}-{parts[1]}"
        date_str = parse_event_date(event)
        if not date_str:
            continue
        
        trade_date = datetime.strptime(date_str, "%Y-%m-%d")
        now = datetime.now(pytz.UTC)
        if trade_date.date() >= now.date():
            continue
        
        city = entry.get("city", "")
        if city.startswith("KX"):
            city = SERIES_TO_CITY.get(city, "")
        if not city:
            series = entry.get("series", "")
            city = SERIES_TO_CITY.get(series, "")
        if not city:
            continue
        
        cache_key = f"{city}_{date_str}"
        if cache_key not in temp_cache:
            temp_cache[cache_key] = get_actual_high(city, date_str)
        actual = temp_cache[cache_key]
        if actual is None:
            continue
        
        # Determine win/loss
        kind, strike = parse_strike(ticker)
        if kind is None:
            continue
        
        direction = entry.get("direction", "YES")
        if kind == "B":
            # Bracket: B67.5 = "67° to 68°" → YES if actual is 67 or 68
            bracket_low = int(strike - 0.5)
            bracket_high = int(strike + 0.5)
            market_yes = bracket_low <= actual <= bracket_high
        else:  # T - threshold
            strike_type = entry.get("strike_type")
            if not strike_type:
                try:
                    import kalshi_client
                    mkt = kalshi_client.get_market(ticker)
                    strike_type = mkt.get("strike_type", "greater")
                except:
                    strike_type = "greater"
            if strike_type == "less":
                market_yes = actual <= strike - 1
            else:
                market_yes = actual >= strike + 1
        
        won = (direction == "YES" and market_yes) or (direction == "NO" and not market_yes)
        
        contracts = entry.get("contracts", 0)
        price_cents = entry.get("price_cents", 0)
        cost = contracts * price_cents
        if won:
            pnl = (100 * contracts) - cost
        else:
            pnl = -cost
        
        entry["status"] = "won" if won else "lost"
        entry["actual_temp"] = actual
        entry["pnl_cents"] = pnl
        entry["settled_at"] = now.isoformat()
        settled += 1
    
    if settled > 0:
        with open(journal_file, "w") as f:
            json.dump(journal, f, indent=2)
        print(f"\n  📓 Journal: settled {settled} entries")


if __name__ == "__main__":
    print("\n🏦 WEATHER TRADE SETTLER\n")
    settle()
    # Also settle journal entries using cached temps
    settle_journal({})
