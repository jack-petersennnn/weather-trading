#!/usr/bin/env python3
"""
Spike Monitor — Real-time price spike detector for Kalshi weather positions.
Polls every 1 second, detects favorable price spikes, auto-sells when profitable.

Logic:
- Track entry price for each position (from trades.json)
- Ignore spikes below entry price (still underwater)
- Auto-sell when price spikes well above entry
- BUT check our forecast first — if data strongly supports the position, hold

Run: python3 spike_monitor.py
Stop: Ctrl+C (writes stats on exit)
"""

import json
import math
import os
import sys
import time
import signal
import statistics
import re
from datetime import datetime, timezone, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kalshi_client
import city_logger
from fast_scanner import SERIES_META as FS_CITIES
from metar_tracker import evaluate_position as metar_evaluate

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRADES_FILE = os.path.join(BASE_DIR, "trades.json")
SPIKE_LOG = os.path.join(BASE_DIR, "spike_log.json")
SPIKE_TRADE_LOG = os.path.join(BASE_DIR, "spike_trade_log.jsonl")  # Detailed per-trade log
CONFIG_FILE = os.path.join(BASE_DIR, "trading_config.json")

# ── Configuration ────────────────────────────────────────────────────

def load_config():
    try:
        with open(CONFIG_FILE) as f:
            return json.load(f).get("spike_monitor", {})
    except:
        return {}

def get_cfg():
    cfg = load_config()
    return {
        "poll_interval": cfg.get("poll_interval_sec", 1.0),
        # Minimum profit ratio to trigger sell (e.g., 1.2 = price must be 1.2x entry)
        "min_profit_ratio": cfg.get("min_profit_ratio", 1.2),
        # Absolute minimum price jump in cents to consider (avoid noise on cheap contracts)
        "min_spike_cents": cfg.get("min_spike_cents", 3),
        # If our forecast probability is above this, HOLD entirely
        "hold_if_prob_above": cfg.get("hold_if_prob_above", 0.70),
        # If our prob is between partial_sell_min and hold_above, sell enough to break even
        "partial_sell_prob_min": cfg.get("partial_sell_prob_min", 0.40),
        # Sell discount: place limit order slightly below market to ensure fill
        "sell_discount_cents": cfg.get("sell_discount_cents", 1),
    }


def _norm_cdf(x):
    """Standard normal CDF — same as fast_scanner.norm_cdf."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _ticker_to_series(ticker):
    """Extract series prefix from ticker, e.g. KXHIGHTATL-26FEB22-B59.5 -> KXHIGHTATL"""
    parts = ticker.split("-")
    return parts[0] if parts else None


def _load_forecast_cache():
    """Load the training forecast log (updated every fast_scanner run).
    Returns dict keyed by 'City|YYYY-MM-DD' with ensemble_mean/ensemble_std."""
    try:
        log_file = os.path.join(BASE_DIR, "training_forecast_log.json")
        with open(log_file) as f:
            return json.load(f)
    except Exception:
        return {}

_forecast_cache = None
_forecast_cache_mtime = 0

def _get_forecast_cache():
    """Cached loader for training_forecast_log.json — reloads on file change."""
    global _forecast_cache, _forecast_cache_mtime
    log_file = os.path.join(BASE_DIR, "training_forecast_log.json")
    try:
        mtime = os.path.getmtime(log_file)
        if _forecast_cache and mtime == _forecast_cache_mtime:
            return _forecast_cache
        _forecast_cache = _load_forecast_cache()
        _forecast_cache_mtime = mtime
        return _forecast_cache
    except Exception:
        return _forecast_cache or {}


def compute_live_prob(ticker, direction, **kwargs):
    """Compute our probability for a ticker using cached ensemble forecast data.
    
    Uses training_forecast_log.json (written every fast_scanner run, every 20 min)
    which contains bias-corrected, per-city weighted ensemble mean & std.
    Returns float probability or None if unable to compute.
    """
    try:
        series = _ticker_to_series(ticker)
        if not series or series not in FS_CITIES:
            return None
        
        city = FS_CITIES[series]["city"]
        
        # Parse strike and date from ticker
        parts = ticker.split("-")
        if len(parts) != 3:
            return None
        strike_str = parts[2]
        
        try:
            date_part = parts[1]
            target_date = datetime.strptime(date_part, "%y%b%d").strftime("%Y-%m-%d")
        except:
            return None
        
        # Look up cached ensemble from training_forecast_log
        cache = _get_forecast_cache()
        key = f"{city}|{target_date}"
        entry = cache.get(key)
        if not entry:
            return None
        
        mean = entry.get("ensemble_mean")
        std = entry.get("ensemble_std")
        if mean is None or std is None:
            return None
        std = max(std, 1.0)  # Floor at 1.0 to prevent division issues
        
        # Compute probability (same logic as fast_scanner.find_opportunities)
        # Apply continuity correction: NWS reports integer °F, Kalshi settles on integers
        if strike_str.startswith("B"):
            strike = float(strike_str[1:])
            # B67.5 = "67 to 68" → P(66.5 < temp < 68.5)
            floor_val = strike - 1.0
            cap_val = strike + 1.0
            our_yes = _norm_cdf((cap_val - mean) / std) - _norm_cdf((floor_val - mean) / std)
        elif strike_str.startswith("T"):
            strike = float(strike_str[1:])
            # Determine if this is a "greater" or "less" market
            # Try to get strike_type from trade entry, fall back to Kalshi API lookup
            st = kwargs.get("strike_type")
            if not st:
                try:
                    import kalshi_client as kc
                    mkt_info = kc.get_market(ticker)
                    st = mkt_info.get("strike_type", "greater")
                except:
                    st = "greater"  # default assumption
            if st == "less":
                # T61 "60° or below": cap_strike=61, boundary at 60.5
                our_yes = _norm_cdf(((strike - 0.5) - mean) / std)
            else:
                # T68 "69° or above": floor_strike=68, boundary at 68.5
                our_yes = 1 - _norm_cdf(((strike + 0.5) - mean) / std)
        else:
            return None
        
        our_yes = min(our_yes, 0.95)
        
        if direction == "YES":
            return our_yes
        else:
            return min(1.0 - our_yes, 0.95)
    except Exception as e:
        print(f"    ⚠ compute_live_prob({ticker}): {e}")
        return None


# ── State ────────────────────────────────────────────────────────────

class SpikeMonitor:
    def __init__(self):
        self.cfg = get_cfg()
        self.running = True
        self.trades = {}        # ticker -> trade data from trades.json
        self.entry_prices = {}  # ticker -> entry price cents
        self.directions = {}    # ticker -> YES/NO
        self.our_probs = {}     # ticker -> our probability estimate
        self.price_history = defaultdict(list)  # ticker -> [(ts, price)]
        self.sells_made = []
        self.scan_count = 0
        self.last_positions = {} # ticker -> position count from Kalshi
        self.original_contracts = {}  # ticker -> original contract count (before any sells)
        self.peak_prices = {}   # ticker -> highest price seen since last sell
        self.sell_phases = {}   # ticker -> current phase ("initial", "covered", "profit_taken", "moon")
        self.last_sell_attempt = {}  # ticker -> timestamp of last sell attempt (cooldown)
        self.SELL_COOLDOWN_SEC = 60  # Don't retry selling same ticker more than once per minute
        
        # Load trades
        self.reload_trades()
        
        # Graceful shutdown
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
    
    def _shutdown(self, *args):
        print(f"\n⛔ Shutting down spike monitor after {self.scan_count} scans, {len(self.sells_made)} sells")
        self.running = False
    
    def _parse_ticker_date(self, ticker):
        """Extract target date from ticker like KXHIGHTATL-26FEB23-T38 -> 2026-02-23"""
        parts = ticker.split("-")
        if len(parts) < 2:
            return None
        date_part = parts[1]  # e.g., 26FEB23
        try:
            # Format: YYMMMDD
            return datetime.strptime(date_part, "%y%b%d").strftime("%Y-%m-%d")
        except:
            return None
    
    def _is_relevant_date(self, ticker):
        """Only track positions for today or future dates."""
        target = self._parse_ticker_date(ticker)
        if not target:
            return True  # Can't parse, keep it
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return target >= today
    
    def sync_from_kalshi(self):
        """Sync trades.json with actual Kalshi positions (source of truth)."""
        try:
            positions = kalshi_client.get_positions()
            kalshi_map = {}
            kalshi_positions = {}
            for p in positions.get("market_positions", []):
                count = p.get("position", 0)
                kalshi_map[p["ticker"]] = abs(count)  # include zeros for sync
                kalshi_positions[p["ticker"]] = p
            
            with open(TRADES_FILE) as f:
                data = json.load(f)
            
            fixes = 0
            for t in data.get("trades", []):
                if t.get("status") != "open":
                    continue
                ticker = t["ticker"]
                kalshi_count = kalshi_map.get(ticker)
                if kalshi_count is None or kalshi_count == 0:
                    t["status"] = "exited"
                    t["result"] = "synced_closed"
                    fixes += 1
                else:
                    if kalshi_count != t.get("contracts", 0):
                        t["contracts"] = kalshi_count
                        fixes += 1
                    # Always update market_exposure from Kalshi (source of truth for cap checks)
                    kp = kalshi_positions.get(ticker)
                    if kp:
                        t["market_exposure"] = kp.get("market_exposure", 0)
                        # CRITICAL: Sync direction from Kalshi (prevents position-flip bugs)
                        kalshi_dir = "YES" if kp.get("position", 0) > 0 else "NO"
                        if t.get("direction") != kalshi_dir:
                            print(f"  🚨 Direction fix: {ticker} {t.get('direction')} → {kalshi_dir}")
                            t["direction"] = kalshi_dir
                            fixes += 1
            
            if fixes > 0:
                with open(TRADES_FILE, "w") as f:
                    json.dump(data, f, indent=2)
                print(f"  🔄 Synced {fixes} positions from Kalshi")
        except Exception as e:
            print(f"⚠ Kalshi sync error: {e}")
    
    def reload_trades(self):
        """Load entry prices and directions from trades.json, filtered to relevant dates.
        Computes live probability for any trade missing our_prob and backfills trades.json."""
        try:
            with open(TRADES_FILE) as f:
                data = json.load(f)
            
            # Clear old tracking
            self.trades.clear()
            self.entry_prices.clear()
            self.directions.clear()
            self.our_probs.clear()
            
            skipped = 0
            skipped_ghost = 0
            backfilled = 0
            for t in data.get("trades", []):
                if t.get("status") == "open":
                    ticker = t["ticker"]
                    if not self._is_relevant_date(ticker):
                        skipped += 1
                        continue
                    # Skip ghost positions with no real entry price (unfixed kalshi_sync)
                    if t.get("source") == "kalshi_sync" and t.get("entry_price_cents", 0) == 50:
                        skipped_ghost += 1
                        continue
                    self.trades[ticker] = t
                    self.entry_prices[ticker] = t["entry_price_cents"]
                    direction = t.get("direction", "YES")
                    self.directions[ticker] = direction
                    
                    # Track original contracts and sell phase
                    orig = t.get("pm_original_contracts", t.get("contracts", 0))
                    if ticker not in self.original_contracts:
                        self.original_contracts[ticker] = orig
                    # Determine sell phase from trade state
                    if t.get("spike_partial_sell"):
                        if t.get("cost_cents", 1) == 0:
                            self.sell_phases[ticker] = "covered"
                        else:
                            self.sell_phases[ticker] = "covered"
                    else:
                        self.sell_phases.setdefault(ticker, "initial")
                    
                    # Use stored prob if available, otherwise compute live
                    stored_prob = t.get("our_prob")
                    if stored_prob is not None and stored_prob != 0.5:
                        self.our_probs[ticker] = stored_prob
                    else:
                        live_prob = compute_live_prob(ticker, direction, strike_type=t.get("strike_type"))
                        if live_prob is not None:
                            self.our_probs[ticker] = live_prob
                            # Backfill into trades.json data
                            t["our_prob"] = live_prob
                            backfilled += 1
                            print(f"  🔮 Computed live prob for {ticker}: {live_prob:.2%}")
                        else:
                            self.our_probs[ticker] = 0.5
                            print(f"  ⚠ Could not compute prob for {ticker}, using 0.5 fallback")
            
            # Save backfilled probabilities to trades.json
            if backfilled > 0:
                with open(TRADES_FILE, "w") as f:
                    json.dump(data, f, indent=2)
                print(f"  📝 Backfilled {backfilled} trades with live probabilities")
            
            if skipped > 0:
                print(f"  📅 Skipped {skipped} expired positions")
            if skipped_ghost > 0:
                print(f"  👻 Skipped {skipped_ghost} ghost positions (no real entry price)")
        except Exception as e:
            print(f"⚠ Error loading trades: {e}")
    
    def get_market_prices(self):
        """Batch-fetch current prices for all positions via series queries."""
        # Get open positions from Kalshi
        try:
            pos_data = kalshi_client.get_positions()
            positions = pos_data.get("market_positions", [])
        except Exception as e:
            print(f"⚠ Position fetch error: {e}")
            return {}
        
        # Group by series, only relevant (today/future) dates
        series_map = {}  # series -> set of tickers we hold
        for p in positions:
            ticker = p["ticker"]
            contracts = p.get("position", 0)
            if contracts == 0:
                continue
            if not self._is_relevant_date(ticker):
                continue
            self.last_positions[ticker] = abs(contracts)
            parts = ticker.split("-")
            series = parts[0]
            if series not in series_map:
                series_map[series] = set()
            series_map[series].add(ticker)
        
        # Fetch markets per series (1 API call per series)
        prices = {}  # ticker -> {yes_bid, yes_ask, last_price}
        for series, tickers in series_map.items():
            try:
                result = kalshi_client.get_markets(series_ticker=series, status="open")
                for m in result.get("markets", []):
                    t = m.get("ticker")
                    if t in tickers:
                        prices[t] = {
                            "yes_bid": m.get("yes_bid", 0),
                            "yes_ask": m.get("yes_ask", 0),
                            "no_bid": m.get("no_bid", 0),
                            "no_ask": m.get("no_ask", 0),
                            "last_price": m.get("last_price", 0),
                        }
            except Exception as e:
                print(f"⚠ Market fetch error for {series}: {e}")
                time.sleep(0.2)
        
        return prices
    
    def check_spike(self, ticker, prices):
        """Check if a position has spiked enough to sell."""
        if ticker not in self.entry_prices:
            return None
        
        entry = self.entry_prices[ticker]
        direction = self.directions.get(ticker, "YES")
        
        # Current sellable price depends on direction
        if direction == "YES":
            # We hold YES contracts, sell at yes_bid
            current = prices.get("yes_bid", 0)
        else:
            # We hold NO contracts, sell at no_bid
            current = prices.get("no_bid", 0)
        
        if current <= 0:
            return None
        
        # Track price history
        now = time.time()
        self.price_history[ticker].append((now, current))
        # Keep last 5 min of history
        cutoff = now - 300
        self.price_history[ticker] = [(t, p) for t, p in self.price_history[ticker] if t > cutoff]
        
        # Calculate profit ratio
        profit_ratio = current / entry if entry > 0 else 0
        profit_cents = current - entry
        
        # Skip if still below entry — not profitable
        if current <= entry:
            return None
        
        # Always try live probability at decision time (freshest data)
        direction = self.directions.get(ticker, "YES")
        trade_data = self.trades.get(ticker, {})
        live_prob = compute_live_prob(ticker, direction, strike_type=trade_data.get("strike_type"))
        if live_prob is not None:
            our_prob = live_prob
            self.our_probs[ticker] = live_prob
        else:
            our_prob = self.our_probs.get(ticker, 0.5)
        contracts = self.last_positions.get(ticker, 0)
        if contracts <= 0:
            return None
        
        phase = self.sell_phases.get(ticker, "initial")
        original = self.original_contracts.get(ticker, contracts)
        
        # ── Track peak price (for trailing stop on covered/moon positions) ──
        prev_peak = self.peak_prices.get(ticker, 0)
        if current > prev_peak:
            self.peak_prices[ticker] = current
        peak = self.peak_prices.get(ticker, current)
        
        # ══════════════════════════════════════════════════════════════
        # PHASE 1: INITIAL — Haven't sold anything yet
        # Trigger: price spikes enough to cover entry + take some profit
        # Action: sell enough to recover ~120% of entry (cover + 20% profit)
        # ══════════════════════════════════════════════════════════════
        # ── Compute max-profit metrics once, used by ALL phases ──
        max_profit = 100 - entry  # max possible profit in cents
        pct_of_max = profit_cents / max_profit if max_profit > 0 else 0
        
        # ══════════════════════════════════════════════════════════════
        # RULE 0: NEAR-CEILING FULL EXIT
        # If current >= 95¢ AND entry was under 70¢, sell 100%.
        # Only 5¢ of upside left — not worth holding for moon bag.
        # Skip this for positions entered at high prices (no real spike).
        # ══════════════════════════════════════════════════════════════
        if current >= 95 and entry < 70:
            return {
                "ticker": ticker, "direction": direction,
                "entry_cents": entry, "current_cents": current,
                "profit_ratio": round(profit_ratio, 2), "profit_cents": profit_cents,
                "pct_of_max": round(pct_of_max, 2), "max_profit": max_profit,
                "our_prob": our_prob, "contracts": contracts,
                "sell_mode": "full",
                "sell_contracts": contracts,
                "hold_contracts": 0,
                "phase": f"ceiling_exit_{phase}",
            }
        
        if phase == "initial":
            if profit_cents < self.cfg["min_spike_cents"]:
                return None
            if profit_ratio < self.cfg["min_profit_ratio"]:
                return None
            
            # Don't sell cheap YES lottery tickets at small absolute profits
            if entry <= 5 and profit_cents <= 10 and direction == "YES":
                return None
            
            # ── Confidence-scaled sell triggers using % of max profit ──
            # High confidence: hold longer, but sell at 60%+ of max profit
            # Medium confidence: sell at 45%+ of max profit
            # Low confidence: basic thresholds above are enough
            if our_prob >= 0.75:
                if pct_of_max < 0.55:
                    return None
            elif our_prob >= 0.60:
                if pct_of_max < 0.45:
                    return None
            
            # How many to sell to cover 120% of total entry cost
            total_cost = entry * contracts
            target_recover = int(total_cost * 1.20)  # 120% = entry + 20% profit
            sell_contracts = max(1, -(-target_recover // current))  # ceiling division
            
            if sell_contracts >= contracts:
                # Can't keep a moon bag, sell all but 1 if we have 2+
                if contracts >= 2:
                    sell_contracts = contracts - 1
                else:
                    sell_contracts = contracts  # Only 1 contract, full sell
            
            return {
                "ticker": ticker, "direction": direction,
                "entry_cents": entry, "current_cents": current,
                "profit_ratio": round(profit_ratio, 2), "profit_cents": profit_cents,
                "pct_of_max": round(pct_of_max, 2), "max_profit": max_profit,
                "our_prob": our_prob, "contracts": contracts,
                "sell_mode": "partial" if sell_contracts < contracts else "full",
                "sell_contracts": sell_contracts,
                "hold_contracts": contracts - sell_contracts,
                "phase": "cover",
            }
        
        # ══════════════════════════════════════════════════════════════
        # PHASE 2: COVERED — Entry is covered, riding with remaining contracts
        # Now we decide moon bag size based on data confidence
        # If price stays high or keeps climbing: sell more for profit, keep moon bag
        # If price drops from peak: trailing stop triggers sell to lock in remaining profit
        # ══════════════════════════════════════════════════════════════
        elif phase == "covered":
            # Determine ideal moon bag size based on confidence
            if our_prob >= 0.80:
                # Very high confidence — data strongly supports, keep 50% as moon
                moon_pct = 0.50
                trailing_stop_pct = 0.30  # Wider stop, let it breathe
            elif our_prob >= 0.60:
                # Good confidence — keep 30% as moon
                moon_pct = 0.30
                trailing_stop_pct = 0.20
            elif our_prob >= 0.40:
                # Medium confidence — keep 20% as moon
                moon_pct = 0.20
                trailing_stop_pct = 0.15
            else:
                # Low confidence — keep minimal moon (1 contract), tight stop
                moon_pct = 0.0
                trailing_stop_pct = 0.10
            
            moon_contracts = max(1, int(original * moon_pct)) if moon_pct > 0 else (1 if original >= 3 else 0)
            profit_sell = contracts - moon_contracts
            
            # ── Trailing stop check ──
            # If price dropped significantly from peak, sell to lock in profit
            if peak > 0 and current < peak * (1 - trailing_stop_pct):
                # Price dropped past trailing stop — sell everything except moon bag
                if profit_sell > 0:
                    return {
                        "ticker": ticker, "direction": direction,
                        "entry_cents": entry, "current_cents": current,
                        "profit_ratio": round(current / max(entry, 1), 2),
                        "profit_cents": current - entry if entry > 0 else current,
                        "pct_of_max": round(pct_of_max, 2), "max_profit": max_profit,
                        "our_prob": our_prob, "contracts": contracts,
                        "sell_mode": "partial" if moon_contracts > 0 else "full",
                        "sell_contracts": profit_sell if moon_contracts > 0 else contracts,
                        "hold_contracts": moon_contracts,
                        "phase": "profit_stop",
                    }
            
            # ── Profit take: if price is still high, sell down to moon bag ──
            # Wait for price to settle (at least 60 seconds of data) before profit-taking
            history = self.price_history.get(ticker, [])
            if len(history) >= 60 and profit_sell > 0:
                # Check if price has been stable/high for last 60s
                recent_prices = [p for _, p in history[-60:]]
                avg_recent = sum(recent_prices) / len(recent_prices)
                # If average recent price is above our entry + min profit, take profit
                if avg_recent > entry * 1.15 and current > entry:
                    return {
                        "ticker": ticker, "direction": direction,
                        "entry_cents": entry, "current_cents": current,
                        "profit_ratio": round(current / max(entry, 1), 2),
                        "profit_cents": current - entry if entry > 0 else current,
                        "pct_of_max": round(pct_of_max, 2), "max_profit": max_profit,
                        "our_prob": our_prob, "contracts": contracts,
                        "sell_mode": "partial" if moon_contracts > 0 else "full",
                        "sell_contracts": profit_sell if moon_contracts > 0 else contracts,
                        "hold_contracts": moon_contracts,
                        "phase": "profit_take",
                    }
            
            return None  # Waiting — price hasn't triggered any sell condition
        
        # ══════════════════════════════════════════════════════════════
        # PHASE 3: MOON — Only moon bag remains. Ride to settlement.
        # Only sell if trailing stop triggers (price crashing from peak)
        # ══════════════════════════════════════════════════════════════
        elif phase == "moon":
            # Tighter trailing stop for moon bags based on confidence
            if our_prob >= 0.70:
                moon_stop_pct = 0.35  # Let high-confidence moons breathe
            else:
                moon_stop_pct = 0.20  # Tighter stop for low confidence
            
            # Only trigger if we've seen a meaningful peak (at least 1.2x entry or > 60¢)
            if peak >= max(entry * 1.2, 60) and current < peak * (1 - moon_stop_pct):
                return {
                    "ticker": ticker, "direction": direction,
                    "entry_cents": 0, "current_cents": current,
                    "profit_ratio": 0, "profit_cents": current,
                    "pct_of_max": round(pct_of_max, 2), "max_profit": max_profit,
                    "our_prob": our_prob, "contracts": contracts,
                    "sell_mode": "full",
                    "sell_contracts": contracts,
                    "hold_contracts": 0,
                    "phase": "moon_stop",
                }
            
            return None  # Moon bag rides
        
        return None
    
    def execute_sell(self, spike):
        """Sell the position using Kalshi's sell (close) action.
        
        CRITICAL: Uses sell_position() with action='sell', NOT place_order() with action='buy'.
        place_order buy on opposite side can OVERSHOOT and flip the position direction.
        sell_position is capped by Kalshi to never exceed your actual holdings.
        """
        ticker = spike["ticker"]
        direction = spike["direction"]
        sell_mode = spike.get("sell_mode", "full")
        contracts = spike.get("sell_contracts", spike["contracts"]) if sell_mode == "partial" else spike["contracts"]
        sell_price = spike["current_cents"] - self.cfg["sell_discount_cents"]
        
        if contracts <= 0 or sell_price <= 0:
            return False
        
        # Cooldown: don't hammer the same ticker every loop
        now_ts = time.time()
        last_attempt = self.last_sell_attempt.get(ticker, 0)
        if now_ts - last_attempt < self.SELL_COOLDOWN_SEC:
            return False
        self.last_sell_attempt[ticker] = now_ts
        
        # SAFETY: Verify actual position on Kalshi before selling
        try:
            positions = kalshi_client.get_positions()
            kalshi_pos = None
            for p in positions.get("market_positions", []):
                if p["ticker"] == ticker:
                    kalshi_pos = p
                    break
            
            if not kalshi_pos or kalshi_pos.get("position", 0) == 0:
                print(f"  ⚠ No position found on Kalshi for {ticker}, skipping sell")
                return False
            
            actual_count = abs(kalshi_pos["position"])
            actual_direction = "YES" if kalshi_pos["position"] > 0 else "NO"
            
            # CIRCUIT BREAKER: If direction doesn't match what we think, STOP
            if actual_direction != direction:
                print(f"  🚨 DIRECTION MISMATCH: We think {direction} but Kalshi says {actual_direction} for {ticker}!")
                print(f"  🚨 Skipping sell to prevent position flip. Manual intervention needed.")
                return False
            
            # Cap sell to actual position — never sell more than we hold
            if contracts > actual_count:
                print(f"  ⚠ Capping sell from {contracts} to {actual_count} (actual Kalshi position)")
                contracts = actual_count
                spike["sell_contracts"] = contracts
                spike["hold_contracts"] = max(0, actual_count - contracts)
                
        except Exception as e:
            print(f"  ⚠ Position verification failed: {e}, aborting sell for safety")
            return False
        
        try:
            # Use sell_position (action='sell') — Kalshi enforces you can't sell more than you hold
            result = kalshi_client.sell_position(
                ticker=ticker,
                side=direction.lower(),  # sell the side we actually hold (yes or no)
                contracts=contracts,
                price_cents=sell_price,
            )
            
            order = result.get("order", {})
            order_id = order.get("order_id", "?")
            filled = order.get("fill_count", 0)
            status = order.get("status", "?")
            
            mode_label = f"PARTIAL {contracts}/{spike['contracts']}" if sell_mode == "partial" else "FULL"
            hold_note = f", holding {spike.get('hold_contracts', 0)} free" if sell_mode == "partial" else ""
            pom = spike.get('pct_of_max', '?')
            pom_str = f", {int(pom*100)}% of max" if isinstance(pom, (int, float)) else ""
            print(f"  💰 SPIKE {mode_label}: {ticker} {direction} x{contracts} @ {sell_price}¢ "
                  f"(entry {spike['entry_cents']}¢, +{spike['profit_cents']}¢/contract, "
                  f"{spike['profit_ratio']}x{pom_str}) — {status}, filled {filled}{hold_note}")
            
            # If resting (not filled), leave it for a few seconds then cancel if still open
            if status == "resting" and filled == 0:
                print(f"    ⏳ Order resting, will check fill in 3s...")
                time.sleep(3)
                try:
                    # Check order status
                    orders = kalshi_client.get_orders(ticker=ticker, status="resting")
                    for o in orders.get("orders", []):
                        if o.get("order_id") == order_id:
                            kalshi_client.cancel_order(order_id)
                            print(f"    ❌ Canceled unfilled spike sell order")
                            return False
                except:
                    pass
            
            if filled > 0:
                # Log it
                spike["filled"] = filled
                spike["sell_price"] = sell_price
                spike["timestamp"] = datetime.now(timezone.utc).isoformat()
                self.sells_made.append(spike)
                self._save_spike_log(spike)
                
                # Update trades.json
                phase = spike.get("phase", "cover")
                self._mark_sold(ticker, filled, sell_price, phase)
                
                # Phase transition
                remaining = spike.get("hold_contracts", 0)
                if remaining <= 0:
                    # Fully exited
                    self.sell_phases.pop(ticker, None)
                    self.peak_prices.pop(ticker, None)
                elif phase == "cover":
                    self.sell_phases[ticker] = "covered"
                    self.peak_prices[ticker] = spike["current_cents"]  # Reset peak tracking
                    self.entry_prices[ticker] = spike["entry_cents"]  # Keep original entry for reference
                    print(f"    📊 Phase → COVERED. {remaining} contracts riding, entry covered + profit banked.")
                elif phase in ("profit_take", "profit_stop"):
                    self.sell_phases[ticker] = "moon"
                    self.entry_prices[ticker] = 0  # Moon bag is free
                    print(f"    🌙 Phase → MOON. {remaining} contracts riding free to settlement.")
                elif phase == "moon_stop":
                    self.sell_phases.pop(ticker, None)
                    self.peak_prices.pop(ticker, None)
                    print(f"    🛑 Moon bag closed. Trailing stop hit.")
                
                # City log
                city = self.trades.get(ticker, {}).get("city", "unknown")
                try:
                    city_logger.log_position_action(city, "spike_sell", ticker, {
                        "entry": spike["entry_cents"], "sell": sell_price,
                        "profit_ratio": spike["profit_ratio"],
                        "contracts": filled,
                    })
                except:
                    pass
                
                return True
            
            return False
            
        except Exception as e:
            print(f"  ⚠ Sell error for {ticker}: {e}")
            return False
    
    def _save_spike_log(self, spike):
        """Append spike sell to both summary log and detailed trade log."""
        # Summary log (JSON array)
        try:
            log = []
            if os.path.exists(SPIKE_LOG):
                with open(SPIKE_LOG) as f:
                    log = json.load(f)
            log.append(spike)
            with open(SPIKE_LOG, "w") as f:
                json.dump(log, f, indent=2)
        except:
            pass
        
        # Detailed trade log (JSONL with reasoning)
        try:
            sell_mode = spike.get("sell_mode", "full")
            our_prob = spike.get("our_prob", 0)
            entry = spike.get("entry_cents", 0)
            current = spike.get("current_cents", 0)
            phase = spike.get("phase", "unknown")
            sold = spike.get("sell_contracts", spike.get("filled", 0))
            held = spike.get("hold_contracts", 0)
            total = spike.get("contracts", 0)
            
            phase_labels = {
                "cover": f"COVER SELL — Sold {sold}/{total} to recover 120% of entry cost. "
                         f"{held} contracts now riding covered. Prob {our_prob:.0%}. "
                         f"Price: {entry}¢ → {current}¢ ({spike.get('profit_ratio', '?')}x).",
                "profit_take": f"PROFIT TAKE — Price stayed high, selling {sold} more for profit. "
                               f"Keeping {held} as moon bag. Prob {our_prob:.0%}. "
                               f"Price at {current}¢.",
                "profit_stop": f"TRAILING STOP — Price dropped from peak, locking in profit on {sold} contracts. "
                               f"Keeping {held} as moon bag. Prob {our_prob:.0%}. "
                               f"Peak was {self.peak_prices.get(spike.get('ticker'), '?')}¢, now {current}¢.",
                "moon_stop": f"MOON STOP — Trailing stop hit on moon bag. Sold {sold} remaining. "
                             f"Prob {our_prob:.0%}. Price dropped to {current}¢.",
                "metar_lost": f"🌡️ METAR LOSS — Observed temps confirm position lost. "
                              f"Selling {sold} to cut losses. Holding {held}. Price {current}¢.",
                "metar_likely_lost": f"🌡️ METAR LIKELY LOST — Observed temps trending against us. "
                                     f"Selling {sold} to reduce exposure. Holding {held}. Price {current}¢.",
            }
            reasoning = phase_labels.get(phase, f"SELL ({phase}) — {sold} contracts at {current}¢, prob {our_prob:.0%}")
            
            trade_entry = {
                "timestamp": spike.get("timestamp"),
                "ticker": spike.get("ticker"),
                "city": self.trades.get(spike.get("ticker", ""), {}).get("city", "unknown"),
                "direction": spike.get("direction"),
                "sell_mode": sell_mode,
                "phase": phase,
                "entry_cents": entry,
                "sell_cents": spike.get("sell_price"),
                "profit_ratio": spike.get("profit_ratio"),
                "profit_per_contract_cents": spike.get("profit_cents"),
                "contracts_sold": spike.get("filled"),
                "contracts_held": held,
                "total_profit_cents": spike.get("profit_cents", 0) * spike.get("filled", 0),
                "our_prob": our_prob,
                "reasoning": reasoning,
            }
            
            with open(SPIKE_TRADE_LOG, "a") as f:
                f.write(json.dumps(trade_entry) + "\n")
        except:
            pass
    
    def _mark_sold(self, ticker, contracts_sold, sell_price, phase="cover"):
        """Update trades.json to mark position as sold by spike monitor."""
        try:
            with open(TRADES_FILE) as f:
                data = json.load(f)
            for t in data["trades"]:
                if t["ticker"] == ticker and t["status"] == "open":
                    remaining = t.get("contracts", 0) - contracts_sold
                    now_iso = datetime.now(timezone.utc).isoformat()
                    
                    # Build sell record
                    sell_record = {
                        "phase": phase,
                        "sold": contracts_sold,
                        "at_price": sell_price,
                        "timestamp": now_iso,
                        "remaining": remaining,
                    }
                    
                    if remaining <= 0:
                        # Fully exited
                        t["status"] = "spike_sold"
                        t["exit_price_cents"] = sell_price
                        t["exit_timestamp"] = now_iso
                        t["exit_reason"] = f"spike_monitor_{phase}"
                        self.entry_prices.pop(ticker, None)
                    else:
                        # Still have contracts
                        t["contracts"] = remaining
                        if phase in ("profit_take", "profit_stop", "moon_stop"):
                            t["cost_cents"] = 0  # These are free
                    
                    # Append to sell history (track all phases)
                    if "spike_sells" not in t:
                        t["spike_sells"] = []
                        # Migrate old format if present
                        if "spike_partial_sell" in t:
                            t["spike_sells"].append(t.pop("spike_partial_sell"))
                    t["spike_sells"].append(sell_record)
                    break
            
            with open(TRADES_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"⚠ Error updating trades.json: {e}")
    
    def check_metar_sells(self, prices):
        """Check settlement-day positions against real-time METAR observations.
        Only queries METAR for positions whose target_date is today (local time)."""
        sells = []
        
        for ticker, trade in self.trades.items():
            city = trade.get("city")
            direction = self.directions.get(ticker, "YES")
            contracts = self.last_positions.get(ticker, 0)
            if not city or contracts <= 0:
                continue
            
            # Parse target date and strike from ticker
            parts = ticker.split("-")
            if len(parts) != 3:
                continue
            try:
                target_date = datetime.strptime(parts[1], "%y%b%d").strftime("%Y-%m-%d")
            except:
                continue
            strike_str = parts[2]
            
            # Only check METAR on settlement day (today in the city's local timezone)
            from metar_tracker import CITY_STATIONS, _utc_offset_hours
            station = CITY_STATIONS.get(city)
            if station:
                tz_name = station["tz"]
                now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
                offset = _utc_offset_hours(tz_name, now_utc)
                local_now = now_utc + timedelta(hours=offset)
                local_today = local_now.strftime("%Y-%m-%d")
                if target_date != local_today:
                    continue  # Not settlement day for this city, skip
            
            # Evaluate against METAR observations
            try:
                trade_st = self.trades.get(ticker, {}).get("strike_type")
                evaluation = metar_evaluate(city, target_date, direction, strike_str, strike_type=trade_st)
            except Exception as e:
                continue
            
            if not evaluation:
                continue
            
            verdict = evaluation["verdict"]
            sell_pct = evaluation["sell_pct"]
            reason = evaluation["reason"]
            
            if sell_pct <= 0:
                continue
            
            # Calculate contracts to sell
            sell_contracts = max(1, int(contracts * sell_pct))
            if sell_contracts > contracts:
                sell_contracts = contracts
            
            # Always keep at least 1 contract as moon bag if we have 3+
            # (unless it's a definitive loss)
            hold = contracts - sell_contracts
            if hold == 0 and contracts >= 3 and verdict != "lost":
                sell_contracts = contracts - 1
                hold = 1
            
            entry = self.entry_prices.get(ticker, 0)
            price_data = prices.get(ticker, {})
            if direction == "YES":
                current = price_data.get("yes_bid", 0)
            else:
                current = price_data.get("no_bid", 0)
            
            if current <= 0:
                continue
            
            print(f"  🌡️  METAR {verdict.upper()}: {ticker} ({city}) — {reason}")
            print(f"      Selling {sell_contracts}/{contracts} contracts @ {current}¢")
            
            sells.append({
                "ticker": ticker, "direction": direction,
                "entry_cents": entry, "current_cents": current,
                "profit_ratio": round(current / max(entry, 1), 2) if entry > 0 else 0,
                "profit_cents": current - entry if entry > 0 else current,
                "pct_of_max": round((current - entry) / max(100 - entry, 1), 2) if entry > 0 else 0,
                "max_profit": 100 - entry if entry > 0 else 100,
                "our_prob": self.our_probs.get(ticker, 0.5),
                "contracts": contracts,
                "sell_mode": "partial" if hold > 0 else "full",
                "sell_contracts": sell_contracts,
                "hold_contracts": hold,
                "phase": f"metar_{verdict}",
            })
        
        return sells
    
    def run(self):
        """Main loop."""
        # Initial sync from Kalshi
        print("🔄 Initial Kalshi sync...")
        self.sync_from_kalshi()
        self.reload_trades()
        
        print(f"🔍 Spike Monitor started — polling every {self.cfg['poll_interval']}s")
        print(f"   Min profit ratio: {self.cfg['min_profit_ratio']}x")
        print(f"   Min spike: {self.cfg['min_spike_cents']}¢")
        print(f"   Hold if prob > {self.cfg['hold_if_prob_above']}")
        print(f"   Partial sell range: {self.cfg['partial_sell_prob_min']}-{self.cfg['hold_if_prob_above']}")
        print(f"   Tracking {len(self.entry_prices)} relevant positions")
        print()
        
        last_trade_reload = time.time()
        last_sync = time.time()
        last_metar_check = 0  # Check immediately on first loop
        
        while self.running:
            try:
                self.scan_count += 1
                
                # Reload trades every 60 seconds (in case scanner placed new ones)
                if time.time() - last_trade_reload > 60:
                    self.reload_trades()
                    last_trade_reload = time.time()
                
                # Full Kalshi sync every 5 minutes
                if time.time() - last_sync > 300:
                    self.sync_from_kalshi()
                    self.reload_trades()
                    last_sync = time.time()
                
                # Fetch all current prices
                prices = self.get_market_prices()
                
                # Check each position for spikes
                spikes = []
                for ticker, p in prices.items():
                    spike = self.check_spike(ticker, p)
                    if spike:
                        spikes.append(spike)
                
                # Execute sells for any spikes found (max 3 per cycle to prevent runaway)
                sells_this_cycle = 0
                for spike in spikes:
                    if sells_this_cycle >= 3:
                        break
                    if self.execute_sell(spike):
                        sells_this_cycle += 1
                
                # ── METAR intraday check (every 5 minutes) ──
                if time.time() - last_metar_check > 300:
                    try:
                        metar_sells = self.check_metar_sells(prices)
                        for ms in metar_sells:
                            self.execute_sell(ms)
                    except Exception as e:
                        print(f"⚠ METAR check error: {e}")
                    last_metar_check = time.time()
                
                # Periodic status (every 60 scans)
                if self.scan_count % 60 == 0:
                    now = datetime.now(timezone.utc).strftime("%H:%M:%S")
                    print(f"  [{now}] Scan #{self.scan_count} — {len(prices)} positions tracked, "
                          f"{len(self.sells_made)} sells total")
                
                time.sleep(self.cfg["poll_interval"])
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"⚠ Scan error: {e}")
                time.sleep(5)  # Back off on errors
        
        print(f"\n📊 Final stats: {self.scan_count} scans, {len(self.sells_made)} sells")
        if self.sells_made:
            total_profit = sum(s["profit_cents"] * s.get("filled", 0) for s in self.sells_made)
            print(f"   Total spike profit: ${total_profit/100:.2f}")


if __name__ == "__main__":
    monitor = SpikeMonitor()
    monitor.run()
