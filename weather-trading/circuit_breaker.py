#!/usr/bin/env python3
"""
Circuit Breaker — Pauses trading based on rolling 24h realized losses.

Primary trigger: rolling 24h realized losses exceed X% of account balance.
Secondary trigger: daily loss limit (calendar day).

Replaces old consecutive-loss logic.
"""

import json
import os
from datetime import datetime, timezone, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "trading_config.json")
DEFAULT_STATE_FILE = os.path.join(BASE_DIR, "circuit_breaker_state.json")
TRADES_FILE = os.path.join(BASE_DIR, "trades.json")


def _load_config():
    try:
        with open(CONFIG_FILE) as f:
            return json.load(f).get("circuit_breaker", {})
    except:
        return {}


def _load_state():
    cfg = _load_config()
    state_file = os.path.join(BASE_DIR, cfg.get("state_file", "circuit_breaker_state.json"))
    if os.path.exists(state_file):
        try:
            with open(state_file) as f:
                return json.load(f)
        except:
            pass
    return {"tripped_at": None, "trip_reason": None, "last_result": None, "history": []}


def _save_state(state):
    cfg = _load_config()
    state_file = os.path.join(BASE_DIR, cfg.get("state_file", "circuit_breaker_state.json"))
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2)


def _load_trades():
    if not os.path.exists(TRADES_FILE):
        return []
    try:
        with open(TRADES_FILE) as f:
            return json.load(f).get("trades", [])
    except:
        return []


def _get_balance_cents():
    """Get current balance from Kalshi."""
    try:
        import kalshi_client
        bal = kalshi_client.get_balance()
        return bal.get("balance", 0)
    except:
        return 0


def record_result(result: str, ticker: str = "", pnl_cents: int = 0):
    """Record a trade result with P&L for rolling window tracking."""
    state = _load_state()
    state["last_result"] = result
    state["history"].append({
        "result": result,
        "ticker": ticker,
        "pnl_cents": pnl_cents,
        "ts": datetime.now(timezone.utc).isoformat()
    })
    # Keep last 200 for rolling window
    state["history"] = state["history"][-200:]
    _save_state(state)


def _compute_rolling_24h_loss():
    """
    Compute total realized losses in the last 24 hours.
    
    Uses TWO sources and takes the worse (higher loss) number:
    1. circuit_breaker_state.json history (recorded at settlement time)
    2. trades.json settled/exited trades (backup, catches anything missed)
    
    Returns (total_loss_cents, details_str).
    Loss is returned as a positive number.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    
    # Source 1: CB state history
    state = _load_state()
    cb_loss = 0
    for entry in state.get("history", []):
        if entry.get("ts", "") >= cutoff:
            pnl = entry.get("pnl_cents", 0)
            if pnl < 0:
                cb_loss += abs(pnl)
    
    # Source 2: trades.json
    trades_loss = 0
    trades = _load_trades()
    for t in trades:
        ts = t.get("settled_at") or t.get("exit_timestamp") or ""
        if ts >= cutoff and t.get("status") in ("won", "lost", "settled", "exited", "exited_blown", "spike_sold"):
            pnl = t.get("pnl_cents", 0) or 0
            if pnl < 0:
                trades_loss += abs(pnl)
    
    total_loss = max(cb_loss, trades_loss)
    source = "cb_history" if cb_loss >= trades_loss else "trades_json"
    return total_loss, f"24h loss: ${total_loss/100:.2f} (from {source}, cb=${cb_loss/100:.2f}, trades=${trades_loss/100:.2f})"


def _check_rolling_24h_drawdown():
    """Primary trigger: rolling 24h realized losses exceed threshold % of balance."""
    cfg = _load_config()
    dd_pct = cfg.get("rolling_24h_drawdown_pct", 0.10)
    if not dd_pct or dd_pct >= 1.0:
        return False, "no rolling drawdown limit"
    
    total_loss, detail_str = _compute_rolling_24h_loss()
    balance = _get_balance_cents()
    
    if balance <= 0:
        return True, f"Zero/negative balance — {detail_str}"
    
    # Use balance + recent losses as approximate start-of-window capital
    start_capital = balance + total_loss
    if start_capital <= 0:
        return True, f"Start-of-window capital <= 0 — {detail_str}"
    
    loss_ratio = total_loss / start_capital
    
    if loss_ratio >= dd_pct:
        return True, f"Rolling 24h drawdown {loss_ratio:.1%} >= {dd_pct:.0%} (lost ${total_loss/100:.2f} of ${start_capital/100:.2f}) — {detail_str}"
    
    return False, f"Rolling 24h: {loss_ratio:.1%} of {dd_pct:.0%} limit — {detail_str}"


def _check_daily_loss_limit():
    """Secondary trigger: calendar-day loss limit."""
    cfg = _load_config()
    daily_limit_pct = cfg.get("daily_loss_limit_pct", 0.15)
    if not daily_limit_pct:
        return False, "no daily limit set"
    
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    daily_pnl_cents = 0
    
    trades = _load_trades()
    for trade in trades:
        settled_at = trade.get("settled_at", "")
        if settled_at.startswith(today):
            daily_pnl_cents += trade.get("pnl_cents", 0) or 0
    
    if daily_pnl_cents >= 0:
        return False, f"Daily P&L: +${daily_pnl_cents/100:.2f}"
    
    balance = _get_balance_cents()
    starting_value = balance + abs(daily_pnl_cents)
    if starting_value <= 0:
        return True, "Zero starting value"
    
    loss_pct = abs(daily_pnl_cents) / starting_value
    if loss_pct >= daily_limit_pct:
        return True, f"Daily loss {loss_pct:.1%} >= {daily_limit_pct:.1%} (${daily_pnl_cents/100:.2f})"
    
    return False, f"Daily P&L: ${daily_pnl_cents/100:.2f} ({loss_pct:.1%} of {daily_limit_pct:.0%} limit)"


def is_tripped():
    """Check if circuit breaker is active. Returns (tripped: bool, reason: str)."""
    cfg = _load_config()
    if not cfg.get("enabled", True):
        return False, "disabled"
    
    state = _load_state()
    
    # Check if manually tripped with cooldown
    tripped_at = state.get("tripped_at")
    if tripped_at:
        cooldown_hours = cfg.get("cooldown_hours", 6)
        tripped_dt = datetime.fromisoformat(tripped_at)
        elapsed = (datetime.now(timezone.utc) - tripped_dt).total_seconds() / 3600
        if elapsed < cooldown_hours:
            remaining = cooldown_hours - elapsed
            return True, f"TRIPPED ({state.get('trip_reason', '?')}), {remaining:.1f}h cooldown remaining"
        else:
            # Cooldown expired — reset
            state["tripped_at"] = None
            state["trip_reason"] = None
            _save_state(state)
    
    # Primary: rolling 24h drawdown
    dd_tripped, dd_reason = _check_rolling_24h_drawdown()
    if dd_tripped:
        state["tripped_at"] = datetime.now(timezone.utc).isoformat()
        state["trip_reason"] = dd_reason
        _save_state(state)
        return True, f"ROLLING 24H DRAWDOWN: {dd_reason}"
    
    # Secondary: daily loss limit
    daily_tripped, daily_reason = _check_daily_loss_limit()
    if daily_tripped:
        state["tripped_at"] = datetime.now(timezone.utc).isoformat()
        state["trip_reason"] = daily_reason
        _save_state(state)
        return True, f"DAILY LOSS LIMIT: {daily_reason}"
    
    return False, f"ok ({dd_reason})"


def status():
    """Return current circuit breaker status dict."""
    state = _load_state()
    tripped, reason = is_tripped()
    total_loss, loss_detail = _compute_rolling_24h_loss()
    return {
        "tripped": tripped,
        "reason": reason,
        "rolling_24h_loss_cents": total_loss,
        "rolling_24h_detail": loss_detail,
        "last_result": state.get("last_result"),
        "recent_history": state.get("history", [])[-10:],
    }
