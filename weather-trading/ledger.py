#!/usr/bin/env python3
"""
Ledger — Append-only trade ledger with FIFO lot engine.

All positions normalized to YES exposure:
  BUY YES @ p  →  (+q, p)
  SELL YES @ p →  (-q, p)
  BUY NO @ p   →  (-q, 100-p)   (equivalent to SELL YES)
  SELL NO @ p  →  (+q, 100-p)   (equivalent to BUY YES)

P&L rules:
  - Exit: (exit_price - entry_price) * qty - fees
  - Settlement: closes remaining lots at 100 (YES wins) or 0 (NO wins)
  - Settlement on qty=0 produces zero P&L (prevents phantom P&L)

Storage:
  ledger.jsonl  — append-only event log (one JSON per line)
  ledger_state.json — checkpoint (lots, seen fills, totals; rebuildable from ledger)
"""

import json
import os
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LEDGER_FILE = os.path.join(BASE_DIR, "ledger.jsonl")
STATE_FILE = os.path.join(BASE_DIR, "ledger_state.json")


# ═══════════════════════════════════════════════════════════════
# Event writing
# ═══════════════════════════════════════════════════════════════

def _append_event(event):
    """Append a single event to ledger.jsonl. Crash-safe: newline before JSON."""
    with open(LEDGER_FILE, "a") as f:
        f.write("\n" + json.dumps(event, separators=(",", ":")))
        f.flush()
        os.fsync(f.fileno())


def new_decision_id():
    return str(uuid.uuid4())


def new_group_id():
    return str(uuid.uuid4())


def log_decision(decision_id, action, market_ticker, side, direction,
                 qty_intended, limit_price_cents, reason="",
                 group_id=None, model_data=None):
    """Log a trading decision (intent). No P&L here."""
    event = {
        "type": "DECISION",
        "decision_id": decision_id,
        "ts": datetime.now(timezone.utc).timestamp(),
        "action": action,
        "market_ticker": market_ticker,
        "side": side,
        "dir": direction,
        "qty_intended": qty_intended,
        "limit_price_cents": limit_price_cents,
        "reason": reason,
    }
    if group_id:
        event["group_id"] = group_id
    if model_data:
        event["model"] = model_data
    _append_event(event)
    return event


def log_fill(fill_id, order_id, market_ticker, side, direction,
             qty, price_cents, fee_cents=0, decision_id=None,
             client_order_id=None, ts=None):
    """Log a fill from Kalshi. Writes raw + applies to lot engine."""
    event = {
        "type": "FILL",
        "fill_id": fill_id,
        "order_id": order_id,
        "market_ticker": market_ticker,
        "side": side,
        "dir": direction,
        "qty": qty,
        "price_cents": price_cents,
        "fee_cents": fee_cents,
        "ts": ts or datetime.now(timezone.utc).timestamp(),
    }
    if decision_id:
        event["decision_id"] = decision_id
    if client_order_id:
        event["client_order_id"] = client_order_id
    _append_event(event)
    return event


def log_settlement(market_ticker, result, settle_fee_cents=0,
                   actual_temperature=None, model_forecast=None, ts=None):
    """Log a settlement from Kalshi."""
    event = {
        "type": "SETTLEMENT",
        "market_ticker": market_ticker,
        "result": result,
        "settle_fee_cents": settle_fee_cents,
        "ts": ts or datetime.now(timezone.utc).timestamp(),
    }
    if actual_temperature is not None:
        event["actual_temperature"] = actual_temperature
    if model_forecast is not None:
        event["model_forecast"] = model_forecast
        if actual_temperature is not None:
            event["forecast_error"] = round(actual_temperature - model_forecast, 2)
    _append_event(event)
    return event


# ═══════════════════════════════════════════════════════════════
# YES normalization
# ═══════════════════════════════════════════════════════════════

def normalize_fill(side, direction, qty, price_cents):
    """Convert any fill to YES terms.

    Returns (qty_signed_yes, effective_price_yes_cents).
    Positive qty = opening/increasing YES. Negative = closing/reducing.
    """
    side = side.upper()
    direction = direction.upper()

    if side == "YES" and direction == "BUY":
        return (+qty, price_cents)
    elif side == "YES" and direction == "SELL":
        return (-qty, price_cents)
    elif side == "NO" and direction == "BUY":
        # Buying NO = selling YES equivalent
        return (-qty, 100 - price_cents)
    elif side == "NO" and direction == "SELL":
        # Selling NO = buying YES equivalent
        return (+qty, 100 - price_cents)
    else:
        raise ValueError(f"Invalid side/direction: {side}/{direction}")


# ═══════════════════════════════════════════════════════════════
# FIFO lot engine
# ═══════════════════════════════════════════════════════════════

class Lot:
    __slots__ = ("qty_remaining", "entry_price", "entry_fee_per_contract",
                 "decision_id", "open_ts")

    def __init__(self, qty, entry_price, fee_cents=0, decision_id=None, ts=None):
        self.qty_remaining = qty
        self.entry_price = entry_price
        self.entry_fee_per_contract = fee_cents / abs(qty) if qty != 0 else 0
        self.decision_id = decision_id
        self.open_ts = ts

    def to_dict(self):
        return {
            "qty_remaining": self.qty_remaining,
            "entry_price": self.entry_price,
            "entry_fee_per_contract": round(self.entry_fee_per_contract, 4),
            "decision_id": self.decision_id,
            "open_ts": self.open_ts,
        }

    @classmethod
    def from_dict(cls, d):
        lot = cls(d["qty_remaining"], d["entry_price"])
        lot.entry_fee_per_contract = d.get("entry_fee_per_contract", 0)
        lot.decision_id = d.get("decision_id")
        lot.open_ts = d.get("open_ts")
        return lot


class LotEngine:
    """FIFO lot engine for YES-normalized positions."""

    def __init__(self):
        self.lots = defaultdict(deque)         # ticker -> deque of Lot
        self.realized_pnl = defaultdict(int)   # ticker -> cents
        self.total_fees = defaultdict(int)      # ticker -> cents
        self.seen_fill_ids = set()
        self.pnl_by_decision = defaultdict(int) # decision_id -> cents

    def apply_fill(self, ticker, qty_signed, price_cents, fee_cents=0,
                   decision_id=None, fill_id=None, ts=None):
        """Apply a YES-normalized fill. Returns realized P&L in cents (0 for opens).

        Supports short lots: if selling more YES than held, excess opens short lots
        (negative qty_remaining). Buying YES closes short lots first (FIFO), then
        opens long lots with any remainder.
        """
        if fill_id and fill_id in self.seen_fill_ids:
            return 0  # Idempotent: already processed
        if fill_id:
            self.seen_fill_ids.add(fill_id)

        self.total_fees[ticker] += fee_cents
        ticker_lots = self.lots[ticker]
        pnl = 0
        total_qty = abs(qty_signed)
        closed_qty = 0

        if qty_signed > 0:
            # BUY YES: close short lots first (FIFO), then open long lots
            remaining = qty_signed

            while remaining > 0 and ticker_lots and ticker_lots[0].qty_remaining < 0:
                lot = ticker_lots[0]
                short_qty = abs(lot.qty_remaining)
                matched = min(remaining, short_qty)

                # Short P&L: entry_price - exit_price
                lot_pnl = matched * (lot.entry_price - price_cents)
                entry_fee_alloc = round(matched * lot.entry_fee_per_contract)
                lot_pnl -= entry_fee_alloc

                pnl += lot_pnl
                if lot.decision_id:
                    self.pnl_by_decision[lot.decision_id] += lot_pnl

                lot.qty_remaining += matched
                remaining -= matched
                closed_qty += matched

                if lot.qty_remaining == 0:
                    ticker_lots.popleft()

            if remaining > 0:
                # Allocate fee proportionally to opening portion
                open_fee = round(fee_cents * remaining / total_qty) if total_qty > 0 else 0
                ticker_lots.append(Lot(
                    qty=remaining,
                    entry_price=price_cents,
                    fee_cents=open_fee,
                    decision_id=decision_id,
                    ts=ts,
                ))

        elif qty_signed < 0:
            # SELL YES: close long lots first (FIFO), then open short lots
            to_close = abs(qty_signed)

            while to_close > 0 and ticker_lots and ticker_lots[0].qty_remaining > 0:
                lot = ticker_lots[0]
                matched = min(to_close, lot.qty_remaining)

                lot_pnl = matched * (price_cents - lot.entry_price)
                entry_fee_alloc = round(matched * lot.entry_fee_per_contract)
                lot_pnl -= entry_fee_alloc

                pnl += lot_pnl
                if lot.decision_id:
                    self.pnl_by_decision[lot.decision_id] += lot_pnl

                lot.qty_remaining -= matched
                to_close -= matched
                closed_qty += matched

                if lot.qty_remaining <= 0:
                    ticker_lots.popleft()

            if to_close > 0:
                open_fee = round(fee_cents * to_close / total_qty) if total_qty > 0 else 0
                ticker_lots.append(Lot(
                    qty=-to_close,
                    entry_price=price_cents,
                    fee_cents=open_fee,
                    decision_id=decision_id,
                    ts=ts,
                ))

        # Fee on closing portion goes to realized P&L
        if closed_qty > 0:
            close_fee = round(fee_cents * closed_qty / total_qty) if total_qty > 0 else 0
            pnl -= close_fee

        if pnl != 0 or closed_qty > 0:
            self.realized_pnl[ticker] += pnl

        return pnl

    def apply_settlement(self, ticker, result):
        """Settle remaining lots. YES→exit at 100, NO→exit at 0, VOID→exit at entry (flat).

        Returns realized P&L from settlement.
        """
        ticker_lots = self.lots[ticker]
        if not ticker_lots:
            return 0  # Nothing to settle — prevents phantom P&L

        if result.upper() == "YES":
            payout = 100
        elif result.upper() == "NO":
            payout = 0
        elif result.upper() == "VOID":
            # Return entry price (flat)
            pnl = 0
            while ticker_lots:
                lot = ticker_lots.popleft()
                # Refund entry fees on void
                pnl += 0  # Flat
            self.realized_pnl[ticker] += pnl
            return pnl
        else:
            # Scalar: treat result as payout value
            try:
                payout = int(result)
            except (ValueError, TypeError):
                payout = 0

        pnl = 0
        while ticker_lots:
            lot = ticker_lots.popleft()
            if lot.qty_remaining > 0:
                # Long: profit when payout > entry
                lot_pnl = lot.qty_remaining * (payout - lot.entry_price)
                entry_fee_alloc = round(lot.qty_remaining * lot.entry_fee_per_contract)
                lot_pnl -= entry_fee_alloc
            elif lot.qty_remaining < 0:
                # Short: profit when entry > payout
                abs_qty = abs(lot.qty_remaining)
                lot_pnl = abs_qty * (lot.entry_price - payout)
                entry_fee_alloc = round(abs_qty * lot.entry_fee_per_contract)
                lot_pnl -= entry_fee_alloc
            else:
                continue
            pnl += lot_pnl
            if lot.decision_id:
                self.pnl_by_decision[lot.decision_id] += lot_pnl

        self.realized_pnl[ticker] += pnl
        return pnl

    def remaining_qty(self, ticker):
        """Net YES contracts held for a ticker (negative = short)."""
        return sum(lot.qty_remaining for lot in self.lots[ticker])

    def open_positions(self):
        """All tickers with remaining inventory (long or short)."""
        return {t: self.remaining_qty(t) for t in self.lots if self.remaining_qty(t) != 0}

    def total_realized_pnl(self):
        """Sum of all realized P&L across all tickers, in cents."""
        return sum(self.realized_pnl.values())

    def total_fees_paid(self):
        """Sum of all fees across all tickers, in cents."""
        return sum(self.total_fees.values())

    def pnl_summary(self):
        """Summary dict for reporting."""
        return {
            "total_realized_pnl_cents": self.total_realized_pnl(),
            "total_fees_cents": self.total_fees_paid(),
            "open_positions": self.open_positions(),
            "tickers_traded": len(self.realized_pnl),
        }

    # ── State persistence ──

    def save_state(self, path=None):
        """Save checkpoint to JSON."""
        path = path or STATE_FILE
        state = {
            "lots": {t: [l.to_dict() for l in lots] for t, lots in self.lots.items() if lots},
            "realized_pnl": dict(self.realized_pnl),
            "total_fees": dict(self.total_fees),
            "seen_fill_ids": list(self.seen_fill_ids),
            "pnl_by_decision": dict(self.pnl_by_decision),
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def load_state(self, path=None):
        """Load checkpoint from JSON."""
        path = path or STATE_FILE
        if not os.path.exists(path):
            return False
        try:
            with open(path) as f:
                state = json.load(f)
            self.lots = defaultdict(deque)
            for t, lot_dicts in state.get("lots", {}).items():
                self.lots[t] = deque(Lot.from_dict(d) for d in lot_dicts)
            self.realized_pnl = defaultdict(int, {k: v for k, v in state.get("realized_pnl", {}).items()})
            self.total_fees = defaultdict(int, {k: v for k, v in state.get("total_fees", {}).items()})
            self.seen_fill_ids = set(state.get("seen_fill_ids", []))
            self.pnl_by_decision = defaultdict(int, {k: v for k, v in state.get("pnl_by_decision", {}).items()})
            return True
        except Exception as e:
            print(f"  ⚠️ Failed to load ledger state: {e}")
            return False

    def rebuild_from_ledger(self, path=None):
        """Rebuild entire state by replaying ledger.jsonl."""
        path = path or LEDGER_FILE
        self.__init__()
        if not os.path.exists(path):
            return

        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue  # Skip corrupted lines (crash-safe)

                if event["type"] == "FILL":
                    qty_signed, eff_price = normalize_fill(
                        event["side"], event["dir"],
                        event["qty"], event["price_cents"]
                    )
                    self.apply_fill(
                        ticker=event["market_ticker"],
                        qty_signed=qty_signed,
                        price_cents=eff_price,
                        fee_cents=event.get("fee_cents", 0),
                        decision_id=event.get("decision_id"),
                        fill_id=event.get("fill_id"),
                        ts=event.get("ts"),
                    )
                elif event["type"] == "SETTLEMENT":
                    self.apply_settlement(
                        ticker=event["market_ticker"],
                        result=event["result"],
                    )


# ═══════════════════════════════════════════════════════════════
# Fill ingestion from Kalshi API
# ═══════════════════════════════════════════════════════════════

def ingest_fills(engine, kalshi_fills, decision_map=None):
    """Process a list of Kalshi fill dicts into the lot engine.

    Args:
        engine: LotEngine instance
        kalshi_fills: list of fill dicts from Kalshi API
        decision_map: optional dict mapping client_order_id -> decision_id
    Returns:
        list of (fill_id, realized_pnl) for fills that closed positions
    """
    decision_map = decision_map or {}
    results = []

    for f in sorted(kalshi_fills, key=lambda x: x.get("ts", 0)):
        fill_id = f.get("fill_id") or f.get("trade_id")
        if fill_id in engine.seen_fill_ids:
            continue

        side = f["side"].upper()
        action = f["action"].upper()
        ticker = f.get("market_ticker") or f["ticker"]
        qty = f["count"]
        fee_cents = round(float(f.get("fee_cost", 0)) * 100)
        decision_id = decision_map.get(f.get("order_id"))

        # Determine price in the side's own terms
        if side == "YES":
            price = f["yes_price"]
        else:
            price = f.get("no_price", 100 - f["yes_price"])

        # Direction
        direction = "BUY" if action == "BUY" else "SELL"

        # Log raw fill
        log_fill(
            fill_id=fill_id,
            order_id=f.get("order_id", ""),
            market_ticker=ticker,
            side=side,
            direction=direction,
            qty=qty,
            price_cents=price,
            fee_cents=fee_cents,
            decision_id=decision_id,
            client_order_id=f.get("client_order_id"),
            ts=f.get("ts"),
        )

        # Normalize and apply
        qty_signed, eff_price = normalize_fill(side, direction, qty, price)
        pnl = engine.apply_fill(
            ticker=ticker,
            qty_signed=qty_signed,
            price_cents=eff_price,
            fee_cents=fee_cents,
            decision_id=decision_id,
            fill_id=fill_id,
            ts=f.get("ts"),
        )
        results.append((fill_id, pnl))

    return results


def ingest_settlements(engine, kalshi_settlements):
    """Process a list of Kalshi settlement dicts into the lot engine.

    Returns list of (ticker, realized_pnl) for settled positions.
    """
    results = []
    for s in sorted(kalshi_settlements, key=lambda x: x.get("settled_time", "")):
        ticker = s["ticker"]
        result = s["market_result"].upper()
        fee_cents = round(float(s.get("fee_cost", 0)) * 100)

        log_settlement(
            market_ticker=ticker,
            result=result,
            settle_fee_cents=fee_cents,
            ts=s.get("settled_time"),
        )

        pnl = engine.apply_settlement(ticker, result)
        results.append((ticker, pnl))

    return results


# ═══════════════════════════════════════════════════════════════
# Quick CLI for testing / manual replay
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "rebuild":
        engine = LotEngine()
        engine.rebuild_from_ledger()
        engine.save_state()
        summary = engine.pnl_summary()
        print(f"Rebuilt from ledger:")
        print(f"  Realized P&L: {summary['total_realized_pnl_cents']}¢ = ${summary['total_realized_pnl_cents']/100:.2f}")
        print(f"  Total fees: {summary['total_fees_cents']}¢ = ${summary['total_fees_cents']/100:.2f}")
        print(f"  Open positions: {summary['open_positions']}")
        print(f"  Tickers traded: {summary['tickers_traded']}")

    elif len(sys.argv) > 1 and sys.argv[1] == "replay-historical":
        # Replay historical fills/settlements from cached API data
        engine = LotEngine()
        fills_path = "/tmp/kalshi_fills_all.json"
        sett_path = "/tmp/kalshi_settlements.json"

        if os.path.exists(fills_path) and os.path.exists(sett_path):
            with open(fills_path) as f:
                fills = json.load(f)
            with open(sett_path) as f:
                settlements = json.load(f)

            print(f"Replaying {len(fills)} fills + {len(settlements)} settlements...")
            fill_results = ingest_fills(engine, fills)
            sett_results = ingest_settlements(engine, settlements)

            summary = engine.pnl_summary()
            print(f"\nResults:")
            print(f"  Realized P&L: {summary['total_realized_pnl_cents']}¢ = ${summary['total_realized_pnl_cents']/100:.2f}")
            print(f"  Total fees: {summary['total_fees_cents']}¢ = ${summary['total_fees_cents']/100:.2f}")
            print(f"  Open positions: {len(summary['open_positions'])}")
            print(f"  Tickers traded: {summary['tickers_traded']}")
            print(f"\n  Expected balance change: ${summary['total_realized_pnl_cents']/100:.2f}")
            print(f"  Actual balance change: $-323.22 (510.76 → 187.54)")
            print(f"  Gap: ${(summary['total_realized_pnl_cents'] - (-32322))/100:.2f}")
        else:
            print(f"Need {fills_path} and {sett_path} — run API cache first")

    else:
        print("Usage: python3 ledger.py [rebuild|replay-historical]")
