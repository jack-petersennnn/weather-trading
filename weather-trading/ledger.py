#!/usr/bin/env python3
"""
Ledger — Append-only trade ledger with FIFO lot engine.

Kalshi mechanics modeled explicitly:
  - BUY SIDE: opens inventory on that side at side_price.
  - SELL SIDE: equivalent to BUY opposite side at other_side_price.
  - YES+NO pair on same market nets immediately for $1.00.
  - Settlement pays winning side at $1.00 per contract.

Fill transformation (normalize_fill):
  BUY YES @ p  → (YES, qty, p)
  BUY NO @ p   → (NO, qty, p)
  SELL YES @ p → (NO, qty, 100-p)   — sell YES = buy NO
  SELL NO @ p  → (YES, qty, 100-p)  — sell NO = buy YES

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
                   actual_temperature=None, model_forecast=None,
                   payout_cents=None, ts=None):
    """Log a settlement from Kalshi."""
    event = {
        "type": "SETTLEMENT",
        "market_ticker": market_ticker,
        "result": result,
        "settle_fee_cents": settle_fee_cents,
        "ts": ts or datetime.now(timezone.utc).timestamp(),
    }
    if payout_cents is not None:
        event["payout_cents"] = payout_cents
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
    """Transform fill to a synthetic BUY in Kalshi's internal accounting.

    Returns (buy_side, qty, buy_price_cents):
      BUY YES @ p  → ("YES", qty, p)
      BUY NO  @ p  → ("NO",  qty, p)
      SELL YES @ p → ("NO",  qty, 100-p)   — sell YES = buy NO
      SELL NO  @ p → ("YES", qty, 100-p)   — sell NO  = buy YES
    """
    side = side.upper()
    direction = direction.upper()

    if side == "YES" and direction == "BUY":
        return ("YES", qty, price_cents)
    elif side == "NO" and direction == "BUY":
        return ("NO", qty, price_cents)
    elif side == "YES" and direction == "SELL":
        return ("NO", qty, 100 - price_cents)
    elif side == "NO" and direction == "SELL":
        return ("YES", qty, 100 - price_cents)
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
    """FIFO lot engine tracking YES and NO inventory separately.
    
    Models Kalshi mechanics: every fill is a BUY (sells transformed to buy
    opposite side). YES+NO pairs on the same market net immediately for $1.00.
    """

    def __init__(self):
        self.lots = defaultdict(lambda: {"YES": deque(), "NO": deque()})
        self.realized_pnl = defaultdict(int)   # ticker -> cents
        self.total_fees = defaultdict(int)      # ticker -> cents
        self.seen_fill_ids = set()
        self.pnl_by_decision = defaultdict(int) # decision_id -> cents

    def apply_fill(self, ticker, buy_side, qty, price_cents, fee_cents=0,
                   decision_id=None, fill_id=None, ts=None):
        """Apply one fill transformed into a synthetic BUY.
        
        buy_side: "YES" or "NO" — which side we're buying
        qty: number of contracts
        price_cents: price paid per contract
        
        If the opposite side has inventory, nets YES+NO pairs for $1.00 each.
        Returns realized P&L in cents (0 for pure opens).
        """
        if fill_id and fill_id in self.seen_fill_ids:
            return 0
        if fill_id:
            self.seen_fill_ids.add(fill_id)

        self.total_fees[ticker] += fee_cents
        ticker_lots = self.lots[ticker]
        pnl = 0
        total_qty = qty
        remaining = qty

        opposite = "NO" if buy_side == "YES" else "YES"
        buy_queue = ticker_lots[buy_side]
        opposite_queue = ticker_lots[opposite]

        # Net against opposite side (YES+NO pair = $1.00)
        while remaining > 0 and opposite_queue:
            lot = opposite_queue[0]
            matched = min(remaining, lot.qty_remaining)

            # Netting P&L: pair pays $1.00. We paid entry for opposite + price for this side.
            # P&L = 100 - price_cents - lot.entry_price (per contract)
            lot_pnl = matched * (100 - price_cents - lot.entry_price)
            lot_pnl -= round(matched * lot.entry_fee_per_contract)

            pnl += lot_pnl
            if lot.decision_id:
                self.pnl_by_decision[lot.decision_id] += lot_pnl

            lot.qty_remaining -= matched
            remaining -= matched

            if lot.qty_remaining == 0:
                opposite_queue.popleft()

        # Fee allocation: closing portion goes to P&L, opening portion to new lots
        if total_qty > 0:
            matched_qty = total_qty - remaining
            if matched_qty > 0:
                pnl -= round(fee_cents * matched_qty / total_qty)
            open_fee = fee_cents - round(fee_cents * matched_qty / total_qty)
        else:
            open_fee = 0

        # Open new lots for remaining quantity
        if remaining > 0:
            buy_queue.append(Lot(
                qty=remaining,
                entry_price=price_cents,
                fee_cents=open_fee,
                decision_id=decision_id,
                ts=ts,
            ))

        if pnl != 0:
            self.realized_pnl[ticker] += pnl

        return pnl

    def apply_settlement(self, ticker, result, payout_cents=None):
        """Settle any unpaired lots at market payout.
        
        YES wins: YES lots get 100¢, NO lots get 0¢
        NO wins: YES lots get 0¢, NO lots get 100¢
        Scalar: use payout_cents directly
        """
        ticker_lots = self.lots[ticker]
        if not ticker_lots["YES"] and not ticker_lots["NO"]:
            return 0

        result_upper = result.upper()
        if result_upper == "YES":
            yes_payout = 100
        elif result_upper == "NO":
            yes_payout = 0
        elif result_upper == "VOID":
            ticker_lots["YES"].clear()
            ticker_lots["NO"].clear()
            return 0
        else:
            # Scalar
            if payout_cents is None:
                try:
                    payout_cents = int(result)
                except (ValueError, TypeError):
                    payout_cents = 0
            yes_payout = max(0, min(100, int(payout_cents)))

        no_payout = 100 - yes_payout
        pnl = 0

        for side, payout in (("YES", yes_payout), ("NO", no_payout)):
            queue = ticker_lots[side]
            while queue:
                lot = queue.popleft()
                lot_pnl = lot.qty_remaining * (payout - lot.entry_price)
                lot_pnl -= round(lot.qty_remaining * lot.entry_fee_per_contract)
                pnl += lot_pnl
                if lot.decision_id:
                    self.pnl_by_decision[lot.decision_id] += lot_pnl

        self.realized_pnl[ticker] += pnl
        return pnl

    def remaining_qty(self, ticker):
        """Net contracts held for a ticker by side."""
        lots = self.lots[ticker]
        return {
            "YES": sum(lot.qty_remaining for lot in lots["YES"]),
            "NO": sum(lot.qty_remaining for lot in lots["NO"]),
        }

    def open_positions(self):
        """All tickers with non-zero inventory."""
        positions = {}
        for ticker in self.lots:
            qty = self.remaining_qty(ticker)
            if qty["YES"] or qty["NO"]:
                positions[ticker] = qty
        return positions

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
            "lots": {
                t: {side: [l.to_dict() for l in sides[side]]
                    for side in ("YES", "NO") if sides[side]}
                for t, sides in self.lots.items()
                if sides["YES"] or sides["NO"]
            },
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
            self.lots = defaultdict(lambda: {"YES": deque(), "NO": deque()})
            for t, side_lots in state.get("lots", {}).items():
                self.lots[t] = {
                    "YES": deque(Lot.from_dict(d) for d in side_lots.get("YES", [])),
                    "NO": deque(Lot.from_dict(d) for d in side_lots.get("NO", [])),
                }
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
                    buy_side, qty, eff_price = normalize_fill(
                        event["side"], event["dir"],
                        event["qty"], event["price_cents"]
                    )
                    self.apply_fill(
                        ticker=event["market_ticker"],
                        buy_side=buy_side,
                        qty=qty,
                        price_cents=eff_price,
                        fee_cents=event.get("fee_cents", 0),
                        decision_id=event.get("decision_id"),
                        fill_id=event.get("fill_id"),
                        ts=event.get("ts"),
                    )
                elif event["type"] == "SETTLEMENT":
                    payout_cents = event.get("payout_cents")
                    self.apply_settlement(
                        ticker=event["market_ticker"],
                        result=event["result"],
                        payout_cents=payout_cents,
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
        buy_side, norm_qty, eff_price = normalize_fill(side, direction, qty, price)
        pnl = engine.apply_fill(
            ticker=ticker,
            buy_side=buy_side,
            qty=norm_qty,
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

        payout_cents = s.get("value")

        log_settlement(
            market_ticker=ticker,
            result=result,
            settle_fee_cents=fee_cents,
            payout_cents=payout_cents,
            ts=s.get("settled_time"),
        )

        pnl = engine.apply_settlement(ticker, result, payout_cents=payout_cents)
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
