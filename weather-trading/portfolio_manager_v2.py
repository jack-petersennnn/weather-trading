#!/usr/bin/env python3
"""
Portfolio Manager v2 - Dual Accounting System

Uses balance_sim for canonical account truth and FIFO LotEngine for attribution.
This fixes the $483 gap issue by using the proven accurate balance simulation
for account-level reporting while keeping FIFO for trade-level attribution.
"""

import json
import os
import sys
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ledger
import kalshi_client
from integrated_balance_sim import get_canonical_account_truth

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRADES_FILE = os.path.join(BASE_DIR, "trades.json")
BACKUP_SUFFIX = datetime.now(timezone.utc).strftime("_%Y%m%d_%H%M%S")


class PortfolioManagerV2:
    """Dual accounting portfolio manager: balance_sim for truth + FIFO for attribution."""
    
    def __init__(self):
        self.engine = ledger.LotEngine()
        self.engine.load_state()  # Load existing FIFO state for attribution
        self.starting_capital_cents = 51076  # $510.76
        
    def sync_from_kalshi(self, dry_run=False):
        """Sync fills from Kalshi API and update ledger."""
        try:
            # Get recent fills from Kalshi
            fills_response = kalshi_client.get_fills()
            fills = fills_response.get("fills", [])
            
            new_fills = []
            for fill in fills:
                fill_id = fill.get("fill_id")
                if fill_id and fill_id not in self.engine.seen_fill_ids:
                    new_fills.append(fill)
            
            print(f"Processing {len(new_fills)} new fills...")
            
            # Process new fills into ledger
            realized_pnl_events = []
            for fill in new_fills:
                if dry_run:
                    print(f"  [DRY RUN] Fill: {fill['market_ticker']} {fill['side']} {fill['dir']} {fill['qty']} @ {fill['price_cents']}¢")
                    continue
                    
                # Log the raw fill
                ledger.log_fill(
                    fill_id=fill["fill_id"],
                    order_id=fill["order_id"],
                    market_ticker=fill["market_ticker"], 
                    side=fill["side"],
                    direction=fill["dir"],
                    qty=fill["qty"],
                    price_cents=fill["price_cents"],
                    fee_cents=fill.get("fee_cents", 0),
                    ts=fill.get("ts")
                )
                
                # Apply to lot engine (for attribution only)
                buy_side, qty, price = ledger.normalize_fill(
                    fill["side"], fill["dir"], fill["qty"], fill["price_cents"]
                )
                
                realized_pnl = self.engine.apply_fill(
                    ticker=fill["market_ticker"],
                    buy_side=buy_side,
                    qty=qty,
                    price_cents=price,
                    fee_cents=fill.get("fee_cents", 0),
                    fill_id=fill["fill_id"],
                    ts=fill.get("ts")
                )
                
                if realized_pnl != 0:
                    realized_pnl_events.append({
                        "ticker": fill["market_ticker"],
                        "realized_pnl_attribution": realized_pnl,
                        "fill_id": fill["fill_id"]
                    })
                    
                print(f"  ✓ {fill['market_ticker']} {fill['side']} {fill['dir']} {fill['qty']} @ {fill['price_cents']}¢ → Attribution P&L: {realized_pnl:+d}¢")
            
            if not dry_run and new_fills:
                self.engine.save_state()
                print(f"Ledger state updated with {len(new_fills)} new fills")
                
            return realized_pnl_events
            
        except Exception as e:
            print(f"Error syncing from Kalshi: {e}")
            return []
    
    def get_portfolio_summary(self):
        """Get complete portfolio summary using dual accounting system."""
        
        # 1. Get canonical account truth from balance_sim
        try:
            canonical = get_canonical_account_truth()
            print("✅ Retrieved canonical account truth from balance_sim")
        except Exception as e:
            print(f"⚠️  Warning: Could not get canonical truth: {e}")
            # Fallback to Kalshi API
            canonical = None
        
        # 2. Get Kalshi live balance for comparison
        try:
            balance_response = kalshi_client.get_balance()
            kalshi_available_cash = balance_response.get("balance", 0)
            kalshi_portfolio_value = balance_response.get("portfolio_value", 0)
        except Exception as e:
            print(f"Warning: Could not fetch Kalshi balance: {e}")
            kalshi_available_cash = 0
            kalshi_portfolio_value = 0
        
        # 3. Get FIFO attribution data
        fifo_realized_pnl = self.engine.total_realized_pnl()
        fifo_total_fees = self.engine.total_fees_paid()
        open_positions = self.engine.open_positions()
        
        # 4. Calculate unrealized P&L for open positions (using FIFO cost basis)
        unrealized_pnl = 0
        open_cost_basis = 0
        position_details = []
        
        try:
            for ticker, sides in open_positions.items():
                for side in ["YES", "NO"]:
                    qty = sides.get(side, 0)
                    if qty > 0:
                        # Get current market price
                        market = kalshi_client.get_market(ticker)
                        if side == "YES":
                            current_price = market.get("yes_bid", 50)
                        else:
                            current_price = market.get("no_bid", 50)
                        
                        # Calculate cost basis from FIFO lots
                        ticker_lots = self.engine.lots[ticker][side]
                        position_cost = sum(lot.qty_remaining * lot.entry_price for lot in ticker_lots)
                        current_value = qty * current_price
                        position_unrealized = current_value - position_cost
                        
                        unrealized_pnl += position_unrealized
                        open_cost_basis += position_cost
                        
                        position_details.append({
                            "ticker": ticker,
                            "side": side,
                            "qty": qty,
                            "cost_basis_cents": position_cost,
                            "current_value_cents": current_value,
                            "unrealized_pnl_cents": position_unrealized,
                            "data_source": "fifo_attribution"
                        })
                        
        except Exception as e:
            print(f"Warning: Could not calculate unrealized P&L: {e}")
            # Fallback: use portfolio_value as proxy for position value
            if kalshi_portfolio_value > kalshi_available_cash:
                open_cost_basis = kalshi_portfolio_value - kalshi_available_cash
        
        # 5. Build dual-source summary
        if canonical:
            # Use balance_sim as canonical truth
            account_summary = {
                # === CANONICAL ACCOUNT TRUTH (from balance_sim) ===
                "available_capital_cents": canonical['available_capital_cents'],
                "portfolio_value_cents": kalshi_portfolio_value,  # Use live Kalshi value for positions
                "canonical_balance_cents": canonical['canonical_balance_cents'],
                "reconciliation_gap_cents": canonical['reconciliation_gap_cents'],
                "reconciliation_status": canonical['reconciliation_status'],
                "mecnet_note": canonical['mecnet_note'],
                
                # Net P&L from canonical simulation
                "total_pnl_cents": canonical['net_pnl_cents'],
                
                # === ATTRIBUTION LAYER (from FIFO) ===
                "fifo_realized_pnl_cents": fifo_realized_pnl,
                "fifo_unrealized_pnl_cents": unrealized_pnl,
                "fifo_total_fees_cents": fifo_total_fees,
                
                # === COMPARATIVE DATA ===
                "kalshi_available_cash": kalshi_available_cash,
                "kalshi_portfolio_value": kalshi_portfolio_value,
                "fifo_vs_canonical_gap": fifo_realized_pnl - canonical['net_pnl_cents'],
                
                # === METADATA ===
                "data_sources": {
                    "account_truth": "balance_sim_canonical",
                    "attribution": "fifo_lot_engine",
                    "live_positions": "kalshi_api"
                },
                "accounting_system": "dual_balance_sim_fifo"
            }
        else:
            # Fallback to FIFO-only (old behavior)
            total_pnl = fifo_realized_pnl + unrealized_pnl
            account_summary = {
                "available_capital_cents": kalshi_available_cash,
                "portfolio_value_cents": kalshi_portfolio_value,
                "realized_pnl_cents": fifo_realized_pnl,
                "unrealized_pnl_cents": unrealized_pnl,
                "total_pnl_cents": total_pnl,
                "total_fees_cents": fifo_total_fees,
                "accounting_system": "fifo_fallback"
            }
        
        # Add position data and metadata
        account_summary.update({
            "open_cost_cents": open_cost_basis,
            "open_positions": open_positions,
            "position_details": position_details,
            "tickers_traded": len(self.engine.realized_pnl),
            "starting_capital_cents": self.starting_capital_cents,
            "last_updated": datetime.now(timezone.utc).isoformat()
        })
        
        return account_summary
    
    def update_trades_json(self, backup=True):
        """Update trades.json with dual accounting system."""
        
        # Backup existing trades.json
        if backup and os.path.exists(TRADES_FILE):
            backup_file = TRADES_FILE + ".backup" + BACKUP_SUFFIX
            with open(TRADES_FILE, 'r') as src, open(backup_file, 'w') as dst:
                dst.write(src.read())
            print(f"Backed up existing trades.json to {backup_file}")
        
        # Load existing trades data to preserve trade list
        existing_data = {}
        if os.path.exists(TRADES_FILE):
            try:
                with open(TRADES_FILE, 'r') as f:
                    existing_data = json.load(f)
            except:
                existing_data = {}
        
        # Get dual accounting portfolio summary
        portfolio = self.get_portfolio_summary()
        
        # Build trades.json with dual accounting
        trades_data = {
            "trades": existing_data.get("trades", []),  # Preserve existing trade list
            "summary": {
                "total_trades": len(existing_data.get("trades", [])),
                "open": len([t for t in existing_data.get("trades", []) if t.get("status") == "open"]),
                "resting": len([t for t in existing_data.get("trades", []) if t.get("status") == "resting"]), 
                "resting_contracts": sum(t.get("resting_count", 0) for t in existing_data.get("trades", [])),
                "settled": len([t for t in existing_data.get("trades", []) if t.get("status") in ["settled", "closed"]]),
                "won": len([t for t in existing_data.get("trades", []) if t.get("pnl_cents", 0) > 0]),
                "lost": len([t for t in existing_data.get("trades", []) if t.get("pnl_cents", 0) < 0]),
                
                # === CANONICAL ACCOUNT VALUES (from balance_sim) ===
                "available_capital_cents": portfolio["available_capital_cents"],
                "portfolio_value_cents": portfolio["portfolio_value_cents"],
                "pnl_cents": portfolio["total_pnl_cents"],
                "reconciliation_gap_cents": portfolio.get("reconciliation_gap_cents", 0),
                "reconciliation_status": portfolio.get("reconciliation_status", "UNKNOWN"),
                
                # === ATTRIBUTION VALUES (from FIFO) ===
                "fifo_realized_pnl_cents": portfolio.get("fifo_realized_pnl_cents", portfolio.get("realized_pnl_cents", 0)),
                "fifo_unrealized_pnl_cents": portfolio.get("fifo_unrealized_pnl_cents", portfolio.get("unrealized_pnl_cents", 0)),
                "fifo_total_fees_cents": portfolio.get("fifo_total_fees_cents", portfolio.get("total_fees_cents", 0)),
                
                # === LEGACY COMPATIBILITY ===
                "realized_pnl_cents": portfolio.get("fifo_realized_pnl_cents", 0),  # Keep for compatibility
                "unrealized_pnl_cents": portfolio.get("fifo_unrealized_pnl_cents", 0),  # Keep for compatibility
                "total_fees_cents": portfolio.get("fifo_total_fees_cents", 0),  # Keep for compatibility
                
                # === METADATA ===
                "mode": existing_data.get("summary", {}).get("mode", "LIVE"),
                "started_at": existing_data.get("summary", {}).get("started_at", "2026-02-19T00:28:00+00:00"),
                "starting_capital_cents": self.starting_capital_cents,
                "synced_from_kalshi": datetime.now(timezone.utc).isoformat(),
                "tickers_traded": portfolio["tickers_traded"],
                "open_cost_cents": portfolio["open_cost_cents"],
                
                # === DUAL ACCOUNTING FLAGS ===
                "balance_sim_canonical": True,
                "fifo_attribution": True,
                "accounting_system": portfolio.get("accounting_system", "dual"),
                "data_sources": portfolio.get("data_sources", {}),
                "mecnet_note": portfolio.get("mecnet_note", ""),
                
                # === LEGACY FLAGS ===
                "fifo_enabled": True,  # Keep for backward compatibility
                "ledger_state_file": ledger.STATE_FILE
            }
        }
        
        # Write updated trades.json
        with open(TRADES_FILE, 'w') as f:
            json.dump(trades_data, f, indent=2)
        
        print("✅ Updated trades.json with dual accounting system")
        
        # Show summary
        if "reconciliation_gap_cents" in portfolio:
            print(f"   Canonical Balance: ${portfolio['available_capital_cents']/100:.2f}")
            print(f"   Reconciliation Gap: {portfolio['reconciliation_gap_cents']:+d}¢ ({portfolio['reconciliation_status']})")
        print(f"   FIFO Realized P&L: {portfolio.get('fifo_realized_pnl_cents', 0):+d}¢")
        print(f"   FIFO Unrealized P&L: {portfolio.get('fifo_unrealized_pnl_cents', 0):+d}¢")
        print(f"   Open positions: {len(portfolio['open_positions'])} tickers")
        
        return trades_data
    
    def reconcile_accounting(self):
        """Full accounting reconciliation report using dual system."""
        print("🔍 DUAL ACCOUNTING RECONCILIATION")
        print("=" * 60)
        
        portfolio = self.get_portfolio_summary()
        
        # Show canonical truth
        if "canonical_balance_cents" in portfolio:
            print("=== CANONICAL ACCOUNT TRUTH (balance_sim) ===")
            print(f"Starting Capital: ${self.starting_capital_cents/100:.2f}")
            print(f"Canonical Balance: ${portfolio['canonical_balance_cents']/100:.2f}")
            print(f"Available Capital: ${portfolio['available_capital_cents']/100:.2f}")
            print(f"Total P&L: {portfolio['total_pnl_cents']:+d}¢ (${portfolio['total_pnl_cents']/100:+.2f})")
            print(f"Reconciliation Gap: {portfolio['reconciliation_gap_cents']:+d}¢ ({portfolio['reconciliation_status']})")
            print(f"Note: {portfolio['mecnet_note']}")
            print()
        
        # Show attribution breakdown
        print("=== ATTRIBUTION LAYER (FIFO) ===")
        print(f"FIFO Realized P&L: {portfolio.get('fifo_realized_pnl_cents', 0):+d}¢")
        print(f"FIFO Unrealized P&L: {portfolio.get('fifo_unrealized_pnl_cents', 0):+d}¢")
        print(f"FIFO Total Fees: {portfolio.get('fifo_total_fees_cents', 0)}¢")
        
        if "fifo_vs_canonical_gap" in portfolio:
            print(f"FIFO vs Canonical Gap: {portfolio['fifo_vs_canonical_gap']:+d}¢ (expected ~$483)")
        print()
        
        # Show positions
        print(f"=== OPEN POSITIONS ({len(portfolio['open_positions'])} tickers) ===")
        if portfolio['position_details']:
            for pos in portfolio['position_details']:
                print(f"  {pos['ticker']} {pos['side']} x{pos['qty']} | "
                      f"Cost: {pos['cost_basis_cents']}¢ | "
                      f"Value: {pos['current_value_cents']}¢ | "
                      f"P&L: {pos['unrealized_pnl_cents']:+d}¢")
        else:
            print("  None")
        
        # Status summary
        print()
        print("=== RECONCILIATION STATUS ===")
        if "reconciliation_status" in portfolio:
            status = portfolio['reconciliation_status']
            if status == "PASS":
                print("  ✅ Canonical accounting reconciles within tolerance")
            elif status == "PARTIAL":
                print("  ⚠️  Known MECNET residual within expected range")
            else:
                print("  ❌ Reconciliation failed - investigate")
        
        print(f"  Accounting System: {portfolio.get('accounting_system', 'unknown')}")
        print(f"  Data Sources: {portfolio.get('data_sources', {})}")
        
        return portfolio


def migrate_to_dual_accounting():
    """Migration utility to move to dual accounting system."""
    print("🔄 Migrating to dual accounting system (balance_sim + FIFO)...")
    
    pm = PortfolioManagerV2()
    
    # Sync any missing fills first
    print("\n1. Syncing fills from Kalshi...")
    realized_events = pm.sync_from_kalshi()
    
    # Show current state
    print("\n2. Current dual accounting state:")
    portfolio = pm.reconcile_accounting()
    
    # Update trades.json with dual accounting
    print("\n3. Updating trades.json...")
    pm.update_trades_json(backup=True)
    
    print("\n✅ Migration to dual accounting complete!")
    print("- balance_sim provides canonical account truth")
    print("- FIFO provides trade-level attribution")
    print("- ~$30.17 MECNET residual is expected and documented")
    
    return pm


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "migrate":
        migrate_to_dual_accounting()
    elif len(sys.argv) > 1 and sys.argv[1] == "sync":
        pm = PortfolioManagerV2()
        pm.sync_from_kalshi(dry_run="--dry" in sys.argv)
    elif len(sys.argv) > 1 and sys.argv[1] == "reconcile":
        pm = PortfolioManagerV2()
        pm.reconcile_accounting()
    else:
        print("Usage:")
        print("  python3 portfolio_manager_v2.py migrate     # Migrate to dual accounting")
        print("  python3 portfolio_manager_v2.py sync        # Sync fills from Kalshi") 
        print("  python3 portfolio_manager_v2.py sync --dry  # Dry run sync")
        print("  python3 portfolio_manager_v2.py reconcile   # Show accounting status")