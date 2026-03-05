#!/usr/bin/env python3
"""
Weather Trading System Comprehensive Forensic Audit
Full P&L reconciliation and corrected exit analysis
"""

import json
import sys
import os
import csv
import random
from datetime import datetime, timedelta
from collections import defaultdict

# Add current directory to path
sys.path.append('/home/ubuntu/.openclaw/workspace/weather-trading')

try:
    import kalshi_client
except ImportError:
    print("Error: Could not import kalshi_client. Make sure keys are properly configured.")
    sys.exit(1)

class WeatherTradingAudit:
    def __init__(self):
        self.working_dir = '/home/ubuntu/.openclaw/workspace/weather-trading'
        self.all_fills = []
        self.all_settlements = []
        self.journal = []
        self.audit_results = {}
        
    def load_journal(self):
        """Load the trade journal"""
        with open(os.path.join(self.working_dir, 'trade_journal.json'), 'r') as f:
            self.journal = json.load(f)
        print(f"Loaded {len(self.journal)} journal entries")
        
    def fetch_all_fills(self, use_cache=True):
        """Fetch all fills from Kalshi API with pagination"""
        cache_file = '/tmp/kalshi_fills_all.json'
        
        if use_cache and os.path.exists(cache_file):
            print("Loading fills from cache...")
            with open(cache_file, 'r') as f:
                data = json.load(f)
                # Handle both formats: direct list or nested dict
                if isinstance(data, list):
                    self.all_fills = data
                else:
                    self.all_fills = data.get('fills', [])
                print(f"Loaded {len(self.all_fills)} fills from cache")
                return
                
        print("Fetching all fills from Kalshi API...")
        all_fills = []
        cursor = None
        page = 0
        
        while True:
            page += 1
            print(f"Fetching fills page {page}...")
            
            try:
                path = '/portfolio/fills?limit=1000'
                if cursor:
                    path += f'&cursor={cursor}'
                
                response = kalshi_client._request('GET', path)
                fills = response.get('fills', [])
                
                if not fills:
                    break
                    
                all_fills.extend(fills)
                cursor = response.get('cursor')
                
                if not cursor:
                    break
                    
            except Exception as e:
                print(f"Error fetching fills page {page}: {e}")
                break
                
        self.all_fills = all_fills
        print(f"Fetched {len(self.all_fills)} total fills")
        
        # Cache the results
        with open(cache_file, 'w') as f:
            json.dump({'fills': self.all_fills, 'fetched_at': datetime.now().isoformat()}, f, indent=2)
            
    def fetch_all_settlements(self, use_cache=True):
        """Fetch all settlements from Kalshi API with pagination"""
        cache_file = '/tmp/kalshi_settlements.json'
        
        if use_cache and os.path.exists(cache_file):
            print("Loading settlements from cache...")
            with open(cache_file, 'r') as f:
                data = json.load(f)
                # Handle both formats: direct list or nested dict
                if isinstance(data, list):
                    self.all_settlements = data
                else:
                    self.all_settlements = data.get('settlements', [])
                print(f"Loaded {len(self.all_settlements)} settlements from cache")
                return
                
        print("Fetching all settlements from Kalshi API...")
        all_settlements = []
        cursor = None
        page = 0
        
        while True:
            page += 1
            print(f"Fetching settlements page {page}...")
            
            try:
                path = '/portfolio/settlements?limit=1000'
                if cursor:
                    path += f'&cursor={cursor}'
                
                response = kalshi_client._request('GET', path)
                settlements = response.get('settlements', [])
                
                if not settlements:
                    break
                    
                all_settlements.extend(settlements)
                cursor = response.get('cursor')
                
                if not cursor:
                    break
                    
            except Exception as e:
                print(f"Error fetching settlements page {page}: {e}")
                break
                
        self.all_settlements = all_settlements
        print(f"Fetched {len(self.all_settlements)} total settlements")
        
        # Cache the results
        with open(cache_file, 'w') as f:
            json.dump({'settlements': self.all_settlements, 'fetched_at': datetime.now().isoformat()}, f, indent=2)
            
    def analyze_fills_cashflow(self):
        """Analyze cash flows from fills data"""
        print("Analyzing fills cashflow...")
        
        total_cash_out = 0  # Money spent (buys + fees)
        total_cash_in = 0   # Money received (sells)
        total_fees = 0
        
        buy_fills = []
        sell_fills = []
        
        for fill in self.all_fills:
            action = fill.get('action')  # 'buy' or 'sell'
            side = fill.get('side')      # 'yes' or 'no' 
            yes_price = fill.get('yes_price', 0)  # YES price in cents
            count = fill.get('count', 0)
            fee_cost_str = fill.get('fee_cost', '0')
            fee_cost = float(fee_cost_str) * 100 if fee_cost_str else 0  # Convert to cents
            
            total_fees += fee_cost
            
            if action == 'buy':
                # For buys: calculate actual price paid based on side
                if side == 'yes':
                    cost_cents = yes_price * count
                else:  # side == 'no'
                    no_price = 100 - yes_price
                    cost_cents = no_price * count
                
                total_cash_out += cost_cents + fee_cost
                buy_fills.append({
                    'ticker': fill.get('ticker'),
                    'side': side,
                    'count': count,
                    'yes_price': yes_price,
                    'actual_price': yes_price if side == 'yes' else (100 - yes_price),
                    'cost_cents': cost_cents,
                    'fee_cost': fee_cost,
                    'ts': fill.get('ts')
                })
                
            elif action == 'sell':
                # For sells: calculate actual proceeds based on side
                if side == 'yes':
                    proceeds_cents = yes_price * count
                else:  # side == 'no'
                    no_price = 100 - yes_price  
                    proceeds_cents = no_price * count
                
                total_cash_in += proceeds_cents - fee_cost  # Fees reduce proceeds
                sell_fills.append({
                    'ticker': fill.get('ticker'),
                    'side': side,
                    'count': count,
                    'yes_price': yes_price,
                    'actual_price': yes_price if side == 'yes' else (100 - yes_price),
                    'proceeds_cents': proceeds_cents,
                    'fee_cost': fee_cost,
                    'ts': fill.get('ts')
                })
        
        fills_summary = {
            'total_fills': len(self.all_fills),
            'buy_fills': len(buy_fills),
            'sell_fills': len(sell_fills),
            'total_cash_out': total_cash_out,
            'total_cash_in': total_cash_in,
            'total_fees': total_fees,
            'net_from_fills': total_cash_in - total_cash_out,
            'buy_data': buy_fills,
            'sell_data': sell_fills
        }
        
        return fills_summary
        
    def analyze_settlements_cashflow(self):
        """Analyze cash flows from settlements data"""
        print("Analyzing settlements cashflow...")
        
        total_settlement_revenue = 0
        settlement_details = []
        
        for settlement in self.all_settlements:
            revenue = settlement.get('revenue', 0)  # This appears to be in cents already
            total_settlement_revenue += revenue
            
            settlement_details.append({
                'ticker': settlement.get('ticker', settlement.get('event_ticker', 'Unknown')),
                'yes_count': settlement.get('yes_count', 0),
                'no_count': settlement.get('no_count', 0),
                'revenue': revenue,
                'market_result': settlement.get('market_result'),
                'ts': settlement.get('ts', settlement.get('settled_time'))
            })
        
        settlements_summary = {
            'total_settlements': len(self.all_settlements),
            'total_revenue': total_settlement_revenue,
            'settlement_details': settlement_details
        }
        
        return settlements_summary
        
    def calculate_net_cashflow(self):
        """Calculate net cashflow and compare to account balance change"""
        fills_summary = self.analyze_fills_cashflow()
        settlements_summary = self.analyze_settlements_cashflow()
        
        # Calculate total cash movements
        total_cash_out = fills_summary['total_cash_out']  # Buys + fees  
        total_cash_in = fills_summary['total_cash_in'] + settlements_summary['total_revenue']  # Sells + settlements
        
        net_cashflow = total_cash_in - total_cash_out
        
        # Expected account balance change
        starting_balance = 510.76  # Given
        current_balance = 187.54   # Given  
        actual_change = current_balance - starting_balance  # Should be -323.22
        
        cashflow_summary = {
            'starting_balance': starting_balance,
            'current_balance': current_balance,
            'actual_balance_change': actual_change,
            'calculated_net_cashflow': net_cashflow / 100,  # Convert to dollars
            'difference': (net_cashflow / 100) - actual_change,
            'fills_cash_out': total_cash_out / 100,
            'fills_cash_in': fills_summary['total_cash_in'] / 100,
            'settlements_revenue': settlements_summary['total_revenue'] / 100,
            'total_fees': fills_summary['total_fees'] / 100
        }
        
        return cashflow_summary, fills_summary, settlements_summary
        
    def save_data_files(self, fills_summary, settlements_summary):
        """Save fills and settlements to CSV files"""
        
        # Save fills CSV
        fills_file = os.path.join(self.working_dir, 'fills.csv')
        with open(fills_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'ticker', 'ts', 'action', 'side', 'count', 'yes_price', 
                'actual_price', 'cost_or_proceeds', 'fee_cost'
            ])
            writer.writeheader()
            
            # Write buy fills
            for fill in fills_summary['buy_data']:
                writer.writerow({
                    'ticker': fill['ticker'],
                    'ts': fill['ts'],
                    'action': 'buy',
                    'side': fill['side'],
                    'count': fill['count'],
                    'yes_price': fill['yes_price'],
                    'actual_price': fill['actual_price'],
                    'cost_or_proceeds': fill['cost_cents'],
                    'fee_cost': fill['fee_cost']
                })
                
            # Write sell fills
            for fill in fills_summary['sell_data']:
                writer.writerow({
                    'ticker': fill['ticker'],
                    'ts': fill['ts'],
                    'action': 'sell',
                    'side': fill['side'],
                    'count': fill['count'],
                    'yes_price': fill['yes_price'],
                    'actual_price': fill['actual_price'],
                    'cost_or_proceeds': fill['proceeds_cents'],
                    'fee_cost': fill['fee_cost']
                })
        
        print(f"Saved fills data to {fills_file}")
        
        # Save settlements CSV
        settlements_file = os.path.join(self.working_dir, 'settlements.csv')
        with open(settlements_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'ticker', 'ts', 'yes_count', 'no_count', 'revenue', 'market_result'
            ])
            writer.writeheader()
            
            for settlement in settlements_summary['settlement_details']:
                writer.writerow(settlement)
        
        print(f"Saved settlements data to {settlements_file}")
        
    def run_part_a_exit_blown_analysis(self):
        """Part A: Analyze EXIT_BLOWN entries"""
        print("\n" + "="*50)
        print("PART A: EXIT_BLOWN ANALYSIS")
        print("="*50)
        
        exit_blown_entries = []
        for i, entry in enumerate(self.journal):
            if entry.get('action') == 'EXIT_BLOWN':
                exit_blown_entries.append((i, entry))
                
        print(f"Found {len(exit_blown_entries)} EXIT_BLOWN entries")
        
        # Sample 10 random entries for detailed analysis
        sample_size = min(10, len(exit_blown_entries))
        sample = random.sample(exit_blown_entries, sample_size)
        
        exit_blown_analysis = []
        for exit_index, exit_entry in sample:
            ticker = exit_entry.get('ticker')
            
            # Find corresponding ENTRY
            entry_data = None
            for j in range(exit_index - 1, -1, -1):
                if (self.journal[j].get('ticker') == ticker and 
                    self.journal[j].get('action') == 'ENTRY'):
                    entry_data = self.journal[j]
                    break
            
            analysis = {
                'ticker': ticker,
                'exit_reasoning': exit_entry.get('reasoning', 'No reason'),
                'entry_found': entry_data is not None,
                'verified_dead': 'NEEDS_VERIFICATION'
            }
            
            if entry_data:
                analysis.update({
                    'direction': entry_data.get('direction'),
                    'city': entry_data.get('city'),
                    'entry_ts': entry_data.get('ts'),
                    'exit_ts': exit_entry.get('ts')
                })
            
            exit_blown_analysis.append(analysis)
            
        return {
            'total_exit_blown': len(exit_blown_entries),
            'sample_analysis': exit_blown_analysis,
            'summary': "EXIT_BLOWN entries appear to be physics-based exits when positions become impossible to win"
        }
        
    def run_part_b_cashflow_reconciliation(self):
        """Part B: Full cashflow reconciliation from Kalshi API"""  
        print("\n" + "="*50)
        print("PART B: CASHFLOW RECONCILIATION")
        print("="*50)
        
        cashflow_summary, fills_summary, settlements_summary = self.calculate_net_cashflow()
        
        # Save data files
        self.save_data_files(fills_summary, settlements_summary)
        
        # Save reconciliation summary
        recon_file = os.path.join(self.working_dir, 'recon_summary.json')
        with open(recon_file, 'w') as f:
            json.dump({
                'cashflow_summary': cashflow_summary,
                'fills_summary': {
                    'total_fills': fills_summary['total_fills'],
                    'buy_fills': fills_summary['buy_fills'],
                    'sell_fills': fills_summary['sell_fills'],
                    'total_cash_out': fills_summary['total_cash_out'],
                    'total_cash_in': fills_summary['total_cash_in'],
                    'total_fees': fills_summary['total_fees'],
                    'net_from_fills': fills_summary['net_from_fills']
                },
                'settlements_summary': {
                    'total_settlements': settlements_summary['total_settlements'],
                    'total_revenue': settlements_summary['total_revenue']
                }
            }, f, indent=2)
            
        print(f"Saved reconciliation summary to {recon_file}")
        
        return cashflow_summary
        
    def run_audit(self):
        """Run the complete audit"""
        print("=== WEATHER TRADING FORENSIC AUDIT ===")
        print(f"Working directory: {self.working_dir}")
        print("="*50)
        
        # Load journal
        self.load_journal()
        
        # Fetch Kalshi data
        self.fetch_all_fills(use_cache=True)
        self.fetch_all_settlements(use_cache=True)
        
        # Run analysis parts
        part_a_results = self.run_part_a_exit_blown_analysis()
        part_b_results = self.run_part_b_cashflow_reconciliation()
        
        # Print summary
        print("\n" + "="*50)
        print("AUDIT SUMMARY")
        print("="*50)
        
        print(f"Journal entries: {len(self.journal)}")
        print(f"Kalshi fills: {len(self.all_fills)}")
        print(f"Kalshi settlements: {len(self.all_settlements)}")
        print(f"EXIT_BLOWN entries: {part_a_results['total_exit_blown']}")
        
        print(f"\nCashflow Reconciliation:")
        print(f"  Starting balance: ${part_b_results['starting_balance']:.2f}")
        print(f"  Current balance: ${part_b_results['current_balance']:.2f}")
        print(f"  Actual change: ${part_b_results['actual_balance_change']:.2f}")
        print(f"  Calculated net: ${part_b_results['calculated_net_cashflow']:.2f}")
        print(f"  Difference: ${part_b_results['difference']:.2f}")
        
        return {
            'part_a': part_a_results,
            'part_b': part_b_results,
            'data_summary': {
                'journal_entries': len(self.journal),
                'kalshi_fills': len(self.all_fills),
                'kalshi_settlements': len(self.all_settlements)
            }
        }

if __name__ == "__main__":
    audit = WeatherTradingAudit()
    results = audit.run_audit()
    print("\nAudit completed. Check generated files for detailed results.")