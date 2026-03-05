#!/usr/bin/env python3
"""
Weather Trading System Forensic Audit
Part A: Analyze EXIT_BLOWN conditions and validate if positions were actually "dead"
"""

import json
import random
from datetime import datetime, timedelta
import sys
import os

# Add current directory to path so we can import kalshi_client
sys.path.append('/home/ubuntu/.openclaw/workspace/weather-trading')

def load_journal():
    """Load the trade journal"""
    with open('/home/ubuntu/.openclaw/workspace/weather-trading/trade_journal.json', 'r') as f:
        return json.load(f)

def find_exit_blown_entries(journal):
    """Find all EXIT_BLOWN entries in the journal"""
    exit_blown = []
    for i, entry in enumerate(journal):
        if entry.get('action') == 'EXIT_BLOWN':
            exit_blown.append((i, entry))
    return exit_blown

def find_entry_for_ticker(journal, ticker, before_index):
    """Find the ENTRY record for a given ticker before a specific index"""
    for i in range(before_index - 1, -1, -1):
        entry = journal[i]
        if entry.get('ticker') == ticker and entry.get('action') == 'ENTRY':
            return i, entry
    return None, None

def summarize_exit_blown_logic():
    """Summarize EXIT_BLOWN conditions from the code analysis"""
    return """
EXIT_BLOWN CONDITIONS (from position_manager.py lines 440-490):

EXIT_BLOWN is triggered when:
1. Position is for TODAY (not future dates)
2. We have real temperature data (effective_high is not None)  
3. The actual observed temperature has made our position impossible to win:

For NO positions:
- NO on bracket (B82.5): Blown if past peak hour AND actual temp landed IN the bracket
- NO on above (T68 greater): Blown if temp hit/exceeded the threshold (69°F)
- NO on below (T61 less): Blown if past peak AND max temp ≤ threshold (60°F)

For YES positions:  
- YES on above (T68 greater): Blown if past peak AND max temp too low to reach threshold
- YES on below (T60 less): Blown if temp already hit/exceeded the threshold (can't go back down)
- YES on bracket (B82.5): Blown if past peak AND temp missed the bracket entirely

This is physics-based - the position is "already dead" because temperature can't change 
in the required direction for us to win.
"""

def analyze_exit_blown_sample():
    """Analyze 10 random EXIT_BLOWN entries to see if they were actually 'dead'"""
    journal = load_journal()
    exit_blown_entries = find_exit_blown_entries(journal)
    
    print(f"Found {len(exit_blown_entries)} EXIT_BLOWN entries")
    
    # Take 10 random samples
    sample_size = min(10, len(exit_blown_entries))
    sample = random.sample(exit_blown_entries, sample_size)
    
    analysis = []
    
    for exit_index, exit_entry in sample:
        ticker = exit_entry.get('ticker')
        entry_index, entry_data = find_entry_for_ticker(journal, ticker, exit_index)
        
        analysis_item = {
            'exit_index': exit_index,
            'entry_index': entry_index,
            'ticker': ticker,
            'exit_data': exit_entry,
            'entry_data': entry_data,
            'analysis': None
        }
        
        if entry_data:
            # Extract key info
            direction = entry_data.get('direction')
            strike_str = ticker.split('-')[-1]  # e.g., "B82.5" or "T68"
            city = entry_data.get('city', 'Unknown')
            
            analysis_item['analysis'] = {
                'direction': direction,
                'strike_str': strike_str,
                'city': city,
                'exit_reason': exit_entry.get('reasoning', 'No reason given')
            }
        
        analysis.append(analysis_item)
    
    return analysis

if __name__ == "__main__":
    print("=== WEATHER TRADING FORENSIC AUDIT ===")
    print("Part A: EXIT_BLOWN Analysis")
    print("=" * 50)
    
    print(summarize_exit_blown_logic())
    print("\n" + "=" * 50)
    print("SAMPLE ANALYSIS OF EXIT_BLOWN ENTRIES:")
    print("=" * 50)
    
    analysis = analyze_exit_blown_sample()
    
    for i, item in enumerate(analysis, 1):
        print(f"\n{i}. Ticker: {item['ticker']}")
        if item['entry_data']:
            print(f"   Entry: {item['entry_data'].get('ts', 'Unknown time')}")
            print(f"   Direction: {item['analysis']['direction']}")
            print(f"   Strike: {item['analysis']['strike_str']}")
            print(f"   City: {item['analysis']['city']}")
        print(f"   Exit: {item['exit_data'].get('ts', 'Unknown time')}")
        print(f"   Exit reason: {item['analysis']['exit_reason'] if item['analysis'] else 'No entry found'}")
        
    print(f"\nNext: Need to check actual temperature data and settlement results to verify if these were truly 'dead' positions...")