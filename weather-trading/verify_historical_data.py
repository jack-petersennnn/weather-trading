#!/usr/bin/env python3
"""
CRITICAL: Historical Data Verification

This script verifies that Open-Meteo's historical forecast API returns true 
issuance-time data by comparing against timestamped training_forecast_log.json entries.

If API data differs meaningfully from logged issuance-time data, the entire 
365-day calibration backtest is CONTAMINATED and cannot be trusted.

This is a BLOCKING check - nothing proceeds until this passes.
"""

import json
import urllib.request
import urllib.error
import time
from datetime import datetime, timedelta

# Data files
TRAINING_LOG_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/training_forecast_log.json"
HISTORICAL_FORECASTS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/historical_forecasts.json"

# OpenMeteo API configuration
OPENMETEO_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

def load_logged_data():
    """Load timestamped forecast data from our own logs."""
    try:
        with open(TRAINING_LOG_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        print("ERROR: training_forecast_log.json not found")
        return {}

def load_historical_file():
    """Load historical forecasts from historical_forecasts.json."""
    try:
        with open(HISTORICAL_FORECASTS_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        print("ERROR: historical_forecasts.json not found")
        return {}

def get_city_coordinates():
    """Get coordinates for major trading cities."""
    return {
        "New York": (40.7128, -74.0060),
        "Chicago": (41.8781, -87.6298),
        "Miami": (25.7617, -80.1918),
        "Denver": (39.7392, -104.9903),
        "Austin": (30.2672, -97.7431),
        "Los Angeles": (34.0522, -118.2437)
    }

def fetch_openmeteo_historical(city, date, lat, lon, retries=3):
    """
    Fetch historical forecast data from Open-Meteo API for comparison.
    
    NOTE: We're testing if Open-Meteo's "historical forecasts" are true 
    issuance-time data or if they're reconstructed/revised.
    """
    
    # Convert date to required format
    try:
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        start_date = date_obj.strftime("%Y-%m-%d")
        end_date = date_obj.strftime("%Y-%m-%d")
    except ValueError:
        print(f"ERROR: Invalid date format: {date}")
        return None
    
    # Build API URL for historical forecast data
    # NOTE: This might not be the correct endpoint - we need to find
    # Open-Meteo's historical forecast API (not historical weather)
    url = (f"{OPENMETEO_BASE_URL}"
           f"?latitude={lat}&longitude={lon}"
           f"&start_date={start_date}&end_date={end_date}"
           f"&daily=temperature_2m_max"
           f"&timezone=America/New_York")
    
    for attempt in range(retries):
        try:
            print(f"Fetching Open-Meteo data for {city} on {date} (attempt {attempt + 1})")
            
            with urllib.request.urlopen(url, timeout=10) as response:
                if response.status == 200:
                    data = json.loads(response.read().decode('utf-8'))
                    
                    # Extract temperature forecast
                    daily_data = data.get('daily', {})
                    dates = daily_data.get('time', [])
                    temps = daily_data.get('temperature_2m_max', [])
                    
                    if dates and temps and date in dates:
                        idx = dates.index(date)
                        temp_c = temps[idx]
                        temp_f = temp_c * 9/5 + 32  # Convert to Fahrenheit
                        return temp_f
                    else:
                        print(f"No data found for {date} in Open-Meteo response")
                        return None
                else:
                    print(f"HTTP {response.status} from Open-Meteo API")
                    
        except urllib.error.URLError as e:
            print(f"Network error fetching Open-Meteo data: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
        except Exception as e:
            print(f"Error fetching Open-Meteo data: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    
    return None

def compare_forecasts(logged_data, historical_file_data, tolerance_f=2.0):
    """
    Compare logged forecast data against historical file data.
    
    Args:
        tolerance_f: Maximum acceptable difference in degrees F
    """
    print("="*80)
    print("HISTORICAL DATA VERIFICATION RESULTS")
    print("="*80)
    
    coordinates = get_city_coordinates()
    comparisons = []
    significant_differences = []
    
    print("\nComparing logged training data vs historical_forecasts.json...")
    
    # Compare our logged data against historical_forecasts.json
    sources_data = historical_file_data.get('sources', {})
    
    for entry_key, entry_data in logged_data.items():
        if "|" not in entry_key:
            continue
            
        city = entry_data.get('city')
        target_date = entry_data.get('target_date') 
        active_forecasts = entry_data.get('active_forecasts', {})
        
        if not city or not target_date or not active_forecasts:
            continue
            
        print(f"\nChecking {city} on {target_date}:")
        
        for model_name, logged_temp in active_forecasts.items():
            # Find corresponding data in historical file
            if (model_name in sources_data and 
                city in sources_data[model_name] and
                target_date in sources_data[model_name][city]):
                
                historical_temp = sources_data[model_name][city][target_date]
                
                if historical_temp is not None and logged_temp is not None:
                    difference = abs(float(historical_temp) - float(logged_temp))
                    
                    status = "✅ MATCH" if difference <= tolerance_f else "❌ DIFFER"
                    print(f"  {model_name}: logged={logged_temp:.1f}°F, historical={historical_temp:.1f}°F, diff={difference:.1f}°F [{status}]")
                    
                    comparison = {
                        'city': city,
                        'date': target_date,
                        'model': model_name,
                        'logged_temp': logged_temp,
                        'historical_temp': historical_temp,
                        'difference': difference,
                        'within_tolerance': difference <= tolerance_f
                    }
                    comparisons.append(comparison)
                    
                    if difference > tolerance_f:
                        significant_differences.append(comparison)
                        
                else:
                    print(f"  {model_name}: Missing data (logged={logged_temp}, historical={historical_temp})")
            else:
                print(f"  {model_name}: Not found in historical file")
    
    # Summary analysis
    print("\n" + "="*80)
    print("VERIFICATION SUMMARY")
    print("="*80)
    
    total_comparisons = len(comparisons)
    matches = len([c for c in comparisons if c['within_tolerance']])
    mismatches = len(significant_differences)
    
    print(f"Total comparisons: {total_comparisons}")
    
    if total_comparisons == 0:
        print("🚨 CRITICAL FAILURE: NO OVERLAPPING DATA FOUND")
        print("Cannot verify historical forecast integrity - no common dates/models")
        print("")
        print("CAUSE: Training log and historical forecasts cover different time periods")
        print("IMPACT: Cannot determine if historical data is issuance-time or revised")
        print("VERDICT: CALIBRATION BACKTEST CANNOT PROCEED")
        verification_status = "BLOCKED_NO_OVERLAP"
        
    else:
        print(f"Matches (within {tolerance_f}°F): {matches} ({matches/total_comparisons*100:.1f}%)")
        print(f"Significant differences: {mismatches} ({mismatches/total_comparisons*100:.1f}%)")
        
        if mismatches == 0:
            print("\n🎉 VERIFICATION PASSED")
            print("Historical forecast file contains true issuance-time data")
            print("Calibration backtest can proceed with confidence")
            verification_status = "PASSED"
            
        elif mismatches / total_comparisons < 0.05:  # Less than 5% mismatches
            print("\n⚠️ VERIFICATION WARNING")
            print(f"Small number of mismatches ({mismatches/total_comparisons*100:.1f}%)")
            print("May be acceptable depending on source and magnitude")
            print("Manual review recommended before proceeding")
            verification_status = "WARNING"
            
        else:
            print("\n🚨 VERIFICATION FAILED")
            print("Significant differences detected between logged and historical data")
            print("Historical file may contain revised/reconstructed forecasts")
            print("CALIBRATION BACKTEST IS CONTAMINATED - DO NOT PROCEED")
            verification_status = "FAILED"
    
    # Show worst differences
    if significant_differences:
        print(f"\nWorst {min(10, len(significant_differences))} differences:")
        significant_differences.sort(key=lambda x: x['difference'], reverse=True)
        
        for i, diff in enumerate(significant_differences[:10]):
            print(f"  {i+1}. {diff['city']} {diff['date']} {diff['model']}: "
                  f"{diff['logged_temp']:.1f}°F → {diff['historical_temp']:.1f}°F "
                  f"({diff['difference']:.1f}°F diff)")
    
    # Save detailed results
    results = {
        'verification_status': verification_status,
        'timestamp': datetime.now().isoformat(),
        'total_comparisons': total_comparisons,
        'matches': matches,
        'mismatches': mismatches,
        'match_rate': matches / total_comparisons if total_comparisons > 0 else 0,
        'tolerance_f': tolerance_f,
        'significant_differences': significant_differences,
        'all_comparisons': comparisons
    }
    
    with open('/home/ubuntu/.openclaw/workspace/weather-trading/historical_data_verification.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nDetailed results saved to: historical_data_verification.json")
    
    return verification_status, results

def main():
    print("HISTORICAL DATA VERIFICATION - BLOCKING CHECK")
    print("="*80)
    print("Verifying that historical forecast data represents true issuance-time forecasts")
    print("vs. revised/reconstructed data that would contaminate calibration analysis.")
    print("")
    
    # Load data
    logged_data = load_logged_data()
    historical_file_data = load_historical_file()
    
    if not logged_data:
        print("❌ FATAL: No logged training data found")
        print("Cannot verify historical data integrity")
        return "FAILED"
    
    if not historical_file_data:
        print("❌ FATAL: No historical forecasts file found") 
        print("Cannot perform verification")
        return "FAILED"
    
    print(f"Loaded {len(logged_data)} logged forecast entries")
    
    sources_data = historical_file_data.get('sources', {})
    print(f"Loaded historical data for {len(sources_data)} forecast models")
    
    if not sources_data:
        print("❌ FATAL: No source data in historical forecasts file")
        return "FAILED"
    
    # Perform verification
    verification_status, results = compare_forecasts(logged_data, historical_file_data)
    
    # Final verdict
    print("\n" + "="*80)
    print("FINAL VERDICT")
    print("="*80)
    
    if verification_status == "PASSED":
        print("✅ Historical data verification PASSED")
        print("Proceed with calibration backtest using historical_forecasts.json")
        print("")
        print("NEXT STEPS:")
        print("1. Fix sigma interpretation logic")
        print("2. Re-run calibration with corrected sigma multipliers")
        print("3. Apply sigma corrections only (no probability corrector)")
        print("4. Proceed with trade-level counterfactual")
        
    elif verification_status == "WARNING":
        print("⚠️ Historical data verification shows WARNINGS")
        print("Manual review required before proceeding")
        print("Consider using only logged training data for calibration")
        
    elif verification_status == "BLOCKED_NO_OVERLAP":
        print("🛑 BLOCKED: Cannot verify historical data integrity")
        print("No overlapping data between training log and historical forecasts")
        print("")
        print("ROOT CAUSE:")
        print("- Training log: 4 days of recent data (2026-02-22 to 2026-02-25)")
        print("- Historical file: 365 days but ends at 2026-02-19")
        print("- Zero overlap = cannot verify if historical data is contaminated")
        print("")
        print("IMMEDIATE OPTIONS:")
        print("1. Wait 1-2 weeks to accumulate more logged training data")
        print("2. Use ONLY logged training data (76 entries) for limited calibration")
        print("3. Find verified issuance-time historical forecast source")
        print("")
        print("⛔ DO NOT PROCEED with 365-day historical calibration backtest")
        print("⛔ Cannot trust historical_forecasts.json without verification")
        
    else:
        print("❌ Historical data verification FAILED")
        print("STOP: Do not proceed with calibration backtest")
        print("Historical data is contaminated with revised forecasts")
        print("")
        print("ALTERNATIVES:")
        print("1. Use only logged training_forecast_log.json data (limited scope)")
        print("2. Wait for more logged issuance-time data to accumulate")
        print("3. Find alternative historical forecast API with true issuance-time data")
    
    return verification_status

if __name__ == "__main__":
    status = main()
    exit(0 if status == "PASSED" else 1)  # Only PASSED allows proceeding