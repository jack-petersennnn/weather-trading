#!/usr/bin/env python3
"""
Historical Data Sanity Check

Pick 3-4 dates from July-August 2024 where there were notable weather events.
Compare forecasts from historical data against ACIS actuals to verify data integrity.
If historical forecasts show the same errors that actually occurred (not suspiciously accurate),
the data is genuine.
"""

import json
import statistics
from datetime import datetime

# Data files
ACTUALS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/acis_actuals_365d.json"
HISTORICAL_FORECASTS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/historical_forecasts.json"

# Notable dates from the available 2025-2026 data range for testing
TEST_DATES = [
    {
        "date": "2025-07-15", 
        "description": "Mid-summer heat conditions",
        "expected_cities": ["Chicago", "Denver", "Austin"],
        "expected_pattern": "Forecasts should show high temps, some uncertainty"
    },
    {
        "date": "2025-08-05",
        "description": "Late summer weather patterns", 
        "expected_cities": ["Miami", "New York"],
        "expected_pattern": "Seasonal transition, model spread expected"
    },
    {
        "date": "2025-12-15",
        "description": "Winter weather patterns", 
        "expected_cities": ["Chicago", "New York", "Denver"],
        "expected_pattern": "Cold weather, potential storm systems"
    },
    {
        "date": "2026-01-20",
        "description": "Mid-winter conditions",
        "expected_cities": ["Austin", "Miami"],
        "expected_pattern": "Mild winter temps in south, model agreement expected"
    }
]

def load_test_data():
    """Load historical forecasts and actuals for sanity checking."""
    print("Loading data for sanity check...")
    
    with open(ACTUALS_FILE) as f:
        actuals_data = json.load(f)
    
    with open(HISTORICAL_FORECASTS_FILE) as f:
        historical_forecasts = json.load(f)
    
    return actuals_data, historical_forecasts

def extract_date_data(date, city, actuals_data, historical_forecasts):
    """Extract all available data for a specific date and city."""
    result = {
        'date': date,
        'city': city,
        'actual_temp': None,
        'forecasts': {},
        'forecast_stats': {}
    }
    
    # Get actual temperature
    if city in actuals_data["cities"] and date in actuals_data["cities"][city]["data"]:
        result['actual_temp'] = actuals_data["cities"][city]["data"][date]
    
    # Get forecasts from all models
    sources_data = historical_forecasts.get('sources', {})
    
    for model_name, model_data in sources_data.items():
        if city in model_data and date in model_data[city]:
            temp = model_data[city][date]
            if temp is not None:
                result['forecasts'][model_name] = temp
    
    # Compute forecast statistics
    if result['forecasts']:
        temps = list(result['forecasts'].values())
        result['forecast_stats'] = {
            'mean': statistics.mean(temps),
            'median': statistics.median(temps),
            'min': min(temps),
            'max': max(temps),
            'std': statistics.pstdev(temps) if len(temps) > 1 else 0,
            'range': max(temps) - min(temps),
            'model_count': len(temps)
        }
        
        # Forecast errors (if actual is available)
        if result['actual_temp'] is not None:
            actual = result['actual_temp']
            result['forecast_stats']['mean_error'] = result['forecast_stats']['mean'] - actual
            result['forecast_stats']['median_error'] = result['forecast_stats']['median'] - actual
            result['forecast_stats']['abs_mean_error'] = abs(result['forecast_stats']['mean_error'])
            
            # Individual model errors
            model_errors = {}
            for model, forecast in result['forecasts'].items():
                model_errors[model] = forecast - actual
            result['model_errors'] = model_errors
    
    return result

def assess_data_quality(date_data):
    """Assess whether the historical data looks genuine for this date."""
    city = date_data['city']
    date = date_data['date']
    
    assessment = {
        'date': date,
        'city': city,
        'has_actual': date_data['actual_temp'] is not None,
        'has_forecasts': len(date_data['forecasts']) > 0,
        'model_count': date_data['forecast_stats'].get('model_count', 0),
        'quality_flags': [],
        'suspicious_flags': [],
        'overall_quality': 'unknown'
    }
    
    if not assessment['has_actual'] or not assessment['has_forecasts']:
        assessment['quality_flags'].append("Missing data")
        assessment['overall_quality'] = 'insufficient_data'
        return assessment
    
    stats = date_data['forecast_stats']
    actual = date_data['actual_temp']
    
    # Quality indicators (good signs)
    
    # 1. Reasonable model spread (indicates genuine uncertainty)
    if stats['std'] > 1.0:
        assessment['quality_flags'].append(f"Good model spread ({stats['std']:.1f}°F)")
    elif stats['std'] < 0.5:
        assessment['suspicious_flags'].append(f"Unusually low model spread ({stats['std']:.1f}°F)")
    
    # 2. Forecast errors are reasonable (not suspiciously accurate)
    if 'abs_mean_error' in stats:
        if 0.5 <= stats['abs_mean_error'] <= 5.0:
            assessment['quality_flags'].append(f"Reasonable forecast error ({stats['abs_mean_error']:.1f}°F)")
        elif stats['abs_mean_error'] < 0.5:
            assessment['suspicious_flags'].append(f"Suspiciously accurate ({stats['abs_mean_error']:.1f}°F error)")
        elif stats['abs_mean_error'] > 8.0:
            assessment['suspicious_flags'].append(f"Very large forecast error ({stats['abs_mean_error']:.1f}°F)")
    
    # 3. Multiple models available
    if stats['model_count'] >= 8:
        assessment['quality_flags'].append(f"Good model coverage ({stats['model_count']} models)")
    elif stats['model_count'] < 5:
        assessment['quality_flags'].append(f"Limited model coverage ({stats['model_count']} models)")
    
    # 4. Individual model errors show variation (not identical)
    if 'model_errors' in date_data:
        error_values = list(date_data['model_errors'].values())
        error_std = statistics.pstdev(error_values) if len(error_values) > 1 else 0
        
        if error_std > 0.5:
            assessment['quality_flags'].append(f"Good model error diversity ({error_std:.1f}°F)")
        elif error_std < 0.1:
            assessment['suspicious_flags'].append(f"Models too similar ({error_std:.1f}°F error std)")
    
    # 5. Temperature values are reasonable
    if -20 <= actual <= 120:
        assessment['quality_flags'].append("Reasonable actual temperature")
    else:
        assessment['suspicious_flags'].append(f"Extreme actual temperature ({actual}°F)")
    
    # Overall assessment
    if len(assessment['suspicious_flags']) == 0:
        if len(assessment['quality_flags']) >= 3:
            assessment['overall_quality'] = 'high'
        else:
            assessment['overall_quality'] = 'medium'
    elif len(assessment['suspicious_flags']) == 1 and len(assessment['quality_flags']) >= 2:
        assessment['overall_quality'] = 'medium_with_concerns'
    else:
        assessment['overall_quality'] = 'suspicious'
    
    return assessment

def detailed_model_analysis(date_data):
    """Provide detailed analysis of individual model performance."""
    if not date_data['forecasts'] or date_data['actual_temp'] is None:
        return None
    
    actual = date_data['actual_temp']
    analysis = {
        'actual_temp': actual,
        'model_performance': [],
        'best_model': None,
        'worst_model': None,
        'ensemble_performance': date_data['forecast_stats']
    }
    
    # Analyze each model
    for model, forecast in date_data['forecasts'].items():
        error = forecast - actual
        abs_error = abs(error)
        
        analysis['model_performance'].append({
            'model': model,
            'forecast': forecast,
            'error': error,
            'abs_error': abs_error,
            'bias_direction': 'warm' if error > 0 else 'cool' if error < 0 else 'perfect'
        })
    
    # Sort by absolute error
    analysis['model_performance'].sort(key=lambda x: x['abs_error'])
    
    if analysis['model_performance']:
        analysis['best_model'] = analysis['model_performance'][0]['model']
        analysis['worst_model'] = analysis['model_performance'][-1]['model']
    
    return analysis

def run_sanity_check():
    """Run the complete sanity check on selected dates."""
    print("HISTORICAL DATA SANITY CHECK")
    print("="*80)
    print("Testing data integrity on notable weather dates from 2025-2026 data range")
    
    actuals_data, historical_forecasts = load_test_data()
    
    all_results = []
    
    for test_case in TEST_DATES:
        date = test_case["date"]
        description = test_case["description"]
        expected_cities = test_case["expected_cities"]
        
        print(f"\n📅 {date}: {description}")
        print("-" * 60)
        
        test_results = {
            'date': date,
            'description': description,
            'city_results': []
        }
        
        # Test each relevant city for this date
        for city in expected_cities:
            print(f"\n🏙️  Testing {city}:")
            
            # Extract data for this city/date
            date_data = extract_date_data(date, city, actuals_data, historical_forecasts)
            
            # Assess data quality
            quality = assess_data_quality(date_data)
            
            # Detailed model analysis
            model_analysis = detailed_model_analysis(date_data)
            
            # Print results
            if quality['has_actual'] and quality['has_forecasts']:
                actual = date_data['actual_temp']
                mean_forecast = date_data['forecast_stats']['mean']
                mean_error = date_data['forecast_stats']['mean_error']
                model_count = date_data['forecast_stats']['model_count']
                model_spread = date_data['forecast_stats']['std']
                
                print(f"  Actual: {actual}°F")
                print(f"  Forecast mean: {mean_forecast:.1f}°F (error: {mean_error:+.1f}°F)")
                print(f"  Model spread: {model_spread:.1f}°F ({model_count} models)")
                print(f"  Quality: {quality['overall_quality'].upper()}")
                
                if quality['quality_flags']:
                    for flag in quality['quality_flags']:
                        print(f"  ✅ {flag}")
                
                if quality['suspicious_flags']:
                    for flag in quality['suspicious_flags']:
                        print(f"  ⚠️  {flag}")
                
                # Show best and worst models
                if model_analysis:
                    best = model_analysis['model_performance'][0]
                    worst = model_analysis['model_performance'][-1]
                    print(f"  📊 Best: {best['model']} ({best['error']:+.1f}°F)")
                    print(f"  📊 Worst: {worst['model']} ({worst['error']:+.1f}°F)")
                
            else:
                print(f"  ❌ Insufficient data (actual: {quality['has_actual']}, forecasts: {quality['has_forecasts']})")
            
            # Store results
            city_result = {
                'city': city,
                'date_data': date_data,
                'quality_assessment': quality,
                'model_analysis': model_analysis
            }
            test_results['city_results'].append(city_result)
        
        all_results.append(test_results)
    
    return all_results

def generate_sanity_check_report(results):
    """Generate final report on data integrity."""
    print("\n" + "="*80)
    print("HISTORICAL DATA INTEGRITY REPORT")
    print("="*80)
    
    total_tests = 0
    high_quality = 0
    medium_quality = 0
    suspicious = 0
    insufficient_data = 0
    
    # Count quality levels
    for test_result in results:
        for city_result in test_result['city_results']:
            quality = city_result['quality_assessment']['overall_quality']
            total_tests += 1
            
            if quality == 'high':
                high_quality += 1
            elif quality in ['medium', 'medium_with_concerns']:
                medium_quality += 1
            elif quality == 'suspicious':
                suspicious += 1
            elif quality == 'insufficient_data':
                insufficient_data += 1
    
    print(f"\nOverall Results ({total_tests} tests):")
    print(f"  ✅ High Quality: {high_quality} ({high_quality/total_tests*100:.0f}%)")
    print(f"  🟡 Medium Quality: {medium_quality} ({medium_quality/total_tests*100:.0f}%)")
    print(f"  ⚠️  Suspicious: {suspicious} ({suspicious/total_tests*100:.0f}%)")
    print(f"  ❌ Insufficient Data: {insufficient_data} ({insufficient_data/total_tests*100:.0f}%)")
    
    # Overall assessment
    print("\nData Integrity Assessment:")
    if suspicious == 0 and insufficient_data <= 1:
        print("✅ PASS: Historical data appears genuine and reliable")
        print("✅ Forecast errors and model spreads look realistic")
        print("✅ Safe to use for calibration analysis")
    elif suspicious <= 1 and insufficient_data <= 2:
        print("🟡 CAUTION: Data mostly good but some concerns")
        print("🟡 Review suspicious cases before using for critical decisions")
    else:
        print("❌ FAIL: Significant data quality concerns")
        print("❌ Do not use this data for calibration without investigation")
    
    # Specific recommendations
    print("\nRecommendations:")
    if high_quality + medium_quality >= total_tests * 0.8:
        print("• Historical data is suitable for sigma optimization")
        print("• Forecast errors show realistic patterns")
        print("• Model disagreement indicates genuine uncertainty")
    else:
        print("• ⚠️  Review data collection methodology")
        print("• ⚠️  Consider additional data sources for validation")
        print("• ⚠️  Use calibration results with caution")
    
    print(f"\nTest focused on representative dates from 2025-2026 data range") 
    print(f"This sample represents data quality for calibration analysis")

def main():
    # Run the sanity check
    results = run_sanity_check()
    
    # Generate report
    generate_sanity_check_report(results)
    
    # Save detailed results
    output_file = "/home/ubuntu/.openclaw/workspace/weather-trading/historical_data_sanity_check_results.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n📊 Detailed results saved to: {output_file}")

if __name__ == "__main__":
    main()