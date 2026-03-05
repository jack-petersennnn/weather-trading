#!/usr/bin/env python3
"""
Sigma Multiplier Optimization for Weather Trading Bot

This script optimizes sigma scaling factors (k where sigma_adj = k * raw_sigma) 
by minimizing Brier score using walk-forward validation on 365 days of historical data.
"""

import json
import math
import statistics
from collections import defaultdict
from datetime import datetime
import os

# Import from analyzer.py for ensemble computation
import sys
sys.path.append('/home/ubuntu/.openclaw/workspace/weather-trading')
from analyzer import weighted_ensemble_stats, compute_probability

# Data files
BACKTEST_RESULTS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/backtest_full_results.json"
ACTUALS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/acis_actuals_365d.json" 
SOURCE_WEIGHTS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/source_weights.json"
HISTORICAL_FORECASTS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/historical_forecasts.json"

def load_optimization_data():
    """Load all data needed for optimization."""
    print("Loading optimization data...")
    
    with open(BACKTEST_RESULTS_FILE) as f:
        backtest_results = json.load(f)
    
    with open(ACTUALS_FILE) as f:
        actuals_data = json.load(f)
    
    with open(SOURCE_WEIGHTS_FILE) as f:
        source_weights = json.load(f)["weights"]
    
    with open(HISTORICAL_FORECASTS_FILE) as f:
        historical_forecasts = json.load(f)
    
    return backtest_results, actuals_data, source_weights, historical_forecasts

def prepare_city_date_data(actuals_data, historical_forecasts, source_weights):
    """Prepare (city, date) data with actual temps and ensemble forecasts."""
    # Extract actuals by (city, date)
    actuals_dict = {}
    for city, city_data in actuals_data["cities"].items():
        for date, temp in city_data["data"].items():
            actuals_dict[(city, date)] = temp
    
    # Reconstruct ensemble forecasts for each (city, date)
    ensemble_data = {}
    sources_data = historical_forecasts.get('sources', {})
    
    print("Reconstructing ensemble forecasts...")
    processed = 0
    skipped = 0
    
    # Get all unique city-date combinations from forecasts
    all_city_dates = set()
    for model_name, model_data in sources_data.items():
        for city in model_data.keys():
            for date in model_data[city].keys():
                all_city_dates.add((city, date))
    
    for city, date in all_city_dates:
        if (city, date) not in actuals_dict:
            skipped += 1
            continue
        
        # Collect forecasts from all models for this city-date
        forecasts = {}
        for model_name, model_data in sources_data.items():
            if city in model_data and date in model_data[city]:
                temp = model_data[city][date]
                if temp is not None:
                    forecasts[model_name] = temp
        
        if len(forecasts) < 3:
            skipped += 1
            continue
        
        # Compute base ensemble stats (with std_multiplier = 1.0)
        base_stats = weighted_ensemble_stats(forecasts, source_weights, 1.0, city)
        if base_stats:
            ensemble_data[(city, date)] = {
                'actual_temp': actuals_dict[(city, date)],
                'ensemble_mean': base_stats['ensemble_mean'],
                'raw_std': base_stats['ensemble_std'],  # Before multiplier adjustment
                'date': date
            }
            processed += 1
    
    print(f"Processed {processed} city-date combinations, skipped {skipped}")
    return ensemble_data

def generate_virtual_strikes_optimized(mean_temp, spacing=3, spread=8):
    """Generate fewer virtual strikes for optimization (faster computation)."""
    strikes = []
    for offset in range(-spread, spread + 1, spacing):
        strike = round(mean_temp + offset)
        if -20 <= strike <= 120:  # Reasonable temperature range
            strikes.append(strike)
    return strikes

def compute_brier_score_for_k(city_data, k, market_types=["threshold_yes"]):
    """Compute Brier score for a given sigma multiplier k."""
    predictions = []
    
    for (city, date), data in city_data.items():
        ensemble_mean = data['ensemble_mean']
        raw_std = data['raw_std']
        actual_temp = data['actual_temp']
        
        # Apply sigma multiplier
        adjusted_std = k * raw_std
        
        # Generate virtual strikes
        strikes = generate_virtual_strikes_optimized(ensemble_mean)
        
        for strike in strikes:
            for market_type in market_types:
                # Compute prediction probability
                if market_type == "threshold_yes":
                    pred_prob = compute_probability(ensemble_mean, adjusted_std, strike - 1, None)
                    actual_outcome = 1 if actual_temp >= strike else 0
                elif market_type == "threshold_no":
                    pred_prob = compute_probability(ensemble_mean, adjusted_std, None, strike - 1)
                    actual_outcome = 1 if actual_temp < strike else 0
                else:
                    continue
                
                # Clip probability for numerical stability
                pred_prob = max(0.001, min(0.999, pred_prob))
                predictions.append((pred_prob, actual_outcome))
    
    if not predictions:
        return float('inf')
    
    # Compute Brier score
    brier_sum = sum((prob - outcome) ** 2 for prob, outcome in predictions)
    return brier_sum / len(predictions)

def compute_sigma_coverage(city_data, k):
    """Compute 1-sigma coverage rate for diagnostic purposes."""
    within_1sigma = []
    
    for (city, date), data in city_data.items():
        ensemble_mean = data['ensemble_mean']
        raw_std = data['raw_std']
        actual_temp = data['actual_temp']
        
        adjusted_std = k * raw_std
        
        if adjusted_std > 0:
            z_score = abs(actual_temp - ensemble_mean) / adjusted_std
            within_1sigma.append(z_score <= 1.0)
    
    if within_1sigma:
        return statistics.mean(within_1sigma)
    return 0.0

def optimize_k_for_city(city, city_data, min_training_days=180):
    """Optimize sigma multiplier for a single city using expanding window walk-forward validation."""
    print(f"\nOptimizing sigma multiplier for {city}...")
    
    # Sort dates for walk-forward validation
    city_dates = sorted([date for (c, date) in city_data.keys() if c == city])
    
    if len(city_dates) < min_training_days + 30:  # Need minimum validation data
        print(f"  Insufficient data for {city}: {len(city_dates)} days")
        return None
    
    print(f"  Available dates: {len(city_dates)}")
    
    # Walk-forward validation results
    validation_scores = []
    k_values = [round(0.5 + i * 0.1, 1) for i in range(16)]  # 0.5 to 2.0 in 0.1 increments
    
    # Start validation after minimum training window
    validation_start_idx = min_training_days
    validation_dates = city_dates[validation_start_idx:]
    
    print(f"  Validation period: {len(validation_dates)} days")
    
    total_validations = 0
    k_scores = {k: [] for k in k_values}
    
    # For each validation date, use expanding window for training
    for val_idx, val_date in enumerate(validation_dates):
        if val_idx % 50 == 0:
            print(f"  Progress: {val_idx}/{len(validation_dates)} validation dates")
        
        # Training data: all data from start up to (but not including) validation date
        train_end_idx = validation_start_idx + val_idx
        train_dates = city_dates[:train_end_idx]
        
        if len(train_dates) < min_training_days:
            continue
        
        # Create training dataset for this city
        train_data = {(city, date): city_data[(city, date)] 
                     for date in train_dates if (city, date) in city_data}
        
        # Validation dataset (single day)
        if (city, val_date) not in city_data:
            continue
        
        val_data = {(city, val_date): city_data[(city, val_date)]}
        
        # Test each k value on validation data (trained on training data)
        best_train_k = None
        best_train_score = float('inf')
        
        # Find best k on training data
        for k in k_values:
            train_score = compute_brier_score_for_k(train_data, k)
            if train_score < best_train_score:
                best_train_score = train_score
                best_train_k = k
        
        # Evaluate best k on validation data
        if best_train_k:
            val_score = compute_brier_score_for_k(val_data, best_train_k)
            k_scores[best_train_k].append(val_score)
            total_validations += 1
    
    print(f"  Completed {total_validations} validations")
    
    if total_validations < 30:  # Need sufficient validation samples
        print(f"  Insufficient validation samples for {city}")
        return None
    
    # Find k with best average validation performance
    k_avg_scores = {}
    for k, scores in k_scores.items():
        if scores:
            k_avg_scores[k] = statistics.mean(scores)
    
    if not k_avg_scores:
        print(f"  No valid scores for {city}")
        return None
    
    optimal_k = min(k_avg_scores.keys(), key=lambda k: k_avg_scores[k])
    optimal_score = k_avg_scores[optimal_k]
    
    # Compute baseline score (k=1.0)
    baseline_score = k_avg_scores.get(1.0, float('inf'))
    brier_improvement = baseline_score - optimal_score
    
    # Compute sigma coverage for diagnostics
    full_city_data = {(c, d): data for (c, d), data in city_data.items() if c == city}
    sigma_coverage = compute_sigma_coverage(full_city_data, optimal_k)
    
    result = {
        'optimal_k': optimal_k,
        'optimal_brier_score': optimal_score,
        'baseline_brier_score': baseline_score,
        'brier_improvement': brier_improvement,
        'validation_samples': total_validations,
        'sigma_1_coverage': sigma_coverage,
        'sigma_coverage_diff': sigma_coverage - 0.683,  # vs theoretical 68.3%
        'k_scores_summary': {k: statistics.mean(scores) for k, scores in k_scores.items() if scores}
    }
    
    print(f"  Optimal k: {optimal_k}")
    print(f"  Brier improvement: {brier_improvement:.6f}")
    print(f"  1σ coverage: {sigma_coverage:.1%} (diff: {result['sigma_coverage_diff']:+.1%})")
    
    return result

def robustness_check_shifts(city_data, optimal_k_values, shifts=[-1.0, 1.0]):
    """Test robustness by shifting all forecasts by ±1°F."""
    print(f"\nRobustness check: Testing forecast shifts {shifts}")
    
    robustness_results = {}
    
    for shift in shifts:
        print(f"\nTesting {shift:+.1f}°F forecast shift...")
        shift_results = {}
        
        # Create shifted data
        shifted_data = {}
        for (city, date), data in city_data.items():
            shifted_data[(city, date)] = {
                'actual_temp': data['actual_temp'],  # Keep actual unchanged
                'ensemble_mean': data['ensemble_mean'] + shift,  # Shift forecast
                'raw_std': data['raw_std'],
                'date': data['date']
            }
        
        # Re-optimize each city with shifted data
        for city in optimal_k_values.keys():
            city_shifted_data = {(c, d): data for (c, d), data in shifted_data.items() if c == city}
            
            if len(city_shifted_data) < 200:  # Need sufficient data
                continue
            
            # Quick optimization (fewer k values, less validation)
            k_values = [round(0.7 + i * 0.2, 1) for i in range(8)]  # 0.7 to 2.1 in 0.2 increments
            k_scores = {}
            
            for k in k_values:
                score = compute_brier_score_for_k(city_shifted_data, k)
                k_scores[k] = score
            
            if k_scores:
                shifted_optimal_k = min(k_scores.keys(), key=lambda k: k_scores[k])
                original_k = optimal_k_values[city]['optimal_k']
                
                # Calculate stability
                k_change = abs(shifted_optimal_k - original_k)
                k_change_pct = k_change / original_k if original_k > 0 else float('inf')
                
                is_stable = k_change_pct < 0.10  # Less than 10% change
                
                shift_results[city] = {
                    'original_k': original_k,
                    'shifted_k': shifted_optimal_k,
                    'k_change': k_change,
                    'k_change_pct': k_change_pct,
                    'is_stable': is_stable
                }
                
                status = "STABLE" if is_stable else "FRAGILE"
                print(f"  {city}: {original_k:.1f} → {shifted_optimal_k:.1f} ({k_change_pct:.1%} change) - {status}")
        
        robustness_results[f"{shift:+.1f}F"] = shift_results
    
    return robustness_results

def main():
    print("SIGMA MULTIPLIER OPTIMIZATION")
    print("="*80)
    print("Optimizing sigma scaling factors using 365-day walk-forward validation")
    print("Target: Minimize Brier score for threshold markets\n")
    
    # Load data
    backtest_results, actuals_data, source_weights, historical_forecasts = load_optimization_data()
    
    # Prepare city-date dataset
    city_data = prepare_city_date_data(actuals_data, historical_forecasts, source_weights)
    
    if not city_data:
        print("ERROR: No city-date data available for optimization!")
        return
    
    # Get unique cities
    cities = list(set(city for city, date in city_data.keys()))
    print(f"Cities to optimize: {cities}")
    
    # Optimize each city
    optimization_results = {}
    
    for city in cities:
        result = optimize_k_for_city(city, city_data, min_training_days=180)
        if result:
            optimization_results[city] = result
    
    if not optimization_results:
        print("ERROR: No successful optimizations!")
        return
    
    # Robustness check
    print("\n" + "="*80)
    print("ROBUSTNESS CHECK: ±1°F Forecast Shifts")
    print("="*80)
    
    robustness_results = robustness_check_shifts(city_data, optimization_results)
    
    # Summary report
    print("\n" + "="*80)
    print("SIGMA OPTIMIZATION RESULTS SUMMARY")
    print("="*80)
    
    print("\nOptimal Sigma Multipliers:")
    print("-" * 50)
    for city, result in optimization_results.items():
        k = result['optimal_k']
        improvement = result['brier_improvement']
        coverage = result['sigma_1_coverage']
        samples = result['validation_samples']
        
        print(f"  {city:12}: k = {k:.1f} (Brier Δ: {improvement:+.6f}, 1σ: {coverage:.1%}, n={samples})")
    
    print("\nRobustness Analysis:")
    print("-" * 50)
    for shift, results in robustness_results.items():
        print(f"  {shift} shift:")
        stable_cities = sum(1 for r in results.values() if r['is_stable'])
        total_cities = len(results)
        print(f"    Stable multipliers: {stable_cities}/{total_cities} cities")
        
        for city, result in results.items():
            if not result['is_stable']:
                print(f"    ⚠️  FRAGILE: {city} ({result['k_change_pct']:.1%} change)")
    
    print("\nRecommendations:")
    print("-" * 50)
    
    all_stable = True
    for shift, results in robustness_results.items():
        for city, result in results.items():
            if not result['is_stable']:
                print(f"  ⚠️  {city}: Multiplier is fragile to forecast timing shifts")
                print(f"     Consider using k = {result['original_k']:.1f} with caution")
                all_stable = False
    
    if all_stable:
        print("  ✅ All multipliers are robust to ±1°F forecast shifts")
        print("  ✅ Recommended to implement optimized multipliers")
    
    # Save results
    output_file = "/home/ubuntu/.openclaw/workspace/weather-trading/sigma_optimization_results.json"
    results_data = {
        'optimization_results': optimization_results,
        'robustness_results': robustness_results,
        'summary': {
            'total_cities': len(optimization_results),
            'avg_brier_improvement': statistics.mean([r['brier_improvement'] for r in optimization_results.values()]),
            'all_multipliers_stable': all_stable
        },
        'recommended_multipliers': {city: result['optimal_k'] for city, result in optimization_results.items()}
    }
    
    with open(output_file, 'w') as f:
        json.dump(results_data, f, indent=2)
    
    print(f"\n📊 Results saved to: {output_file}")
    print(f"💡 Use 'recommended_multipliers' section for city_model_config.json updates")

if __name__ == "__main__":
    main()