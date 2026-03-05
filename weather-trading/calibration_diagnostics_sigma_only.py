#!/usr/bin/env python3
"""
Calibration Diagnostics with Sigma-Only Changes

This script re-runs calibration diagnostics focusing on:
1. Brier score restricted to 15-85% probability range only  
2. Comparison against climatology baseline
3. Focus on threshold markets (rescue mode is threshold-only)
4. Per-city results using corrected sigma multipliers
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
ACTUALS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/acis_actuals_365d.json"
HISTORICAL_FORECASTS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/historical_forecasts.json"
SOURCE_WEIGHTS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/source_weights.json"

def load_data():
    """Load historical data for diagnostics."""
    print("Loading data for calibration diagnostics...")
    
    with open(ACTUALS_FILE) as f:
        actuals_data = json.load(f)
    
    with open(HISTORICAL_FORECASTS_FILE) as f:
        historical_forecasts = json.load(f)
    
    with open(SOURCE_WEIGHTS_FILE) as f:
        source_weights = json.load(f)["weights"]
    
    return actuals_data, historical_forecasts, source_weights

def compute_climatology_baseline(actuals_data):
    """Compute climatology baseline - historical frequency of temperature thresholds."""
    print("Computing climatology baseline...")
    
    city_climatology = {}
    
    for city, city_data in actuals_data["cities"].items():
        temps = list(city_data["data"].values())
        if len(temps) < 30:  # Need sufficient historical data
            continue
        
        # Compute climatological probabilities for various thresholds
        temp_ranges = {}
        
        # Test thresholds from 10°F to 100°F in 5°F increments
        for threshold in range(10, 101, 5):
            above_count = sum(1 for t in temps if t >= threshold)
            below_count = len(temps) - above_count
            
            temp_ranges[f"above_{threshold}F"] = above_count / len(temps)
            temp_ranges[f"below_{threshold}F"] = below_count / len(temps)
        
        city_climatology[city] = {
            'sample_size': len(temps),
            'mean_temp': statistics.mean(temps),
            'std_temp': statistics.pstdev(temps),
            'threshold_probs': temp_ranges
        }
    
    return city_climatology

def prepare_ensemble_data(actuals_data, historical_forecasts, source_weights, sigma_multipliers=None):
    """Prepare ensemble data with optional sigma multipliers."""
    # Extract actuals by (city, date)
    actuals_dict = {}
    for city, city_data in actuals_data["cities"].items():
        for date, temp in city_data["data"].items():
            actuals_dict[(city, date)] = temp
    
    # Reconstruct ensemble forecasts
    ensemble_data = {}
    sources_data = historical_forecasts.get('sources', {})
    
    processed = 0
    skipped = 0
    
    # Get all unique city-date combinations
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
        
        # Get sigma multiplier for this city
        city_multiplier = 1.0
        if sigma_multipliers and city in sigma_multipliers:
            city_multiplier = sigma_multipliers[city]
        
        # Compute ensemble stats
        stats = weighted_ensemble_stats(forecasts, source_weights, city_multiplier, city)
        if stats:
            ensemble_data[(city, date)] = {
                'actual_temp': actuals_dict[(city, date)],
                'ensemble_mean': stats['ensemble_mean'],
                'calibrated_std': stats['calibrated_std'],
                'date': date
            }
            processed += 1
    
    print(f"Processed {processed} ensemble forecasts, skipped {skipped}")
    return ensemble_data

def generate_threshold_predictions(ensemble_data, prob_range=(0.15, 0.85)):
    """Generate threshold market predictions within specified probability range."""
    predictions = []
    
    for (city, date), data in ensemble_data.items():
        ensemble_mean = data['ensemble_mean']
        calibrated_std = data['calibrated_std']
        actual_temp = data['actual_temp']
        
        # Generate strikes around the forecast
        for offset in range(-12, 13, 2):  # ±12°F in 2°F increments
            strike = round(ensemble_mean + offset)
            if not (-20 <= strike <= 120):
                continue
            
            # Threshold YES: P(temp >= strike)
            pred_prob_yes = compute_probability(ensemble_mean, calibrated_std, strike - 1, None)
            
            # Threshold NO: P(temp < strike)  
            pred_prob_no = compute_probability(ensemble_mean, calibrated_std, None, strike - 1)
            
            # Filter to specified probability range
            if prob_range[0] <= pred_prob_yes <= prob_range[1]:
                actual_outcome_yes = 1 if actual_temp >= strike else 0
                predictions.append({
                    'city': city,
                    'date': date,
                    'market_type': 'threshold_yes',
                    'strike': strike,
                    'predicted_prob': pred_prob_yes,
                    'actual_outcome': actual_outcome_yes
                })
            
            if prob_range[0] <= pred_prob_no <= prob_range[1]:
                actual_outcome_no = 1 if actual_temp < strike else 0
                predictions.append({
                    'city': city,
                    'date': date,
                    'market_type': 'threshold_no',
                    'strike': strike,
                    'predicted_prob': pred_prob_no,
                    'actual_outcome': actual_outcome_no
                })
    
    return predictions

def compute_brier_score(predictions):
    """Compute Brier score for a list of predictions."""
    if not predictions:
        return None
    
    brier_sum = sum((p['predicted_prob'] - p['actual_outcome']) ** 2 for p in predictions)
    return brier_sum / len(predictions)

def compute_climatology_brier_baseline(predictions, city_climatology):
    """Compute Brier score using climatological probabilities as baseline."""
    climatology_predictions = []
    
    for pred in predictions:
        city = pred['city']
        strike = pred['strike']
        market_type = pred['market_type']
        
        if city not in city_climatology:
            continue
        
        # Get climatological probability for this threshold
        threshold_key = f"above_{strike}F" if market_type == "threshold_yes" else f"below_{strike}F"
        
        # Find closest available threshold
        available_thresholds = [key for key in city_climatology[city]['threshold_probs'].keys()]
        
        if threshold_key in city_climatology[city]['threshold_probs']:
            clim_prob = city_climatology[city]['threshold_probs'][threshold_key]
        else:
            # Use overall statistics if specific threshold not available
            city_mean = city_climatology[city]['mean_temp']
            city_std = city_climatology[city]['std_temp']
            
            if market_type == "threshold_yes":
                # P(X >= strike) using normal approximation
                z = (strike - city_mean) / city_std if city_std > 0 else 0
                clim_prob = 0.5 * (1 - math.erf(z / math.sqrt(2)))
            else:
                # P(X < strike)
                z = (strike - city_mean) / city_std if city_std > 0 else 0
                clim_prob = 0.5 * (1 + math.erf(z / math.sqrt(2)))
        
        # Clip to reasonable range
        clim_prob = max(0.01, min(0.99, clim_prob))
        
        climatology_predictions.append({
            'predicted_prob': clim_prob,
            'actual_outcome': pred['actual_outcome']
        })
    
    return compute_brier_score(climatology_predictions)

def analyze_by_city(predictions):
    """Break down results by city."""
    city_results = {}
    
    city_groups = defaultdict(list)
    for pred in predictions:
        city_groups[pred['city']].append(pred)
    
    for city, city_preds in city_groups.items():
        if len(city_preds) < 20:  # Need sufficient data
            continue
        
        brier_score = compute_brier_score(city_preds)
        
        # Count market types
        yes_count = sum(1 for p in city_preds if p['market_type'] == 'threshold_yes')
        no_count = len(city_preds) - yes_count
        
        # Accuracy metrics
        correct = sum(1 for p in city_preds if abs(p['predicted_prob'] - p['actual_outcome']) < 0.5)
        accuracy = correct / len(city_preds)
        
        city_results[city] = {
            'brier_score': brier_score,
            'sample_size': len(city_preds),
            'threshold_yes_count': yes_count,
            'threshold_no_count': no_count,
            'accuracy': accuracy,
            'mean_predicted_prob': statistics.mean([p['predicted_prob'] for p in city_preds]),
            'mean_actual_rate': statistics.mean([p['actual_outcome'] for p in city_preds])
        }
    
    return city_results

def run_diagnostics_comparison(baseline_multipliers=None, optimized_multipliers=None):
    """Run diagnostics comparing baseline vs optimized sigma multipliers."""
    print("\nRUNNING CALIBRATION DIAGNOSTICS COMPARISON")
    print("="*80)
    
    # Load data
    actuals_data, historical_forecasts, source_weights = load_data()
    
    # Compute climatology baseline
    city_climatology = compute_climatology_baseline(actuals_data)
    
    results = {}
    
    # Test configurations
    configs = [
        ("baseline", baseline_multipliers),
        ("optimized", optimized_multipliers)
    ]
    
    for config_name, multipliers in configs:
        print(f"\nTesting {config_name} configuration...")
        print("-" * 50)
        
        # Prepare ensemble data with these multipliers
        ensemble_data = prepare_ensemble_data(
            actuals_data, historical_forecasts, source_weights, multipliers
        )
        
        # Generate predictions for 15-85% probability range (avoiding extreme probabilities)
        predictions = generate_threshold_predictions(ensemble_data, prob_range=(0.15, 0.85))
        
        print(f"Generated {len(predictions)} threshold predictions in 15-85% range")
        
        if not predictions:
            print(f"No predictions for {config_name} configuration")
            continue
        
        # Compute overall Brier score
        overall_brier = compute_brier_score(predictions)
        
        # Compute climatology baseline Brier score
        climatology_brier = compute_climatology_brier_baseline(predictions, city_climatology)
        
        # Per-city analysis
        city_results = analyze_by_city(predictions)
        
        # Overall metrics
        yes_predictions = [p for p in predictions if p['market_type'] == 'threshold_yes']
        no_predictions = [p for p in predictions if p['market_type'] == 'threshold_no']
        
        results[config_name] = {
            'overall_brier_score': overall_brier,
            'climatology_baseline_brier': climatology_brier,
            'skill_score': (climatology_brier - overall_brier) / climatology_brier if climatology_brier else 0,
            'total_predictions': len(predictions),
            'threshold_yes_predictions': len(yes_predictions),
            'threshold_no_predictions': len(no_predictions),
            'threshold_yes_brier': compute_brier_score(yes_predictions) if yes_predictions else None,
            'threshold_no_brier': compute_brier_score(no_predictions) if no_predictions else None,
            'city_results': city_results
        }
        
        print(f"Overall Brier Score: {overall_brier:.4f}")
        print(f"Climatology Baseline: {climatology_brier:.4f}")
        print(f"Skill Score: {results[config_name]['skill_score']:.3f} (higher is better)")
        print(f"Threshold YES Brier: {results[config_name]['threshold_yes_brier']:.4f}")
        print(f"Threshold NO Brier: {results[config_name]['threshold_no_brier']:.4f}")
    
    return results

def main():
    print("CALIBRATION DIAGNOSTICS - SIGMA-ONLY CHANGES")
    print("="*80)
    print("Focus: 15-85% probability range, threshold markets only")
    print("Comparison: Model vs Climatology baseline")
    
    # Try to load optimized multipliers if available
    optimized_multipliers = None
    try:
        with open("/home/ubuntu/.openclaw/workspace/weather-trading/sigma_optimization_results.json") as f:
            opt_results = json.load(f)
            optimized_multipliers = opt_results.get("recommended_multipliers", {})
            print(f"Loaded optimized multipliers: {optimized_multipliers}")
    except:
        print("No optimized multipliers found, using baseline only")
    
    # Baseline multipliers (current system)
    baseline_multipliers = {
        "Chicago": 1.0,
        "Denver": 1.0, 
        "Austin": 1.0,
        "New York": 1.0,
        "Miami": 1.0
    }
    
    # Run diagnostics
    if optimized_multipliers:
        results = run_diagnostics_comparison(baseline_multipliers, optimized_multipliers)
        
        # Comparison summary
        print("\n" + "="*80)
        print("COMPARISON SUMMARY")
        print("="*80)
        
        baseline_brier = results["baseline"]["overall_brier_score"]
        optimized_brier = results["optimized"]["overall_brier_score"]
        improvement = baseline_brier - optimized_brier
        improvement_pct = (improvement / baseline_brier) * 100
        
        print(f"Baseline Brier Score: {baseline_brier:.4f}")
        print(f"Optimized Brier Score: {optimized_brier:.4f}")
        print(f"Improvement: {improvement:+.4f} ({improvement_pct:+.1f}%)")
        
        # Per-city improvements
        print("\nPer-City Improvements:")
        print("-" * 30)
        
        for city in baseline_multipliers.keys():
            if (city in results["baseline"]["city_results"] and 
                city in results["optimized"]["city_results"]):
                
                base_city_brier = results["baseline"]["city_results"][city]["brier_score"]
                opt_city_brier = results["optimized"]["city_results"][city]["brier_score"]
                city_improvement = base_city_brier - opt_city_brier
                city_improvement_pct = (city_improvement / base_city_brier) * 100
                
                print(f"  {city:12}: {city_improvement:+.4f} ({city_improvement_pct:+.1f}%)")
        
        # Skill score comparison
        baseline_skill = results["baseline"]["skill_score"]
        optimized_skill = results["optimized"]["skill_score"]
        
        print(f"\nSkill Scores (vs Climatology):")
        print(f"  Baseline: {baseline_skill:.3f}")
        print(f"  Optimized: {optimized_skill:.3f}")
        print(f"  Skill Improvement: {optimized_skill - baseline_skill:+.3f}")
        
    else:
        # Run baseline only
        results = run_diagnostics_comparison(baseline_multipliers, None)
        results = {"baseline": results["baseline"]}
    
    # Save results
    output_file = "/home/ubuntu/.openclaw/workspace/weather-trading/calibration_diagnostics_results.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n📊 Results saved to: {output_file}")
    
    # Summary recommendations
    print("\n" + "="*80)
    print("RECOMMENDATIONS")
    print("="*80)
    
    if optimized_multipliers and improvement > 0:
        print("✅ Optimized sigma multipliers show improvement!")
        print("✅ Recommend implementing optimized multipliers")
        print(f"✅ Expected Brier score reduction: {improvement:.4f}")
    elif optimized_multipliers and improvement < 0:
        print("⚠️  Optimized multipliers show worse performance")
        print("⚠️  Stick with baseline multipliers")
        print("⚠️  Review optimization methodology")
    else:
        print("📋 Baseline performance established")
        print("📋 Use this as reference for future improvements")
    
    # Focus on threshold markets note
    print(f"\n💡 This analysis focuses on threshold markets (rescue mode)")
    print(f"💡 15-85% probability range avoids extreme miscalibration issues")
    print(f"💡 Compare skill scores > 0 indicate model beats climatology")

if __name__ == "__main__":
    main()