#!/usr/bin/env python3
"""
Comprehensive Probability Calibration Backtest for Weather Trading Bot

This script analyzes the calibration of the weather bot's probability predictions
by reconstructing historical ensemble forecasts and comparing predicted probabilities
to actual outcomes across thousands of virtual strikes.
"""

import json
import os
import math
import statistics
from collections import defaultdict
from datetime import datetime

# Import functions from analyzer.py
import sys
sys.path.append('/home/ubuntu/.openclaw/workspace/weather-trading')
from analyzer import weighted_ensemble_stats, norm_cdf, compute_probability, _continuity_correct

# Data files
ACTUALS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/acis_actuals_365d.json"
HISTORICAL_FORECASTS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/historical_forecasts.json" 
SOURCE_WEIGHTS_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/source_weights.json"
CITY_CONFIG_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/city_model_config.json"

# Output directory
RESULTS_DIR = "/home/ubuntu/.openclaw/workspace/weather-trading/calibration_results"

def load_data_files():
    """Load all required data files."""
    print("Loading data files...")
    
    with open(ACTUALS_FILE) as f:
        actuals = json.load(f)
    
    with open(HISTORICAL_FORECASTS_FILE) as f:
        historical_forecasts = json.load(f)
    
    with open(SOURCE_WEIGHTS_FILE) as f:
        source_weights = json.load(f)["weights"]
    
    try:
        with open(CITY_CONFIG_FILE) as f:
            city_config = json.load(f)
    except:
        city_config = {"cities": {}}
    
    return actuals, historical_forecasts, source_weights, city_config

def generate_virtual_strikes(mean_temp, spacing=2, spread=10):
    """Generate virtual strikes around the forecast mean."""
    strikes = []
    for offset in range(-spread, spread + 1, spacing):
        strikes.append(round(mean_temp + offset))
    return strikes

def compute_bot_probability(mean, std, strike, market_type):
    """Compute probability the bot would have calculated for a given strike."""
    if market_type == "threshold_yes":
        # P(temp >= strike)
        return compute_probability(mean, std, strike - 1, None)  # floor = strike - 1 for "strike or above"
    elif market_type == "threshold_no":
        # P(temp < strike) 
        return compute_probability(mean, std, None, strike - 1)  # cap = strike - 1 for "below strike"
    elif market_type == "bracket_yes":
        # P(strike <= temp < strike+1) - 1°F bracket
        return compute_probability(mean, std, strike, strike)
    elif market_type == "bracket_no":
        # P(temp < strike OR temp > strike) = 1 - P(bracket_yes)
        bracket_prob = compute_probability(mean, std, strike, strike)
        return 1 - bracket_prob
    return 0.0

def check_actual_outcome(actual_temp, strike, market_type):
    """Check if actual outcome matched the market prediction."""
    if market_type == "threshold_yes":
        return actual_temp >= strike
    elif market_type == "threshold_no":
        return actual_temp < strike
    elif market_type == "bracket_yes":
        return strike <= actual_temp <= strike  # 1°F bracket
    elif market_type == "bracket_no":
        return not (strike <= actual_temp <= strike)
    return False

def bin_probabilities(prob_outcome_pairs, num_bins=10):
    """Bin probability predictions for reliability analysis."""
    bins = {i: {"predictions": [], "outcomes": [], "bin_center": (i + 0.5) / num_bins} 
            for i in range(num_bins)}
    
    for prob, outcome in prob_outcome_pairs:
        if not (0 <= prob <= 1):
            continue
        
        bin_idx = min(int(prob * num_bins), num_bins - 1)
        bins[bin_idx]["predictions"].append(prob)
        bins[bin_idx]["outcomes"].append(int(outcome))
    
    # Compute reliability stats for each bin
    reliability_data = []
    for i in range(num_bins):
        bin_data = bins[i]
        if bin_data["outcomes"]:
            mean_pred = statistics.mean(bin_data["predictions"])
            actual_rate = statistics.mean(bin_data["outcomes"])
            count = len(bin_data["outcomes"])
            
            reliability_data.append({
                "bin_center": bin_data["bin_center"],
                "mean_predicted": mean_pred,
                "actual_rate": actual_rate,
                "count": count,
                "calibration_error": abs(mean_pred - actual_rate)
            })
    
    return reliability_data

def compute_scores(prob_outcome_pairs):
    """Compute Brier score and log loss."""
    if not prob_outcome_pairs:
        return {"brier_score": None, "log_loss": None}
    
    brier_sum = 0
    log_loss_sum = 0
    valid_pairs = 0
    
    for prob, outcome in prob_outcome_pairs:
        if not (0 <= prob <= 1):
            continue
        
        # Brier score: (p - o)²
        brier_sum += (prob - outcome) ** 2
        
        # Log loss: -[o*log(p) + (1-o)*log(1-p)]
        epsilon = 1e-15  # Avoid log(0)
        p_clipped = max(epsilon, min(1 - epsilon, prob))
        if outcome == 1:
            log_loss_sum += -math.log(p_clipped)
        else:
            log_loss_sum += -math.log(1 - p_clipped)
        
        valid_pairs += 1
    
    if valid_pairs == 0:
        return {"brier_score": None, "log_loss": None}
    
    return {
        "brier_score": brier_sum / valid_pairs,
        "log_loss": log_loss_sum / valid_pairs
    }

def compute_sigma_validation(ensemble_stats, actuals_data):
    """Validate if ensemble std is correctly calibrated."""
    sigma_results = {"1sigma": [], "2sigma": [], "3sigma": []}
    city_results = defaultdict(lambda: {"1sigma": [], "2sigma": [], "3sigma": []})
    
    for (city, date), stats in ensemble_stats.items():
        if (city, date) not in actuals_data:
            continue
        
        actual = actuals_data[(city, date)]
        mean = stats["ensemble_mean"]
        std = stats["calibrated_std"]
        
        # Check if actual falls within 1σ, 2σ, 3σ
        z_score = abs(actual - mean) / std if std > 0 else float('inf')
        
        within_1sigma = z_score <= 1.0
        within_2sigma = z_score <= 2.0
        within_3sigma = z_score <= 3.0
        
        sigma_results["1sigma"].append(within_1sigma)
        sigma_results["2sigma"].append(within_2sigma)
        sigma_results["3sigma"].append(within_3sigma)
        
        city_results[city]["1sigma"].append(within_1sigma)
        city_results[city]["2sigma"].append(within_2sigma)
        city_results[city]["3sigma"].append(within_3sigma)
    
    # Compute percentages
    overall_stats = {}
    for sigma in ["1sigma", "2sigma", "3sigma"]:
        if sigma_results[sigma]:
            overall_stats[sigma + "_rate"] = statistics.mean(sigma_results[sigma])
            overall_stats[sigma + "_count"] = len(sigma_results[sigma])
    
    # Per-city stats
    city_stats = {}
    for city, data in city_results.items():
        city_stats[city] = {}
        for sigma in ["1sigma", "2sigma", "3sigma"]:
            if data[sigma]:
                city_stats[city][sigma + "_rate"] = statistics.mean(data[sigma])
                city_stats[city][sigma + "_count"] = len(data[sigma])
    
    # Compare to theoretical values
    theoretical = {"1sigma": 0.683, "2sigma": 0.954, "3sigma": 0.997}
    
    return {
        "overall": overall_stats,
        "per_city": city_stats,
        "theoretical": theoretical,
        "comparison": {
            "1sigma_diff": overall_stats.get("1sigma_rate", 0) - theoretical["1sigma"],
            "2sigma_diff": overall_stats.get("2sigma_rate", 0) - theoretical["2sigma"], 
            "3sigma_diff": overall_stats.get("3sigma_rate", 0) - theoretical["3sigma"]
        }
    }

def build_calibration_map(reliability_data):
    """Build a mapping from raw probabilities to calibrated probabilities."""
    calibration_map = {}
    
    for market_type, data in reliability_data.items():
        calibration_map[market_type] = []
        
        for bin_data in data:
            if bin_data["count"] >= 5:  # Only trust bins with sufficient data
                calibration_map[market_type].append({
                    "raw_prob": bin_data["mean_predicted"],
                    "calibrated_prob": bin_data["actual_rate"],
                    "bin_center": bin_data["bin_center"],
                    "sample_count": bin_data["count"]
                })
    
    return calibration_map

def main():
    # Create output directory
    os.makedirs(RESULTS_DIR, exist_ok=True)
    
    # Load data
    actuals, historical_forecasts, source_weights, city_config = load_data_files()
    
    print(f"Loaded {len(actuals['cities'])} cities with actual data")
    sources_data = historical_forecasts.get('sources', {})
    print(f"Loaded {len(sources_data)} forecast models")
    
    # Process data to extract actual temperatures by (city, date)
    actuals_data = {}
    for city, city_data in actuals["cities"].items():
        for date, temp in city_data["data"].items():
            actuals_data[(city, date)] = temp
    
    print(f"Total actual temperature records: {len(actuals_data)}")
    
    # Reconstruct ensemble statistics for each city/date combination
    print("Reconstructing ensemble statistics...")
    ensemble_stats = {}
    processed_combinations = 0
    skipped_entries = 0
    
    # Get all cities and dates from the historical forecast data
    all_cities = set()
    all_dates = set()
    
    for model_name, model_data in sources_data.items():
        for city in model_data.keys():
            all_cities.add(city)
            all_dates.update(model_data[city].keys())
    
    print(f"Processing {len(all_cities)} cities × {len(all_dates)} dates...")
    
    for city in all_cities:
        for date in all_dates:
            # Collect all available forecasts for this city/date
            forecasts = {}
            
            for model_name, model_data in sources_data.items():
                if city in model_data and date in model_data[city]:
                    temp = model_data[city][date]
                    if temp is not None:
                        forecasts[model_name] = temp
            
            if len(forecasts) < 3:  # Need at least 3 models for meaningful ensemble
                skipped_entries += 1
                continue
            
            # Get city-specific multiplier if available
            city_multiplier = 1.0
            if city in city_config.get("cities", {}):
                city_multiplier = city_config["cities"][city].get("std_multiplier", 1.0)
            
            # Compute ensemble stats using the same logic as the bot
            stats = weighted_ensemble_stats(forecasts, source_weights, city_multiplier, city)
            
            if stats:
                ensemble_stats[(city, date)] = stats
                processed_combinations += 1
    
    print(f"Reconstructed {len(ensemble_stats)} ensemble forecasts")
    print(f"Skipped {skipped_entries} combinations due to insufficient forecast data")
    
    # Find overlapping data (where we have both actual and forecast)
    overlapping_keys = set(actuals_data.keys()) & set(ensemble_stats.keys())
    print(f"Found {len(overlapping_keys)} overlapping city/date pairs")
    
    if len(overlapping_keys) == 0:
        print("ERROR: No overlapping data found! Check date formats and city names.")
        return
    
    # Generate calibration data
    print("Generating calibration data...")
    
    calibration_data = {
        "threshold_yes": [],  # (predicted_prob, actual_outcome)
        "threshold_no": [],
        "bracket_yes": [],
        "bracket_no": []
    }
    
    total_predictions = 0
    
    for city, date in overlapping_keys:
        actual_temp = actuals_data[(city, date)]
        stats = ensemble_stats[(city, date)]
        
        ensemble_mean = stats["ensemble_mean"]
        calibrated_std = stats["calibrated_std"]
        
        # Generate virtual strikes around the forecast
        virtual_strikes = generate_virtual_strikes(ensemble_mean)
        
        for strike in virtual_strikes:
            # Skip strikes that are too far from reasonable temperatures
            if not (-20 <= strike <= 120):
                continue
            
            # Test all market types for this strike
            market_types = ["threshold_yes", "threshold_no", "bracket_yes", "bracket_no"]
            
            for market_type in market_types:
                predicted_prob = compute_bot_probability(ensemble_mean, calibrated_std, strike, market_type)
                actual_outcome = check_actual_outcome(actual_temp, strike, market_type)
                
                calibration_data[market_type].append((predicted_prob, actual_outcome))
                total_predictions += 1
    
    print(f"Generated {total_predictions} total probability predictions")
    for market_type, data in calibration_data.items():
        print(f"  {market_type}: {len(data)} predictions")
    
    # Compute reliability/calibration data
    print("Computing reliability analysis...")
    reliability_data = {}
    for market_type, prob_outcome_pairs in calibration_data.items():
        reliability_data[market_type] = bin_probabilities(prob_outcome_pairs)
    
    # Compute overall scores
    print("Computing performance scores...")
    score_data = {}
    for market_type, prob_outcome_pairs in calibration_data.items():
        score_data[market_type] = compute_scores(prob_outcome_pairs)
    
    # Overall scores (all market types combined)
    all_pairs = []
    for pairs in calibration_data.values():
        all_pairs.extend(pairs)
    score_data["overall"] = compute_scores(all_pairs)
    
    # Sigma validation
    print("Computing sigma validation...")
    sigma_validation = compute_sigma_validation(ensemble_stats, actuals_data)
    
    # Per-city reliability
    print("Computing per-city reliability...")
    city_calibration_data = defaultdict(lambda: {"threshold_yes": [], "threshold_no": [], "bracket_yes": [], "bracket_no": []})
    
    for city, date in overlapping_keys:
        actual_temp = actuals_data[(city, date)]
        stats = ensemble_stats[(city, date)]
        
        ensemble_mean = stats["ensemble_mean"]
        calibrated_std = stats["calibrated_std"]
        virtual_strikes = generate_virtual_strikes(ensemble_mean)
        
        for strike in virtual_strikes:
            if not (-20 <= strike <= 120):
                continue
            
            for market_type in ["threshold_yes", "threshold_no", "bracket_yes", "bracket_no"]:
                predicted_prob = compute_bot_probability(ensemble_mean, calibrated_std, strike, market_type)
                actual_outcome = check_actual_outcome(actual_temp, strike, market_type)
                city_calibration_data[city][market_type].append((predicted_prob, actual_outcome))
    
    city_reliability = {}
    for city, market_data in city_calibration_data.items():
        city_reliability[city] = {}
        for market_type, prob_outcome_pairs in market_data.items():
            city_reliability[city][market_type] = bin_probabilities(prob_outcome_pairs)
    
    # Build calibration mapping
    calibration_map = build_calibration_map(reliability_data)
    
    # Save all results
    print("Saving results...")
    
    with open(f"{RESULTS_DIR}/reliability.json", "w") as f:
        json.dump(reliability_data, f, indent=2)
    
    with open(f"{RESULTS_DIR}/scores.json", "w") as f:
        json.dump(score_data, f, indent=2)
    
    with open(f"{RESULTS_DIR}/calibration_map.json", "w") as f:
        json.dump(calibration_map, f, indent=2)
    
    with open(f"{RESULTS_DIR}/sigma_validation.json", "w") as f:
        json.dump(sigma_validation, f, indent=2)
    
    with open(f"{RESULTS_DIR}/city_reliability.json", "w") as f:
        json.dump(city_reliability, f, indent=2)
    
    # Print summary
    print("\n" + "="*80)
    print("CALIBRATION BACKTEST RESULTS SUMMARY")
    print("="*80)
    
    print(f"\nData Coverage:")
    print(f"  Total predictions analyzed: {total_predictions:,}")
    print(f"  Cities: {len(set(city for city, date in overlapping_keys))}")
    print(f"  Date range: {len(overlapping_keys)} city-date combinations")
    
    print(f"\nOverall Performance:")
    overall_scores = score_data["overall"]
    if overall_scores["brier_score"] is not None:
        print(f"  Brier Score: {overall_scores['brier_score']:.4f} (lower is better, 0.25 = random)")
        print(f"  Log Loss: {overall_scores['log_loss']:.4f} (lower is better)")
    
    print(f"\nSigma Calibration:")
    sigma_stats = sigma_validation["overall"]
    sigma_comparison = sigma_validation["comparison"]
    print(f"  1σ coverage: {sigma_stats.get('1sigma_rate', 0):.1%} (theory: 68.3%, diff: {sigma_comparison['1sigma_diff']:+.1%})")
    print(f"  2σ coverage: {sigma_stats.get('2sigma_rate', 0):.1%} (theory: 95.4%, diff: {sigma_comparison['2sigma_diff']:+.1%})")
    print(f"  3σ coverage: {sigma_stats.get('3sigma_rate', 0):.1%} (theory: 99.7%, diff: {sigma_comparison['3sigma_diff']:+.1%})")
    
    if abs(sigma_comparison["1sigma_diff"]) > 0.1:
        if sigma_comparison["1sigma_diff"] > 0:
            print("  → Sigma appears too WIDE (under-confident)")
        else:
            print("  → Sigma appears too NARROW (over-confident)")
    else:
        print("  → Sigma calibration looks reasonable")
    
    print(f"\nWorst Calibrated Probability Ranges:")
    # Find bins with largest calibration errors
    worst_errors = []
    for market_type, bins in reliability_data.items():
        for bin_data in bins:
            if bin_data["count"] >= 10:  # Only consider bins with substantial data
                error = bin_data["calibration_error"]
                worst_errors.append((error, market_type, bin_data["bin_center"], bin_data["count"]))
    
    worst_errors.sort(reverse=True)
    for i, (error, market_type, bin_center, count) in enumerate(worst_errors[:5]):
        print(f"  {i+1}. {market_type}: {bin_center:.0%} bin (error: {error:.1%}, n={count})")
    
    print(f"\nRecommended Adjustments:")
    if sigma_comparison["1sigma_diff"] > 0.15:
        print("  - Consider reducing std_multiplier (sigma too wide)")
    elif sigma_comparison["1sigma_diff"] < -0.15:
        print("  - Consider increasing std_multiplier (sigma too narrow)")
    
    if worst_errors and worst_errors[0][0] > 0.2:
        print("  - Significant probability miscalibration detected")
        print("  - Consider implementing probability correction via calibration_map.json")
        print("  - Focus on fixing tail probability estimates (extreme YES bets)")
    
    # Check specific issues with tail probabilities (low probability YES bets)
    threshold_yes_reliability = reliability_data.get("threshold_yes", [])
    low_prob_bins = [bin_data for bin_data in threshold_yes_reliability if bin_data["bin_center"] < 0.2 and bin_data["count"] >= 10]
    
    if low_prob_bins:
        avg_error = statistics.mean([bin_data["calibration_error"] for bin_data in low_prob_bins])
        if avg_error > 0.1:
            print("  - CRITICAL: Tail probabilities (longshot YES bets) are severely miscalibrated!")
            print("    This is likely the main source of losses.")
    
    print(f"\nResults saved to: {RESULTS_DIR}/")
    print("Files created: reliability.json, scores.json, calibration_map.json, sigma_validation.json, city_reliability.json")
    print("="*80)

if __name__ == "__main__":
    main()