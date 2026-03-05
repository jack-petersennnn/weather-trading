#!/usr/bin/env python3
"""
Probability Correction Module

This module applies calibration corrections to raw probability predictions
based on the calibration backtest results. Use this to fix longshot YES bet
miscalibration and improve overall bot profitability.
"""

import json
import os

CALIBRATION_MAP_FILE = "/home/ubuntu/.openclaw/workspace/weather-trading/calibration_results/calibration_map.json"

# Cache for calibration corrections
_calibration_corrections = None

def load_calibration_corrections():
    """Load calibration correction mappings from backtest results."""
    global _calibration_corrections
    
    if _calibration_corrections is not None:
        return _calibration_corrections
    
    if not os.path.exists(CALIBRATION_MAP_FILE):
        print("Warning: Calibration map file not found. No corrections will be applied.")
        _calibration_corrections = {}
        return _calibration_corrections
    
    with open(CALIBRATION_MAP_FILE) as f:
        calibration_map = json.load(f)
    
    # Convert to lookup tables for efficient correction
    _calibration_corrections = {}
    
    for market_type, corrections in calibration_map.items():
        # Sort by raw probability for interpolation
        corrections.sort(key=lambda x: x["raw_prob"])
        _calibration_corrections[market_type] = corrections
    
    print(f"Loaded calibration corrections for {len(_calibration_corrections)} market types")
    return _calibration_corrections

def apply_probability_correction(raw_probability, market_type, min_sample_size=100):
    """
    Apply calibration correction to a raw probability.
    
    Args:
        raw_probability: The uncorrected probability (0.0 to 1.0)
        market_type: One of "threshold_yes", "threshold_no", "bracket_yes", "bracket_no"
        min_sample_size: Minimum sample size to trust a correction
        
    Returns:
        Corrected probability (0.0 to 1.0)
    """
    corrections = load_calibration_corrections()
    
    if market_type not in corrections:
        return raw_probability  # No correction available
    
    correction_data = corrections[market_type]
    if not correction_data:
        return raw_probability
    
    # Find the best correction via interpolation
    corrected_prob = interpolate_correction(raw_probability, correction_data, min_sample_size)
    
    # Ensure result is in valid range
    return max(0.001, min(0.999, corrected_prob))

def interpolate_correction(raw_prob, correction_data, min_sample_size):
    """Interpolate correction between calibration data points."""
    
    # Filter out corrections with insufficient sample size
    reliable_corrections = [
        c for c in correction_data 
        if c["sample_count"] >= min_sample_size
    ]
    
    if not reliable_corrections:
        return raw_prob  # No reliable corrections available
    
    # Find surrounding data points
    below = None
    above = None
    
    for correction in reliable_corrections:
        if correction["raw_prob"] <= raw_prob:
            below = correction
        elif correction["raw_prob"] > raw_prob and above is None:
            above = correction
            break
    
    # Apply correction
    if below is None:
        # Extrapolate from lowest data point
        return reliable_corrections[0]["calibrated_prob"]
    elif above is None:
        # Extrapolate from highest data point
        return reliable_corrections[-1]["calibrated_prob"]
    else:
        # Interpolate between two points
        weight = (raw_prob - below["raw_prob"]) / (above["raw_prob"] - below["raw_prob"])
        corrected = below["calibrated_prob"] * (1 - weight) + above["calibrated_prob"] * weight
        return corrected

def get_correction_confidence(raw_probability, market_type):
    """
    Get confidence level for a probability correction.
    
    Returns:
        confidence: 0.0 to 1.0, where 1.0 = high confidence in correction
    """
    corrections = load_calibration_corrections()
    
    if market_type not in corrections:
        return 0.0
    
    correction_data = corrections[market_type]
    
    # Find closest calibration point
    closest_correction = min(
        correction_data,
        key=lambda x: abs(x["raw_prob"] - raw_probability)
    )
    
    # Distance-based confidence (closer = higher confidence)
    distance = abs(closest_correction["raw_prob"] - raw_probability)
    distance_confidence = max(0.0, 1.0 - distance * 10)  # Sharp falloff
    
    # Sample size confidence (more samples = higher confidence)
    sample_confidence = min(1.0, closest_correction["sample_count"] / 500)
    
    # Combined confidence
    return distance_confidence * sample_confidence

def correct_market_probabilities(probabilities_dict):
    """
    Apply corrections to a dictionary of market probabilities.
    
    Args:
        probabilities_dict: Dict with keys like "threshold_yes_prob", "bracket_yes_prob"
    
    Returns:
        Dict with corrected probabilities and correction metadata
    """
    corrected = {}
    metadata = {}
    
    market_type_mapping = {
        "threshold_yes_prob": "threshold_yes",
        "threshold_no_prob": "threshold_no", 
        "bracket_yes_prob": "bracket_yes",
        "bracket_no_prob": "bracket_no"
    }
    
    for key, raw_prob in probabilities_dict.items():
        if key in market_type_mapping:
            market_type = market_type_mapping[key]
            
            corrected_prob = apply_probability_correction(raw_prob, market_type)
            confidence = get_correction_confidence(raw_prob, market_type)
            
            corrected[key] = corrected_prob
            metadata[key] = {
                "raw_probability": raw_prob,
                "corrected_probability": corrected_prob,
                "correction_applied": abs(corrected_prob - raw_prob),
                "confidence": confidence
            }
        else:
            # Pass through uncorrected
            corrected[key] = raw_prob
    
    return corrected, metadata

# Example usage and testing
def demo_corrections():
    """Demonstrate probability corrections on sample data."""
    print("PROBABILITY CORRECTION DEMO")
    print("=" * 50)
    
    # Sample problematic probabilities from backtest
    test_cases = [
        (0.008, "threshold_yes"),  # Very low probability longshot
        (0.15, "threshold_yes"),   # 15% bin with known miscalibration  
        (0.25, "threshold_yes"),   # 25% bin with known miscalibration
        (0.55, "threshold_yes"),   # Mid-range with miscalibration
        (0.85, "threshold_yes"),   # High probability
        (0.99, "threshold_yes"),   # Very high probability
    ]
    
    for raw_prob, market_type in test_cases:
        corrected = apply_probability_correction(raw_prob, market_type)
        confidence = get_correction_confidence(raw_prob, market_type)
        adjustment = corrected - raw_prob
        
        print(f"{market_type}: {raw_prob:.1%} → {corrected:.1%} ({adjustment:+.1%}) [conf: {confidence:.2f}]")

if __name__ == "__main__":
    demo_corrections()