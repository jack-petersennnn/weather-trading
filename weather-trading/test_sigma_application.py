#!/usr/bin/env python3
"""
Test that sigma multipliers are applied correctly in weighted_ensemble_stats
"""
import analyzer

def test_sigma_application():
    """Test that sigma multipliers are applied correctly in ensemble calculations."""
    print("Testing sigma multiplier application in ensemble calculations...")
    
    # Mock forecast data
    test_forecasts = {
        "GFS": 65.0,
        "ICON": 67.0,
        "ECMWF": 64.0,
        "UKMO": 66.0,
        "Canadian GEM": 65.5
    }
    
    # Mock weights (using default equal weights)
    test_weights = {name: 1.0 for name in test_forecasts.keys()}
    
    # Test cities with different multipliers
    test_cities = ["Chicago", "Denver", "Miami", "Austin", "New York"]
    
    print("\nSigma multiplier results per city:")
    print("=" * 60)
    
    for city in test_cities:
        # Get expected multiplier
        expected_multiplier = analyzer.get_sigma_multiplier(city)
        
        # Calculate ensemble stats
        stats = analyzer.weighted_ensemble_stats(
            test_forecasts, test_weights, expected_multiplier, city
        )
        
        raw_std = stats.get("ensemble_std", 0)
        calibrated_std = stats.get("calibrated_std", 0)
        mean = stats.get("ensemble_mean", 0)
        
        # Verify the multiplier was applied correctly
        expected_calibrated = raw_std * expected_multiplier
        
        print(f"{city:12}: multiplier={expected_multiplier:.1f}, "
              f"raw_std={raw_std:.2f}, "
              f"calibrated_std={calibrated_std:.2f}, "
              f"expected={expected_calibrated:.2f}")
        
        # Verify calculation is correct
        if abs(calibrated_std - expected_calibrated) > 0.01:
            print(f"  ⚠️  WARNING: Calculation mismatch!")
        else:
            print(f"  ✅ Correct application")
        
        # Show the predicted range
        print(f"  📊 Predicted range: {mean:.1f}°F ± {calibrated_std:.1f}°F "
              f"({mean - calibrated_std:.1f}°F to {mean + calibrated_std:.1f}°F)")
        print()
    
    print("🎉 Sigma multiplier application test complete!")

if __name__ == "__main__":
    test_sigma_application()