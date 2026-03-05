#!/usr/bin/env python3
"""
Test sigma multiplier hard cap enforcement
"""
import json
import os

# Load the analyzer module
import analyzer

def test_sigma_caps():
    """Test that sigma multipliers are properly capped."""
    print("Testing sigma multiplier hard caps...")
    
    # Test normal cities
    test_cases = [
        ("Chicago", 1.5),
        ("Denver", 1.0),
        ("Miami", 1.1),
        ("Austin", 0.9),
        ("New York", 0.9),
    ]
    
    for city, expected in test_cases:
        actual = analyzer.get_sigma_multiplier(city)
        print(f"  {city}: expected {expected}, got {actual}")
        assert abs(actual - expected) < 0.01, f"Mismatch for {city}: {actual} != {expected}"
    
    print("\n✅ All city sigma multipliers correct!")
    
    # Test the hard cap by temporarily modifying the config
    config_file = "/home/ubuntu/.openclaw/workspace/weather-trading/city_model_config.json"
    
    # Read current config
    with open(config_file) as f:
        original_config = json.load(f)
    
    try:
        # Test values that should be capped
        test_config = original_config.copy()
        
        # Add a test city with extreme values
        test_config["cities"]["TestCity"] = {
            "sigma_multiplier": 2.5,  # Should be capped to 1.50
            "weights": {},
            "biases": {}
        }
        
        # Temporarily write the test config
        with open(config_file, 'w') as f:
            json.dump(test_config, f, indent=2)
        
        # Force reload
        analyzer._city_model_config_cache = None
        
        # Test the cap
        capped_value = analyzer.get_sigma_multiplier("TestCity")
        expected_cap = 1.5
        
        print(f"\nTesting hard cap:")
        print(f"  TestCity with multiplier 2.5 → capped to {capped_value} (expected {expected_cap})")
        assert abs(capped_value - expected_cap) < 0.01, f"Hard cap failed: {capped_value} != {expected_cap}"
        
        print("✅ Hard cap enforcement working!")
        
    finally:
        # Restore original config
        with open(config_file, 'w') as f:
            json.dump(original_config, f, indent=2)
        
        # Force reload
        analyzer._city_model_config_cache = None
    
    print("\n🎉 All sigma multiplier tests passed!")

if __name__ == "__main__":
    test_sigma_caps()