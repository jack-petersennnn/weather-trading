#!/usr/bin/env python3
"""
Generate city_model_config.json from backtest results.
Uses CORRECTED MAE (removes systematic bias) for thresholding.

Corrected MAE = sqrt(raw_mae² - bias²)
This represents the unpredictable error after bias correction is applied.
"""

import json
import math
import os
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BACKTEST_FILE = os.path.join(BASE_DIR, "backtest_full_results.json")
OUTPUT_FILE = os.path.join(BASE_DIR, "city_model_config.json")

# Models that can be backtested (have historical forecasts via Open-Meteo)
BACKTEST_MODELS = ['GFS', 'ICON', 'ECMWF', 'Canadian GEM', 'JMA', 'UKMO', 'Meteo-France Arpege']

# Models that share the same underlying forecast (ensembles mirror their parent)
ENSEMBLE_PARENTS = {
    'Ensemble ECMWF': 'ECMWF',
    'Ensemble GFS': 'GFS',
    'Ensemble ICON': 'ICON',
}

# Training-only models (collect data but weight=0, don't use for trading yet)
TRAINING_MODELS = ['HRRR', 'MET Norway', 'Tomorrow.io', 'Visual Crossing',
                   'NWS Forecast', 'NWS Hourly']

# Threshold: models with corrected MAE above this are disabled per city
MAX_CORRECTED_MAE = 2.0


def corrected_mae(raw_mae, bias):
    """Remove systematic bias from MAE to get unpredictable error.
    corrected = sqrt(mae² - bias²)
    If bias is larger than MAE (shouldn't happen but protect against it),
    use a minimum floor.
    """
    if abs(bias) >= raw_mae:
        return raw_mae * 0.3  # Floor: even with perfect bias correction, some noise remains
    return math.sqrt(raw_mae ** 2 - bias ** 2)


def generate():
    with open(BACKTEST_FILE) as f:
        bt = json.load(f)
    
    results = bt['results']
    
    # Build city list from GFS (most complete)
    all_cities = list(results['GFS']['cities'].keys())
    
    config = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "ACIS_365d_corrected_MAE_threshold",
        "max_corrected_mae_threshold": MAX_CORRECTED_MAE,
        "formula": "corrected_mae = sqrt(raw_mae² - bias²)",
        "training_models": TRAINING_MODELS,
        "cities": {},
    }
    
    for city in sorted(all_cities):
        city_config = {
            "weights": {},
            "biases": {},
            "disabled_models": [],
            "corrected_maes": {},
            "raw_maes": {},
        }
        
        enabled_families = set()
        
        for model in BACKTEST_MODELS:
            if model not in results:
                continue
            city_data = results[model].get('cities', {}).get(city)
            if not city_data:
                continue
            
            raw = city_data['mae']
            bias = city_data['bias']
            cmae = corrected_mae(raw, bias)
            
            city_config['corrected_maes'][model] = round(cmae, 2)
            city_config['raw_maes'][model] = raw
            city_config['biases'][model] = bias
            
            if cmae <= MAX_CORRECTED_MAE:
                # Enabled: weight = inverse corrected MAE (lower error = higher weight)
                weight = round(MAX_CORRECTED_MAE / cmae, 3)
                city_config['weights'][model] = weight
                
                # Family tracking
                from fast_scanner import MODEL_FAMILIES
                fam = MODEL_FAMILIES.get(model)
                if fam:
                    enabled_families.add(fam)
            else:
                city_config['weights'][model] = 0.0
                city_config['disabled_models'].append(model)
        
        # Ensemble models: mirror parent's status
        for ens_model, parent in ENSEMBLE_PARENTS.items():
            parent_weight = city_config['weights'].get(parent, 0.0)
            city_config['weights'][ens_model] = parent_weight
            city_config['biases'][ens_model] = city_config['biases'].get(parent, 0.0)
            if parent_weight == 0:
                city_config['disabled_models'].append(ens_model)
        
        # Training models: always weight=0
        for tm in TRAINING_MODELS:
            city_config['weights'][tm] = 0.0
        
        city_config['enabled_families'] = sorted(enabled_families)
        city_config['family_count'] = len(enabled_families)
        
        config['cities'][city] = city_config
    
    # Summary
    tradeable = [c for c, d in config['cities'].items() if d['family_count'] >= 4]
    config['tradeable_cities'] = sorted(tradeable)
    config['tradeable_count'] = len(tradeable)
    
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"Generated city_model_config.json")
    print(f"Threshold: {MAX_CORRECTED_MAE}°F corrected MAE")
    print(f"Tradeable cities ({len(tradeable)}/19): {', '.join(sorted(tradeable))}")
    print()
    
    for city in sorted(config['cities'].keys()):
        d = config['cities'][city]
        status = '✅' if d['family_count'] >= 4 else '❌'
        disabled = d['disabled_models']
        dis_str = f" | disabled: {', '.join(disabled)}" if disabled else ""
        print(f"  {status} {city}: {d['family_count']} families {d['enabled_families']}{dis_str}")


if __name__ == "__main__":
    generate()
