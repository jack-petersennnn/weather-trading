#!/usr/bin/env python3
"""
Trade-Level Counterfactual Analysis

Load actual trade history from trades.json and compute:
- Raw p_model at decision time
- Sigma-corrected p_model (using optimized multipliers)
- Market price / fill price
- Net EV under raw vs corrected
- Actual outcome

Report whether sigma corrections would have improved trade selection.
Flag results as having ±1-2°F noise from run-timing offsets.
"""

import json
import statistics
from datetime import datetime
from collections import defaultdict
import os

# Import probability calculation functions
import sys
sys.path.append('/home/ubuntu/.openclaw/workspace/weather-trading')
from analyzer import compute_probability

def load_trade_data():
    """Load trade history and optimization results."""
    print("Loading trade data and optimization results...")
    
    # Load actual trades
    try:
        with open("/home/ubuntu/.openclaw/workspace/weather-trading/trades.json") as f:
            trades_json = json.load(f)
            trades_data = trades_json.get("trades", [])
    except Exception as e:
        print(f"Error loading trades: {e}")
        return None, None
    
    # Load sigma optimization results
    optimized_multipliers = None
    try:
        with open("/home/ubuntu/.openclaw/workspace/weather-trading/sigma_optimization_results.json") as f:
            opt_results = json.load(f)
            optimized_multipliers = opt_results.get("recommended_multipliers", {})
            print(f"Loaded optimized multipliers: {optimized_multipliers}")
    except Exception as e:
        print(f"Warning: Could not load optimization results: {e}")
        print("Will compare against baseline multipliers only")
    
    return trades_data, optimized_multipliers

def parse_trade_record(trade):
    """Extract relevant information from a trade record."""
    try:
        # Extract basic trade info based on actual structure
        result = {
            'trade_id': trade.get('ticker', 'unknown'),
            'timestamp': trade.get('timestamp', ''),
            'market_ticker': trade.get('ticker', ''),
            'side': 'yes' if trade.get('direction', '').lower() == 'yes' else 'no',
            'fill_price': trade.get('entry_price_cents', 0) / 100.0,  # Convert cents to dollars
            'quantity': trade.get('contracts', 0),
            'city': trade.get('city', ''),
            'outcome': trade.get('result', None),  # 'win', 'loss', or 'unknown'
            'realized_pnl': trade.get('realized_pnl_cents', 0) / 100.0,  # Convert cents to dollars
        }
        
        # Extract forecast conditions at entry time
        result.update({
            'ensemble_mean': trade.get('entry_forecast_high', 0),  # This appears to be the forecast
            'ensemble_std': trade.get('entry_forecast_std', 0),
            'calibrated_std': trade.get('entry_forecast_std', 0),  # Same as ensemble_std for now
            'raw_p_model': trade.get('our_prob', 0),
            'market_prob': trade.get('market_price_at_entry', 0),
            'edge': trade.get('edge', 0),
            'source_spread': trade.get('entry_source_spread', 0)
        })
        
        # Try to extract threshold temperature from ticker
        ticker = trade.get('ticker', '')
        threshold_temp = None
        market_type = 'threshold_yes'  # Default assumption
        
        # Parse threshold from ticker (e.g., "KXHIGHTLV-26FEB24-B75.5" -> 75.5)
        if 'B' in ticker:
            parts = ticker.split('-')
            if len(parts) >= 3:
                threshold_part = parts[-1]
                if threshold_part.startswith('B'):
                    try:
                        threshold_temp = float(threshold_part[1:])
                    except:
                        threshold_temp = 0
        
        # Determine market type based on direction
        if trade.get('direction', '').upper() == 'NO':
            market_type = 'threshold_no'
        
        result['threshold_temp'] = threshold_temp or 0
        result['market_type'] = market_type
        
        return result
        
    except Exception as e:
        print(f"Error parsing trade: {e}")
        return None

def compute_corrected_probability(trade_data, optimized_multipliers):
    """Recompute probability using optimized sigma multiplier."""
    city = trade_data['city']
    ensemble_mean = trade_data['ensemble_mean']
    ensemble_std = trade_data['ensemble_std']
    threshold_temp = trade_data['threshold_temp']
    market_type = trade_data['market_type']
    
    if not all([city, ensemble_mean, ensemble_std, threshold_temp]):
        return None
    
    # Get raw std (before city multiplier)
    # If we have calibrated_std, back-calculate raw_std
    calibrated_std = trade_data.get('calibrated_std', ensemble_std)
    
    # Estimate original multiplier (this is approximate)
    original_multiplier = calibrated_std / ensemble_std if ensemble_std > 0 else 1.0
    raw_std = ensemble_std  # This should be the raw ensemble std
    
    # Apply optimized multiplier
    if city in optimized_multipliers:
        optimized_multiplier = optimized_multipliers[city]
        corrected_std = raw_std * optimized_multiplier
    else:
        corrected_std = calibrated_std  # No change if city not found
    
    # Recompute probability based on market type
    if market_type in ['threshold_yes', 'threshold']:
        # P(temp >= threshold)
        corrected_prob = compute_probability(ensemble_mean, corrected_std, threshold_temp - 1, None)
    elif market_type == 'threshold_no':
        # P(temp < threshold)
        corrected_prob = compute_probability(ensemble_mean, corrected_std, None, threshold_temp - 1)
    else:
        # Unknown market type, return None
        corrected_prob = None
    
    return corrected_prob, corrected_std, original_multiplier

def compute_expected_value(p_model, market_price, side, quantity):
    """Compute expected value of a trade."""
    if side.lower() == 'yes':
        # Buying YES at market_price
        prob_win = p_model
        payout_if_win = (1 - market_price) * quantity
        cost = market_price * quantity
        ev = prob_win * payout_if_win - (1 - prob_win) * cost
    elif side.lower() == 'no':
        # Buying NO at market_price
        prob_win = 1 - p_model
        payout_if_win = market_price * quantity
        cost = (1 - market_price) * quantity
        ev = prob_win * payout_if_win - (1 - prob_win) * cost
    else:
        ev = 0
    
    return ev

def analyze_trades(trades_data, optimized_multipliers):
    """Analyze each trade with raw vs corrected probabilities."""
    print(f"\nAnalyzing {len(trades_data)} trades...")
    
    analysis_results = []
    
    processed = 0
    skipped = 0
    
    for trade in trades_data:
        trade_data = parse_trade_record(trade)
        
        if not trade_data or not trade_data['raw_p_model']:
            skipped += 1
            continue
        
        # Compute corrected probability if optimized multipliers available
        corrected_analysis = None
        if optimized_multipliers:
            result = compute_corrected_probability(trade_data, optimized_multipliers)
            if result:
                corrected_prob, corrected_std, original_multiplier = result
                corrected_analysis = {
                    'corrected_probability': corrected_prob,
                    'corrected_std': corrected_std,
                    'original_multiplier': original_multiplier,
                    'optimized_multiplier': optimized_multipliers.get(trade_data['city'], 1.0)
                }
        
        # Compute expected values
        raw_ev = compute_expected_value(
            trade_data['raw_p_model'],
            trade_data['fill_price'],
            trade_data['side'],
            trade_data['quantity']
        )
        
        corrected_ev = None
        if corrected_analysis:
            corrected_ev = compute_expected_value(
                corrected_analysis['corrected_probability'],
                trade_data['fill_price'],
                trade_data['side'],
                trade_data['quantity']
            )
        
        # Compile analysis
        analysis = {
            'trade_data': trade_data,
            'raw_expected_value': raw_ev,
            'corrected_expected_value': corrected_ev,
            'ev_improvement': corrected_ev - raw_ev if corrected_ev is not None else None,
            'corrected_analysis': corrected_analysis,
            'should_have_traded_raw': raw_ev > 0,
            'should_have_traded_corrected': corrected_ev > 0 if corrected_ev is not None else None,
            'trade_decision_change': None
        }
        
        # Determine if trade decision would change
        if corrected_ev is not None:
            raw_trade = raw_ev > 0
            corrected_trade = corrected_ev > 0
            
            if raw_trade != corrected_trade:
                if corrected_trade and not raw_trade:
                    analysis['trade_decision_change'] = 'should_trade_more'
                elif not corrected_trade and raw_trade:
                    analysis['trade_decision_change'] = 'should_trade_less'
        
        analysis_results.append(analysis)
        processed += 1
    
    print(f"Processed {processed} trades, skipped {skipped}")
    return analysis_results

def generate_counterfactual_report(analysis_results):
    """Generate comprehensive report on counterfactual analysis."""
    print("\n" + "="*80)
    print("TRADE-LEVEL COUNTERFACTUAL ANALYSIS REPORT")
    print("="*80)
    
    if not analysis_results:
        print("No trade data available for analysis")
        return
    
    # Overall statistics
    total_trades = len(analysis_results)
    trades_with_correction = sum(1 for a in analysis_results if a['corrected_expected_value'] is not None)
    
    print(f"\nData Summary:")
    print(f"  Total trades analyzed: {total_trades}")
    print(f"  Trades with corrections: {trades_with_correction}")
    
    if trades_with_correction == 0:
        print("No corrected probabilities available - optimization results missing")
        return
    
    # EV Analysis
    raw_evs = [a['raw_expected_value'] for a in analysis_results]
    corrected_evs = [a['corrected_expected_value'] for a in analysis_results if a['corrected_expected_value'] is not None]
    ev_improvements = [a['ev_improvement'] for a in analysis_results if a['ev_improvement'] is not None]
    
    print(f"\nExpected Value Analysis:")
    print(f"  Raw EV average: ${statistics.mean(raw_evs):.2f}")
    print(f"  Corrected EV average: ${statistics.mean(corrected_evs):.2f}")
    print(f"  Average improvement: ${statistics.mean(ev_improvements):.2f}")
    
    # Count positive improvements
    positive_improvements = sum(1 for ev in ev_improvements if ev > 0)
    negative_improvements = sum(1 for ev in ev_improvements if ev < 0)
    neutral_improvements = len(ev_improvements) - positive_improvements - negative_improvements
    
    print(f"  Trades with EV improvement: {positive_improvements} ({positive_improvements/len(ev_improvements)*100:.1f}%)")
    print(f"  Trades with EV degradation: {negative_improvements} ({negative_improvements/len(ev_improvements)*100:.1f}%)")
    
    # Trade Decision Changes
    decision_changes = {}
    for analysis in analysis_results:
        change = analysis.get('trade_decision_change')
        if change:
            decision_changes[change] = decision_changes.get(change, 0) + 1
    
    if decision_changes:
        print(f"\nTrade Decision Changes:")
        for change_type, count in decision_changes.items():
            print(f"  {change_type.replace('_', ' ').title()}: {count} trades")
    
    # City-specific analysis
    city_analysis = defaultdict(lambda: {'raw_ev': [], 'corrected_ev': [], 'improvements': []})
    
    for analysis in analysis_results:
        city = analysis['trade_data']['city']
        city_analysis[city]['raw_ev'].append(analysis['raw_expected_value'])
        
        if analysis['corrected_expected_value'] is not None:
            city_analysis[city]['corrected_ev'].append(analysis['corrected_expected_value'])
            city_analysis[city]['improvements'].append(analysis['ev_improvement'])
    
    print(f"\nPer-City Analysis:")
    print("-" * 50)
    
    for city, data in city_analysis.items():
        if len(data['improvements']) == 0:
            continue
        
        avg_raw = statistics.mean(data['raw_ev'])
        avg_corrected = statistics.mean(data['corrected_ev'])
        avg_improvement = statistics.mean(data['improvements'])
        improvement_pct = (avg_improvement / abs(avg_raw)) * 100 if avg_raw != 0 else 0
        
        print(f"  {city:12}: Raw EV ${avg_raw:+7.2f}, Corrected EV ${avg_corrected:+7.2f}, Δ ${avg_improvement:+6.2f} ({improvement_pct:+.1f}%)")
    
    # Outcome analysis (if available)
    settled_trades = [a for a in analysis_results if a['trade_data']['outcome'] is not None]
    
    if settled_trades:
        print(f"\nOutcome Analysis ({len(settled_trades)} settled trades):")
        
        # Compare predictions to actual outcomes
        raw_correct = 0
        corrected_correct = 0
        
        for analysis in settled_trades:
            trade_data = analysis['trade_data']
            actual_outcome = trade_data['outcome']
            
            # Convert probabilities to binary predictions (>50% = True)
            raw_prediction = analysis['raw_expected_value'] > 0
            
            if analysis['corrected_expected_value'] is not None:
                corrected_prediction = analysis['corrected_expected_value'] > 0
                
                # Check correctness (assuming positive EV aligns with actual outcome)
                if trade_data['side'].lower() == 'yes':
                    if (raw_prediction and actual_outcome) or (not raw_prediction and not actual_outcome):
                        raw_correct += 1
                    if (corrected_prediction and actual_outcome) or (not corrected_prediction and not actual_outcome):
                        corrected_correct += 1
        
        if len(settled_trades) > 0:
            raw_accuracy = raw_correct / len(settled_trades) * 100
            corrected_accuracy = corrected_correct / len(settled_trades) * 100
            
            print(f"  Raw model 'accuracy': {raw_accuracy:.1f}%")
            print(f"  Corrected model 'accuracy': {corrected_accuracy:.1f}%")
    
    # Risk Assessment
    print(f"\n" + "="*80)
    print("RISK ASSESSMENT & CAVEATS")
    print("="*80)
    
    print("⚠️  IMPORTANT LIMITATIONS:")
    print("• Results have ±1-2°F noise from run-timing offsets")
    print("• Only trust CLEAR improvements, not marginal ones")
    print("• Sigma corrections affect spread, not bias")
    print("• Trade timing may not match forecast timing exactly")
    print("• Market conditions may have changed between forecast and trade")
    
    # Significance assessment
    significant_improvements = sum(1 for ev in ev_improvements if ev > 2.0)  # $2+ improvement
    significant_degradations = sum(1 for ev in ev_improvements if ev < -2.0)  # $2+ degradation
    
    print(f"\nSignificance Assessment:")
    print(f"• Trades with significant improvement (>$2): {significant_improvements}")
    print(f"• Trades with significant degradation (>$2): {significant_degradations}")
    
    if statistics.mean(ev_improvements) > 1.0:
        print("✅ CLEAR POSITIVE SIGNAL: Sigma corrections would improve trade selection")
    elif statistics.mean(ev_improvements) < -1.0:
        print("❌ CLEAR NEGATIVE SIGNAL: Sigma corrections would worsen trade selection")
    else:
        print("🟡 UNCLEAR SIGNAL: Improvements are marginal, within noise bounds")
    
    return {
        'total_trades': total_trades,
        'trades_with_correction': trades_with_correction,
        'average_ev_improvement': statistics.mean(ev_improvements) if ev_improvements else 0,
        'positive_improvements': positive_improvements,
        'negative_improvements': negative_improvements,
        'significant_improvements': significant_improvements,
        'significant_degradations': significant_degradations,
        'city_analysis': dict(city_analysis)
    }

def main():
    print("TRADE-LEVEL COUNTERFACTUAL ANALYSIS")
    print("="*80)
    print("Analyzing historical trades with raw vs sigma-corrected probabilities")
    
    # Load data
    trades_data, optimized_multipliers = load_trade_data()
    
    if not trades_data:
        print("ERROR: Could not load trade data")
        return
    
    # Run analysis
    analysis_results = analyze_trades(trades_data, optimized_multipliers)
    
    if not analysis_results:
        print("ERROR: No trades to analyze")
        return
    
    # Generate report
    summary = generate_counterfactual_report(analysis_results)
    
    # Save results
    output_file = "/home/ubuntu/.openclaw/workspace/weather-trading/trade_counterfactual_results.json"
    results_data = {
        'analysis_results': analysis_results,
        'summary': summary,
        'optimized_multipliers_used': optimized_multipliers,
        'analysis_timestamp': datetime.now().isoformat()
    }
    
    with open(output_file, 'w') as f:
        json.dump(results_data, f, indent=2, default=str)
    
    print(f"\n📊 Detailed results saved to: {output_file}")

if __name__ == "__main__":
    main()