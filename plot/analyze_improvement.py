#!/usr/bin/env python3
"""
Quick analysis script to print performance improvement statistics.
"""

import pandas as pd
import sys
from pathlib import Path

def analyze_improvement(baseline_file, comparison_file):
    """
    Analyze performance improvement between two policies.
    
    Args:
        baseline_file: CSV file with baseline policy (e.g., PS)
        comparison_file: CSV file with comparison policy (e.g., Gittins)
    """
    baseline = pd.read_csv(baseline_file)
    comparison = pd.read_csv(comparison_file)
    
    baseline_name = Path(baseline_file).stem
    comparison_name = Path(comparison_file).stem
    
    print("=" * 80)
    print(f"Performance Comparison: {comparison_name} vs {baseline_name}")
    print("=" * 80)
    
    metrics = [
        'Average Task Latency',
        '25% Task Latency', 
        'Median Task Latency',
        '75% Task Latency',
        '90% Task Latency',
        '95% Task Latency',
        '99% Task Tail Latency',
        '99.9% Task Latency'
    ]
    
    # Check if this is a load sweep or single point
    if len(baseline) > 1:
        print(f"\nLoad Sweep Analysis ({len(baseline)} load points)")
        print("-" * 80)
        
        for i in range(len(baseline)):
            load = baseline.iloc[i]['Load']
            print(f"\n📊 Load = {load:.2f}")
            print(f"{'Metric':<30} {'Baseline':<12} {'Comparison':<12} {'Improvement':<12} {'%'}")
            print("-" * 80)
            
            for metric in metrics:
                base_val = baseline.iloc[i][metric]
                comp_val = comparison.iloc[i][metric]
                improvement = base_val - comp_val
                pct = (improvement / base_val) * 100
                
                symbol = "✅" if pct > 0 else "❌"
                print(f"{metric:<30} {base_val:<12.2f} {comp_val:<12.2f} {improvement:<12.2f} {pct:>6.2f}% {symbol}")
        
        # Summary statistics
        print("\n" + "=" * 80)
        print("Summary Statistics (Across All Loads)")
        print("=" * 80)
        print(f"{'Metric':<30} {'Avg Improvement':<20} {'Best Load':<15}")
        print("-" * 80)
        
        for metric in metrics:
            improvements = ((baseline[metric] - comparison[metric]) / baseline[metric] * 100).values
            avg_improvement = improvements.mean()
            best_load_idx = improvements.argmax()
            best_load = baseline.iloc[best_load_idx]['Load']
            best_improvement = improvements[best_load_idx]
            
            print(f"{metric:<30} {avg_improvement:>6.2f}% (avg)        {best_load:.2f} ({best_improvement:>6.2f}%)")
    
    else:
        # Single load point
        load = baseline.iloc[0]['Load']
        print(f"\nSingle Load Point: {load:.2f}")
        print("-" * 80)
        print(f"{'Metric':<30} {baseline_name:<12} {comparison_name:<12} {'Improvement':<12} {'%'}")
        print("-" * 80)
        
        for metric in metrics:
            base_val = baseline.iloc[0][metric]
            comp_val = comparison.iloc[0][metric]
            improvement = base_val - comp_val
            pct = (improvement / base_val) * 100
            
            symbol = "✅" if pct > 0 else "❌"
            print(f"{metric:<30} {base_val:<12.2f} {comp_val:<12.2f} {improvement:<12.2f} {pct:>6.2f}% {symbol}")
    
    print("\n" + "=" * 80)

def main():
    if len(sys.argv) != 3:
        print("Usage: python analyze_improvement.py <baseline_file> <comparison_file>")
        print("\nExample:")
        print("  python analyze_improvement.py ../ps-bimodal-out ../gittins-bimodal-out")
        sys.exit(1)
    
    baseline_file = sys.argv[1]
    comparison_file = sys.argv[2]
    
    analyze_improvement(baseline_file, comparison_file)

if __name__ == '__main__':
    main()
