#!/usr/bin/env python3
"""
Plot simulation results comparing different scheduling policies.
Generates elbow curves (load vs latency) and improvement tables.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
import os
from pathlib import Path

def read_results(filepath):
    """Read simulation results from CSV file."""
    try:
        # Try reading as CSV
        df = pd.read_csv(filepath)
        
        # Verify required columns exist
        required_cols = ['Load', 'Average Task Latency', 'Median Task Latency']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"Warning: {filepath} missing columns: {missing_cols}")
            print(f"Available columns: {list(df.columns)}")
            return None
        
        return df
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None

def plot_elbow_curves(data_dict, output_file='elbow_curves.png'):
    """
    Plot elbow curves showing load vs latency for key metrics.
    
    Args:
        data_dict: Dictionary mapping policy names to DataFrames
        output_file: Output filename for the plot
    """
    # Key metrics to plot
    metrics = [
        'Average Task Latency',
        'Median Task Latency', 
        '90% Task Latency',
        '95% Task Latency',
        '99% Task Tail Latency',
        # '99.9% Task Latency'
    ]
    
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    axes = axes.flatten()
    
    colors = plt.cm.Set2(np.linspace(0, 1, len(data_dict)))
    markers = ['o', 's', '^', 'D', 'v', '<', '>', 'p', '*']
    
    for idx, metric in enumerate(metrics):
        ax = axes[idx]
        
        # Collect all values for this metric to determine y-axis limits
        all_values = []
        for i, (policy, df) in enumerate(data_dict.items()):
            loads = df['Load'].values
            values = df[metric].values
            all_values.extend(values)
            
            ax.plot(loads, values, label=policy, marker=markers[i % len(markers)], 
                   linewidth=2.5, markersize=7, color=colors[i], alpha=0.8)
        
        ax.set_xlabel('System Load', fontsize=12, fontweight='bold')
        ax.set_ylabel('Latency (time units)', fontsize=12, fontweight='bold')
        ax.set_title(metric, fontsize=13, fontweight='bold')
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.legend(fontsize=10, loc='best')
        
        # Set axis limits
        ax.set_xlim(left=0)
        
        # Set y-axis limits with some padding
        if len(all_values) > 0:
            y_min = min(all_values)
            y_max = max(all_values)
            
            # Use log scale if values span multiple orders of magnitude
            if y_max / max(y_min, 1) > 100:
                ax.set_yscale('log')
                # For log scale, use the actual min/max
                ax.set_ylim(bottom=0, top=5000)
            else:
                # For linear scale, add padding
                y_range = y_max - y_min
                padding = y_range * 0.1
                ax.set_ylim(bottom=0, top=5000)
    
    plt.suptitle('Elbow Curves: Load vs Latency', fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout()
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"✅ Saved elbow curves to {output_file}")

def print_improvement_table(baseline_df, comparison_df, baseline_name, comparison_name):
    """
    Print numerical improvement table comparing two policies.
    
    Args:
        baseline_df: DataFrame with baseline policy results
        comparison_df: DataFrame with comparison policy results
        baseline_name: Name of baseline policy
        comparison_name: Name of comparison policy
    """
    metrics = [
        'Average Task Latency',
        'Median Task Latency',
        '90% Task Latency',
        '95% Task Latency',
        '99% Task Tail Latency',
        '99.9% Task Latency'
    ]
    
    print("\n" + "=" * 120)
    print(f"PERFORMANCE IMPROVEMENT: {comparison_name} vs {baseline_name}")
    print("=" * 120)
    
    # Header
    print(f"\n{'Load':<8}", end='')
    for metric in metrics:
        short_name = metric.replace(' Task Latency', '').replace('Task Tail Latency', 'Tail')
        print(f"{short_name:<20}", end='')
    print()
    
    print(f"{'':8}", end='')
    for _ in metrics:
        print(f"{'Base':<8}{'Comp':<8}{'Δ%':<4}", end='')
    print()
    print("-" * 120)
    
    # Data rows
    for i in range(len(baseline_df)):
        load = baseline_df.iloc[i]['Load']
        print(f"{load:<8.2f}", end='')
        
        for metric in metrics:
            base_val = baseline_df.iloc[i][metric]
            comp_val = comparison_df.iloc[i][metric]
            improvement = ((base_val - comp_val) / base_val) * 100
            
            print(f"{base_val:<8.0f}{comp_val:<8.0f}{improvement:>+3.0f}%  ", end='')
        print()
    
    # Summary row
    print("-" * 120)
    print(f"{'AVG':<8}", end='')
    
    for metric in metrics:
        base_vals = baseline_df[metric].values
        comp_vals = comparison_df[metric].values
        improvements = ((base_vals - comp_vals) / base_vals) * 100
        avg_improvement = improvements.mean()
        
        avg_base = base_vals.mean()
        avg_comp = comp_vals.mean()
        
        print(f"{avg_base:<8.0f}{avg_comp:<8.0f}{avg_improvement:>+3.0f}%  ", end='')
    print()
    
    print("=" * 120)
    
    # Best improvements summary
    print(f"\n🎯 KEY HIGHLIGHTS:")
    print("-" * 60)
    
    for metric in metrics:
        base_vals = baseline_df[metric].values
        comp_vals = comparison_df[metric].values
        improvements = ((base_vals - comp_vals) / base_vals) * 100
        
        avg_improvement = improvements.mean()
        max_improvement = improvements.max()
        max_idx = improvements.argmax()
        max_load = baseline_df.iloc[max_idx]['Load']
        
        symbol = "✅" if avg_improvement > 0 else "❌"
        print(f"{symbol} {metric:<30}: {avg_improvement:>+6.2f}% avg  |  {max_improvement:>+6.2f}% best @ load={max_load:.2f}")
    
    print("=" * 120 + "\n")

def main():
    """Main function to generate plots and tables."""
    if len(sys.argv) < 2:
        print("Usage: python plot_results.py <file1> <file2> [file3] ...")
        print("   or: python plot_results.py --dir <directory>")
        print("\nExample:")
        print("  python plot_results.py ../ps-bimodal-out ../gittins-bimodal-out")
        print("  python plot_results.py --dir ../")
        sys.exit(1)
    
    # Determine input mode
    if sys.argv[1] == '--dir':
        # Read all files from directory
        directory = sys.argv[2] if len(sys.argv) > 2 else '../'
        files = sorted(Path(directory).glob('*-out'))
        if not files:
            files = sorted(Path(directory).glob('*-bimodal'))
            files.extend(sorted(Path(directory).glob('*-normal')))
            files.extend(sorted(Path(directory).glob('*-pareto')))
        filepaths = [str(f) for f in files]
    else:
        # Read specified files
        filepaths = sys.argv[1:]
    
    if not filepaths:
        print("No files found!")
        sys.exit(1)
    
    print(f"📊 Reading {len(filepaths)} files...")
    
    # Read all data
    data_dict = {}
    for filepath in filepaths:
        df = read_results(filepath)
        if df is not None and len(df) > 0:
            # Sort by Load to ensure proper line plotting
            df = df.sort_values('Load').reset_index(drop=True)
            
            # Extract policy name from filename
            policy_name = Path(filepath).stem
            data_dict[policy_name] = df
            try:
                load_min = df['Load'].min()
                load_max = df['Load'].max()
                print(f"  ✓ Loaded {policy_name}: {len(df)} rows, load range [{load_min:.2f}, {load_max:.2f}]")
            except Exception as e:
                print(f"  ✓ Loaded {policy_name}: {len(df)} rows (error getting load range: {e})")
        else:
            print(f"  ✗ Failed to load {filepath}")
    
    if not data_dict:
        print("❌ No valid data loaded!")
        sys.exit(1)
    
    # Create plots directory if it doesn't exist
    os.makedirs('plots', exist_ok=True)
    
    # Generate elbow curves
    print("\n📈 Generating elbow curves...")
    plot_elbow_curves(data_dict, 'plots/elbow_curves.png')
    
    # Print improvement tables for all pairwise comparisons
    if len(data_dict) >= 2:
        policies = list(data_dict.keys())
        
        # Find baseline (typically PS)
        baseline_name = None
        for policy in policies:
            if 'ps' in policy.lower() and 'gittins' not in policy.lower():
                baseline_name = policy
                break
        
        if baseline_name is None:
            baseline_name = policies[0]
        
        # Compare each policy against baseline
        for policy in policies:
            if policy != baseline_name:
                # Sort both dataframes by Load for proper comparison
                baseline_sorted = data_dict[baseline_name].sort_values('Load').reset_index(drop=True)
                comparison_sorted = data_dict[policy].sort_values('Load').reset_index(drop=True)
                
                print_improvement_table(
                    baseline_sorted,
                    comparison_sorted,
                    baseline_name,
                    policy
                )
    
    print("\n✅ All analysis complete!")
    print(f"📁 Plots saved to: plots/elbow_curves.png")

if __name__ == '__main__':
    main()
