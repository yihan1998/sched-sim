# Plotting Scripts for Simulation Results

This directory contains scripts to visualize scheduling simulation results.

## Main Script: `plot_results.py`

### Features

The script generates three types of plots:

1. **Latency Comparison**: Compare latency metrics across policies
   - Bar charts (single load point)
   - Line charts (load sweep)

2. **Percentile Comparison**: Distribution of latencies across percentiles
   - Shows 25th, 50th, 75th, 90th, 95th, 99th, 99.9th percentiles
   - Log scale for better visibility

3. **Improvement Heatmap**: Visual heatmap showing % improvement
   - Generated when comparing exactly 2 policies
   - Shows which metrics/loads benefit most

### Usage

#### Compare specific files:
```bash
cd plot/
python3 plot_results.py ../gittins-bimodal ../ps-bimodal
```

#### Auto-discover files in parent directory:
```bash
cd plot/
python3 plot_results.py --dir ../
```

#### Compare multiple policies:
```bash
python3 plot_results.py ../gittins-bimodal ../ps-bimodal ../srjf-bimodal
```

### Input Format

The script expects CSV files with the following columns:
- `Workers`: Number of workers
- `Sim Duration`: Simulation duration
- `Load`: System load (0.0 to 1.0)
- `Real Load`: Actual measured load
- `Average Task Latency`
- `25% Task Latency`
- `Median Task Latency`
- `75% Task Latency`
- `90% Task Latency`
- `95% Task Latency`
- `99% Task Tail Latency`
- `99.9% Task Latency`

### Output

All plots are saved to `plots/` subdirectory:
- `latency_comparison.png`: Main comparison chart
- `percentile_comparison.png`: Percentile distribution
- `improvement_heatmap.png`: Heatmap (2 policies only)

### Examples

#### Single Load Point
```bash
# Compare Gittins vs PS at load=0.9
python3 plot_results.py ../gittins-bimodal ../ps-bimodal
```

#### Load Sweep
```bash
# Compare across multiple load values (0.1 to 1.0)
python3 plot_results.py ../gittins-bimodal-out ../ps-bimodal-out
```

### Requirements

```bash
pip install pandas matplotlib numpy
```

### Tips

- File names are used as labels in plots (e.g., `gittins-bimodal` → "gittins-bimodal")
- For load sweeps, ensure all CSV files have matching load values
- The script automatically detects single-point vs load-sweep data
- Use meaningful filenames for clearer plot legends

## Other Scripts

### `plot_distribution.py` (TODO)
Plot service time distributions used in simulations.

### `plot_convergence.py` (TODO)
Check if simulation has run long enough for stable results.

## Example Workflow

```bash
# Run simulations (from parent directory)
cd ..
python3 sim/simulation.py configs/config.json

# Generate plots
cd plot/
python3 plot_results.py --dir ../

# View plots
open plots/latency_comparison.png
open plots/percentile_comparison.png
open plots/improvement_heatmap.png
```
