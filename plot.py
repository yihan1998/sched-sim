import pandas as pd
import matplotlib.pyplot as plt

# List of filenames and labels
files = [
    # "global-ddl-aware-out",
    # "jsq-by-capacity-ddl-aware-out",
    # "jsq-ddl-aware-out",
    # "global-local-preempt-out",
    "jsedq-local-preempt-out",
    "jsq-local-preempt-out",
    # "global-no-preempt-out",
    # "jsedq-no-preempt-out",
    # "jsq-no-preempt-out"
]
labels = [
    # "Global DDL Aware",
    # "JSEDQ DDL Aware",
    # "JSQ DDL Aware",
    # "Global Local Preempt",
    "JSEDQ Local Preempt",
    "JSQ Local Preempt",
    # "Global No Preempt",
    # "JSEDQ No Preempt",
    # "JSQ No Preempt"
]

# Initialize a dictionary to store DataFrames
dataframes = {}

# Read each file into a DataFrame
for file, label in zip(files, labels):
    df = pd.read_csv(file)
    df = df.sort_values(by="Load")
    dataframes[label] = df

# # Print Load and Latency Columns
# for label, df in dataframes.items():
#     print(f"Dataset: {label}")
#     print(df[["Load", "Average Job Latency", "25% Job Latency", "Median Job Latency", 
#               "75% Job Latency", "90% Job Latency", "95% Job Latency", 
#               "99% Job Latency", "99.9% Job Latency"]])
#     print("\n")

# Plot average latency
plt.figure(figsize=(12, 8))
for label, df in dataframes.items():
    plt.plot(df["Real Load"], df["Average Job Latency"], marker='o', label=label)
plt.ylim(bottom=0)
plt.xlabel("Real Load")
plt.ylabel("Average Latency")
plt.title("Real Load vs Average Latency")
plt.legend()
plt.grid()
plt.tight_layout()
plt.savefig("average_latency.png", dpi=500)

# Plot median latency
plt.figure(figsize=(12, 8))
for label, df in dataframes.items():
    plt.plot(df["Real Load"], df["Median Job Latency"], marker='o', label=label)
plt.ylim(bottom=0)
plt.xlabel("Real Load")
plt.ylabel("Median Latency")
plt.title("Real Load vs Median Latency")
plt.legend()
plt.grid()
plt.tight_layout()
plt.savefig("median_latency.png", dpi=500)

# Plot 90th percentile latency
plt.figure(figsize=(12, 8))
for label, df in dataframes.items():
    plt.plot(df["Real Load"], df["90% Job Latency"], marker='o', label=label)
plt.ylim(bottom=0)
plt.xlabel("Real Load")
plt.ylabel("90th Percentile Latency")
plt.title("Real Load vs 90th Percentile Latency")
plt.legend()
plt.grid()
plt.tight_layout()
plt.savefig("p90_latency.png", dpi=500)

# Plot 95th percentile latency
plt.figure(figsize=(12, 8))
for label, df in dataframes.items():
    plt.plot(df["Real Load"], df["95% Job Latency"], marker='o', label=label)
plt.ylim(bottom=0)
plt.xlabel("Real Load")
plt.ylabel("95th Percentile Latency")
plt.title("Real Load vs 95th Percentile Latency")
plt.legend()
plt.grid()
plt.tight_layout()
plt.savefig("p95_latency.png", dpi=500)

# Plot 99th percentile latency
plt.figure(figsize=(12, 8))
for label, df in dataframes.items():
    plt.plot(df["Real Load"], df["99% Job Latency"], marker='o', label=label)
plt.ylim(bottom=0, top=20000)
plt.xlabel("Real Load")
plt.ylabel("99th Percentile Latency")
plt.title("Real Load vs 99th Percentile Latency")
plt.legend()
plt.grid()
plt.tight_layout()
plt.savefig("p99_latency.png", dpi=500)
