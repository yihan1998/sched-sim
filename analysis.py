import numpy as np
import sys
import os
import json

RESULTS_DIR_NAME = "results/"
RESULTS_SUBDIR_NAME = RESULTS_DIR_NAME + "sim_{}/"
THREAD_RUN_FORMAT = "{}_t{}"
TASK_FILE_NAME = "task_times.csv"
JOB_FILE_NAME = "job_times.csv"
META_FILE_NAME = "meta.json"
STATS_FILE_NAME = "stats.json"
WORKER_FILE_NAME = "worker_usage.csv"
CSV_HEADER = "Workers,Sim Duration,Load,Real Load,Average Task Latency,25% Task Latency,Median Task Latency,75% Task Latency,90% Task Latency,95% Task Latency,99% Task Tail Latency,99.9% Task Latency,Average Job Latency,25% Job Latency,Median Job Latency,75% Job Latency,90% Job Latency,95% Job Latency,99% Job Latency,99.9% Job Latency"


def analyze_sim_run(run_name, output_file, print_results=False, time_dropped=0):
    task_file = open(RESULTS_SUBDIR_NAME.format(run_name) + TASK_FILE_NAME, "r")
    job_file = open(RESULTS_SUBDIR_NAME.format(run_name) + JOB_FILE_NAME, "r")
    meta_file = open(RESULTS_SUBDIR_NAME.format(run_name) + META_FILE_NAME, "r")
    stats_file = open(RESULTS_SUBDIR_NAME.format(run_name) + STATS_FILE_NAME, "r")
    worker_file = open(RESULTS_SUBDIR_NAME.format(run_name) + WORKER_FILE_NAME, "r")

    meta_data = json.load(meta_file)
    stats = json.load(stats_file)

    meta_file.close()
    stats_file.close()

    # Worker Stats
    busy_time = 0
    task_time = 0

    next(worker_file) # skip first line
    for line in worker_file:
        data = line.strip().split(",")
        busy_time += float(data[1])
        task_time += float(data[2])
    worker_file.close()

    task_latencies = []
    job_latencies = []

    complete_tasks = stats["Completed Tasks"]

    total_tasks = 0

    next(task_file) # skip first line
    for line in task_file:
        data = line.split(",")
        if float(data[1]) > time_dropped * stats["End Time"] and float(data[2]) >= 0:
            total_tasks += 1
            task_latencies.append(float(data[2]))
    task_percentiles = np.percentile(task_latencies, [25, 50, 75, 90, 95, 99, 99.9])
    task_average_latency = np.mean(task_latencies)
    task_min_latency = np.min(task_latencies)
    task_max_latency = np.max(task_latencies)

    next(job_file) # skip first line
    for line in job_file:
        data = line.split(",")
        if float(data[1]) > time_dropped * stats["End Time"] and float(data[2]) >= 0:
            total_tasks += 1
            job_latencies.append(float(data[2]))
    job_percentiles = np.percentile(job_latencies, [25, 50, 75, 90, 95, 99, 99.9])
    job_average_latency = np.mean(job_latencies)
    job_min_latency = np.min(job_latencies)
    job_max_latency = np.max(job_latencies)

    workers = meta_data["num_workers"]
    avg_load = (busy_time / (workers * stats["End Time"]))

    data_string = "{},{},{:.2f},{:.2f}," \
                "{:.2f},{:.2f},{:.2f},{:.2f},{:.2f},{:.2f},{:.2f},{:.2f}," \
                "{:.2f},{:.2f},{:.2f},{:.2f},{:.2f},{:.2f},{:.2f},{:.2f}".format(
        meta_data["num_workers"], meta_data["sim_duration"], meta_data["avg_system_load"], avg_load * 100, 
        task_average_latency, task_percentiles[0], task_percentiles[1], task_percentiles[2], task_percentiles[3], task_percentiles[4], task_percentiles[5], task_percentiles[6],
        job_average_latency, job_percentiles[0], job_percentiles[1], job_percentiles[2], job_percentiles[3], job_percentiles[4], job_percentiles[5],  task_percentiles[6])
    output_file.write(data_string + "\n")

def main():
    if len(sys.argv) != 4:
        print("Invalid number of arguments.")
        exit(0)

    output_file_name = sys.argv[-2]
    output_file = open(output_file_name, "w")
    output_file.write(CSV_HEADER + "\n")

    sim_list = []
    name = sys.argv[1].strip()

    # File with list of sim names
    if os.path.isfile("./" + name):
        sim_list_file = open(name)
        sim_list = sim_list_file.readlines()
        sim_list_file.close()

    # Name of one run
    elif os.path.isdir(RESULTS_SUBDIR_NAME.format(name)):
        sim_list.append(name)
    else:
        print("File or directory not found")

    for sim_name in sim_list:
        analyze_sim_run(sim_name.strip(), output_file, time_dropped=int(sys.argv[-1])/100)
        print("Simulation {} analysis complete".format(sim_name))

    output_file.close()

if __name__ == "__main__":
    main()