#!/usr/bin/env python
"""Creates a runs a simulation."""

import logging
import random
import os
import json
import math
import sys
import datetime
import pathlib

from simulation_state import SimulationState
from worker import Worker
from task import Task
import progress_bar as progress
from sim_config import SimConfig

SINGLE_THREAD_SIM_NAME_FORMAT = "{}_{}"
MULTI_THREAD_SIM_NAME_FORMAT = "{}_{}_t{}"
RESULTS_DIR = "{}/results/"
META_LOG_FILE = "{}/results/meta_log"
CONFIG_LOG_DIR = "{}/config_records/"


class Simulation:
    """Runs the simulation based on the simulation state."""
    def __init__(self, configuration, sim_dir_path):
        self.config = configuration
        self.state = SimulationState(configuration)
        self.sim_dir_path = sim_dir_path

    def run(self):
        """Run the simulation."""

        # Initialize data
        self.state.initialize_state(self.config)

        # A short duration may result in no tasks
        self.state.tasks_scheduled = len(self.state.tasks)
        if self.state.tasks_scheduled == 0:
            return
        
        # Start at first time stamp with an arrival
        task_number = 0
        self.state.timer.increment(self.state.tasks[0].arrival_time)

        if self.config.progress_bar:
            print("\nSimulation started")

        # Run for acceptable time or until all tasks are done
        while self.state.any_incomplete() and \
                (self.config.sim_duration is None or self.state.timer.get_time() < self.config.sim_duration):

            # Put new task arrivals in queues
            while task_number < self.state.tasks_scheduled and \
                    self.state.tasks[task_number].arrival_time <= self.state.timer.get_time():

                logging.debug("[ARRIVAL]: {}".format(self.state.tasks[task_number]))

                task = self.state.tasks[task_number]
                task.process(time_increment=0)
                task_number += 1

            # Schedule threads
            for worker in self.state.workers:
                worker.schedule()

            # Move forward in time
            self.state.timer.increment(1)

            # Print progress bar
            if self.config.progress_bar and self.state.timer.get_time() % 10000 == 0:
                progress.print_progress(self.state.timer.get_time(), self.config.sim_duration, length=50, decimals=3)

        self.state.add_final_stats()

    def save_stats(self):
        """Save simulation date to file."""
        # Make files and directories
        new_dir_name = RESULTS_DIR.format(self.sim_dir_path) + "sim_{}/".format(self.config.name)
        os.makedirs(os.path.dirname(new_dir_name))
        worker_file = open("{}worker_usage.csv".format(new_dir_name, self.config.name), "w")
        task_file = open("{}task_times.csv".format(new_dir_name, self.config.name), "w")
        meta_file = open("{}meta.json".format(new_dir_name), "w")
        stats_file = open("{}stats.json".format(new_dir_name), "w")

        # Write worker information
        worker_file.write(','.join(Worker.get_stat_headers(self.config)) + "\n")
        for thread in self.state.workers:
            worker_file.write(','.join(thread.get_stats()) + "\n")
        worker_file.close()

        # Write task information
        task_file.write(','.join(Task.get_stat_headers(self.config)) + "\n")
        for task in self.state.tasks:
            task_file.write(','.join(task.get_stats()) + "\n")
        task_file.close()

        # Save the configuration
        json.dump(self.config.__dict__, meta_file, indent=0)
        meta_file.close()

        # Save global stats
        json.dump(self.state.results(), stats_file, indent=0)
        stats_file.close()

        # # If recording work steal stats, save
        # if self.config.record_steals:
        #     ws_file = open("{}work_steal_stats.csv".format(new_dir_name), "w")
        #     ws_file.write("Local Thread,Remote Thread,Time Since Last Check,Queue Length,Check Count,Successful\n")
        #     for check in self.state.ws_checks:
        #         ws_file.write("{},{},{},{},{},{}\n".format(check[0], check[1], check[2], check[3], check[4], check[5]))
        #     ws_file.close()

        # # If recording allocations, save
        # if self.config.record_allocations:
        #     realloc_sched_file = open("{}realloc_schedule".format(new_dir_name), "w")
        #     realloc_sched_file.write(str(self.state.reallocation_schedule))
        #     realloc_sched_file.close()

        # # If recording queue lengths, save
        # if self.config.record_queue_lens:
        #     qlen_file = open("{}queue_lens.csv".format(new_dir_name), "w")
        #     for lens in self.state.queue_lens:
        #         qlen_file.write(",".join([str(x) for x in lens]) + "\n")
        #     qlen_file.close()

if __name__ == "__main__":

    run_name = SINGLE_THREAD_SIM_NAME_FORMAT.format(os.uname().nodename,
                                                    datetime.datetime.now().strftime("%y-%m-%d_%H:%M:%S"))
    path_to_sim = os.path.relpath(pathlib.Path(__file__).resolve().parents[1], start=os.curdir)

    if os.path.isfile(sys.argv[1]):
        cfg_json = open(sys.argv[1], "r")
        cfg = json.load(cfg_json, object_hook=SimConfig.decode_object)
        cfg.name = run_name
        cfg_json.close()

        if "-d" in sys.argv:
            logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(message)s')
            sys.argv.remove("-d")

        if len(sys.argv) > 2:
            if not os.path.isdir(RESULTS_DIR.format(path_to_sim)):
                os.makedirs(RESULTS_DIR.format(path_to_sim))
            meta_log = open(META_LOG_FILE.format(path_to_sim), "a")
            meta_log.write("{}: {}\n".format(run_name, sys.argv[2]))
            meta_log.close()
            cfg.description = sys.argv[2]

    else:
        print("Config file not found.")
        exit(1)

    sim = Simulation(cfg, path_to_sim)
    sim.run()
    sim.save_stats()

    if not(os.path.isdir(CONFIG_LOG_DIR.format(path_to_sim))):
        os.makedirs(CONFIG_LOG_DIR.format(path_to_sim))
    config_record = open(CONFIG_LOG_DIR.format(path_to_sim) + run_name + ".json", "w")
    cfg_json = open(sys.argv[1], "r")
    config_record.write(cfg_json.read())
    cfg_json.close()
    config_record.close()
