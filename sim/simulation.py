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
import heapq
import scipy.stats as stats
from scipy.integrate import quad

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
        
        # For Gittins index scheduling: maintain a global pool of waiting tasks
        self.waiting_tasks = []  # List of all tasks waiting to be executed
        self.need_reschedule = False  # Flag to trigger rescheduling
        
        # Pre-compute Gittins index lookup tables for different distributions
        self.gittins_cache = {}
        if configuration.gittins_index:
            if configuration.normal_service_time:
                self._precompute_normal_gittins()
            elif configuration.bimodal_service_time:
                self._precompute_bimodal_gittins()
            # elif configuration.lognormal_service_time:
            #     self._precompute_lognormal_gittins()
    
    def _precompute_normal_gittins(self):
        """
        Pre-compute Gittins indices for normal distribution at discrete age points.
        This avoids expensive online computation during simulation.
        """
        mu = self.config.AVERAGE_SERVICE_TIME
        sigma = self.config.NORMAL_STD_DEV
        a, b = (0 - mu) / sigma, float('inf')
        trunc_norm = stats.truncnorm(a, b, loc=mu, scale=sigma)
        
        # Create age grid from 0 to mean + 4*sigma
        max_age = mu + 4 * sigma
        age_grid = list(range(0, int(max_age) + 100, 50))  # Every 50 units
        
        print(f"Pre-computing Gittins indices for normal distribution (μ={mu}, σ={sigma})...")
        
        for s in age_grid:
            s_eff = max(s, 0.1)
            
            # Grid of quantum values to search over
            t_grid = [s_eff + 10, s_eff + 50, s_eff + 100, s_eff + 200, mu, mu + sigma, mu + 2*sigma, max_age]
            t_grid = [t for t in t_grid if t > s_eff]
            
            min_rank = float('inf')
            
            for t in t_grid:
                try:
                    # Numerator: E[min(S, t) - s | S > s]
                    integral_part, _ = quad(lambda x: (x - s_eff) * trunc_norm.pdf(x), s_eff, t, limit=50)
                    tail_part = (t - s_eff) * (1 - trunc_norm.cdf(t))
                    numerator = integral_part + tail_part
                    
                    # Denominator: P(S <= t | S > s)
                    denominator = trunc_norm.cdf(t) - trunc_norm.cdf(s_eff)
                    
                    if denominator > 1e-10:
                        rank = numerator / denominator
                        min_rank = min(min_rank, rank)
                except:
                    continue
            
            if min_rank == float('inf'):
                min_rank = s_eff + 1.0
            
            self.gittins_cache[s] = min_rank
        
        print(f"Pre-computed {len(self.gittins_cache)} Gittins index values.")
    
    def _precompute_bimodal_gittins(self):
        """
        Pre-compute Gittins indices for bimodal distribution (95% at 500, 5% at 10500).
        
        Bimodal PMF: P(S = 500) = 0.95, P(S = 10500) = 0.05
        Mean = 0.95 * 500 + 0.05 * 10500 = 475 + 525 = 1000
        """
        print("Pre-computing Gittins indices for bimodal distribution (95% at 500, 5% at 10500)...")

        # Bimodal: 95% are 500, 5% are 10500
        p_short = 0.95
        short_time = 500
        long_time = 10500
        
        # Age grid - we need to cover 0 to beyond long_time
        max_age = long_time + 1000
        age_grid = list(range(0, int(max_age) + 100, 50))
        
        for s in age_grid:
            s_eff = max(s, 0.1)
            
            # Grid of quantum values
            t_grid = [s_eff + 10, s_eff + 50, s_eff + 100, s_eff + 200, 
                      short_time, short_time + 100, short_time + 500, 
                      long_time, long_time + 500, max_age]
            t_grid = [t for t in t_grid if t > s_eff]
            
            min_rank = float('inf')
            
            for t in t_grid:
                # For bimodal, compute Gittins index G(s,t) = E[min(S,t) - s | S > s] / P(S <= t | S > s)
                
                # Step 1: Compute P(S > s) - survival probability
                if s_eff < short_time:
                    # Both short and long jobs survive
                    p_survive = 1.0
                elif s_eff < long_time:
                    # Only long jobs survive
                    p_survive = 1 - p_short  # = 0.05
                else:
                    # No jobs survive past long_time
                    p_survive = 0.0
                
                if p_survive < 1e-10:
                    continue
                
                # Step 2: Compute E[min(S,t) - s | S > s]
                # For each job type, compute contribution to numerator
                numerator = 0.0
                
                # Short jobs (S = 500)
                if s_eff < short_time:
                    if t >= short_time:
                        # Short jobs complete at 500, contribute (500 - s)
                        numerator += p_short * (short_time - s_eff)
                    else:
                        # Short jobs get truncated at t, contribute (t - s)
                        numerator += p_short * (t - s_eff)
                # else: short jobs already completed, contribute 0
                
                # Long jobs (S = 10500)
                if s_eff < long_time:
                    if t >= long_time:
                        # Long jobs complete at 10500, contribute (10500 - s)
                        numerator += (1 - p_short) * (long_time - s_eff)
                    else:
                        # Long jobs get truncated at t, contribute (t - s)
                        numerator += (1 - p_short) * (t - s_eff)
                # else: long jobs already completed, contribute 0
                
                # Normalize by P(S > s)
                numerator /= p_survive
                
                # Step 3: Compute P(S <= t | S > s) = P(s < S <= t) / P(S > s)
                # Count which job types complete between s and t
                p_complete = 0.0
                
                # Short jobs complete at t if s < 500 <= t
                if s_eff < short_time and t >= short_time:
                    p_complete += p_short
                
                # Long jobs complete at t if s < 10500 <= t
                if s_eff < long_time and t >= long_time:
                    p_complete += (1 - p_short)
                
                denominator = p_complete / p_survive
                
                # Step 4: Compute Gittins rank
                if denominator > 1e-10:
                    rank = numerator / denominator
                    min_rank = min(min_rank, rank)
            
            # Store minimum rank over all t
            if min_rank == float('inf'):
                # If no valid rank found, use age-based fallback
                min_rank = s_eff + 1.0
            
            self.gittins_cache[s] = min_rank
        
        print(f"Pre-computed {len(self.gittins_cache)} Gittins index values.")
        print(f"Pre-computed {len(self.gittins_cache)} Gittins index values.")
    
    def _precompute_lognormal_gittins(self):
        """
        Pre-compute Gittins indices for lognormal distribution.
        """
        mu = self.config.AVERAGE_SERVICE_TIME
        
        # For lognormal: if mean = exp(μ + σ²/2), we need to solve for μ given mean
        # Use σ = 1.0 as typical value for moderate variability
        sigma_ln = 1.0
        mu_ln = math.log(mu) - (sigma_ln**2) / 2
        
        print(f"Pre-computing Gittins indices for lognormal distribution (mean={mu})...")
        
        from scipy.stats import lognorm
        lognorm_dist = lognorm(s=sigma_ln, scale=math.exp(mu_ln))
        
        # Age grid
        max_age = mu * 5  # Lognormal has heavy tail
        age_grid = list(range(0, int(max_age) + 100, 50))
        
        for s in age_grid:
            s_eff = max(s, 0.1)
            
            # Grid of quantum values
            t_grid = [s_eff + 10, s_eff + 50, s_eff + 100, s_eff + 500, mu, mu * 2, mu * 3, max_age]
            t_grid = [t for t in t_grid if t > s_eff]
            
            min_rank = float('inf')
            
            for t in t_grid:
                try:
                    # Numerator: E[min(S, t) - s | S > s]
                    integral_part, _ = quad(lambda x: (x - s_eff) * lognorm_dist.pdf(x), s_eff, t, limit=50)
                    tail_part = (t - s_eff) * (1 - lognorm_dist.cdf(t))
                    numerator = integral_part + tail_part
                    
                    # Denominator: P(S <= t | S > s)
                    denominator = lognorm_dist.cdf(t) - lognorm_dist.cdf(s_eff)
                    
                    if denominator > 1e-10:
                        rank = numerator / denominator
                        min_rank = min(min_rank, rank)
                except:
                    continue
            
            if min_rank == float('inf'):
                min_rank = s_eff + 1.0
            
            self.gittins_cache[s] = min_rank
        
        print(f"Pre-computed {len(self.gittins_cache)} Gittins index values.")

    def compute_gittins_rank(self, task, current_time):
        """
        Compute the Gittins index for a task.
        
        The Gittins index is: G(s) = inf_{t > s} { E[min(S, t) - s | S > s] / P(S <= t | S > s) }
        
        For Pareto distribution with conditional PDF f_{S|S>s}(x) = α * s^α * x^{-α-1} for x > s:
        
        Using sympy derivation:
        - P[S <= t | S > s] = 1 - (s/t)^α
        - E[min(S, t) - s | S > s] = integral(x * f_cond, s, t) + t * (s/t)^α - s
        - After simplification: rank = s / (α - 1) * [1 - (s/t)^(α-1)] / [1 - (s/t)^α]
        
        We choose a specific quantum t > s to evaluate the rank.
        """
        if task.complete:
            return float('inf')
        
        # Age s = work done = original service time - remaining time  
        s = task.service_time - task.time_left
        
        if self.config.pareto_service_time:
            alpha = self.config.PARETO_SHAPE_PARAMETER
            x_m = self.config.AVERAGE_SERVICE_TIME * (alpha - 1) / alpha  # Scale parameter
            
            # Age s = work done so far
            # For new tasks, s might be very small or 0
            
            # Choose a fixed quantum t for evaluation
            t = self.config.AVERAGE_SERVICE_TIME * 2.0
            
            # For very new tasks (s close to 0), use a small positive value to avoid division issues
            s_eff = max(s, 1.0)
            
            # Ensure t > s_eff (required for the formula)
            if t <= s_eff:
                t = s_eff * 2.0
            
            if alpha <= 1:
                # Degenerate case
                rank = s_eff
            else:
                # Gittins rank formula (from sympy derivation):
                # rank = (s / (α - 1)) * [1 - (s/t)^(α-1)] / [1 - (s/t)^α]
                
                s_over_t = s_eff / t
                
                # Numerator: s / (α - 1) * [1 - (s/t)^(α-1)]
                numerator = (s_eff / (alpha - 1)) * (1 - s_over_t ** (alpha - 1))
                
                # Denominator: 1 - (s/t)^α
                denominator = 1 - s_over_t ** alpha
                
                if denominator > 1e-10:
                    rank = numerator / denominator
                else:
                    rank = numerator / 1e-10
            
        elif self.config.normal_service_time:
            # For normal distribution, use pre-computed lookup table
            s_eff = max(s, 0.1)
            
            # Find closest pre-computed value
            if self.gittins_cache:
                age_key = min(self.gittins_cache.keys(), key=lambda k: abs(k - s_eff))
                rank = self.gittins_cache[age_key]
            else:
                # Fallback if cache not available
                rank = s_eff + 1.0
            
        # elif self.config.lognormal_service_time or self.config.bimodal_service_time:
        #     # Use pre-computed lookup table
        #     s_eff = max(s, 0.1)
            
        #     if self.gittins_cache:
        #         age_key = min(self.gittins_cache.keys(), key=lambda k: abs(k - s_eff))
        #         rank = self.gittins_cache[age_key]
        #     else:
        #         # Fallback: for heavy-tailed distributions, approximate with age-based priority
        #         rank = s + 1.0
            
        else:
            # For exponential (constant service time), use remaining time (SRPT)
            rank = task.time_left
        
        return rank
    
    def should_reschedule(self):
        """
        Determine if we need to reschedule by checking if the k-lowest rank/remaining tasks have changed.
        Returns True if the set of k tasks that should be running has changed.
        Only used for centralized scheduling.
        """
        if not self.config.global_queue:
            return False
            
        current_time = self.state.timer.get_time()
        
        # Collect ALL tasks: waiting in queue + currently running
        all_tasks = []
        
        # Waiting tasks from global queue
        all_tasks.extend(self.state.main_queue.queue)
        
        # Currently running tasks
        running_tasks = []
        for worker in self.state.workers:
            if worker.is_busy():
                all_tasks.append(worker.current_task)
                running_tasks.append(worker.current_task)
        
        if len(all_tasks) == 0:
            return False
        
        # Compute ranks for all tasks
        task_ranks = []
        for task in all_tasks:
            if self.config.gittins_index:
                rank = self.compute_gittins_rank(task, current_time)
            elif self.config.shortest_remaining_job_first:
                rank = task.time_left
            else:
                rank = task.time_left
            task_ranks.append((rank, id(task), task))
        
        # Get k lowest rank tasks
        k = len(self.state.workers)
        k_lowest = heapq.nsmallest(k, task_ranks)
        k_lowest_tasks = set(task for _, _, task in k_lowest)
        
        # Check if k-lowest differs from currently running
        running_set = set(running_tasks)
        return k_lowest_tasks != running_set
    
    def global_reschedule(self):
        """
        Global rescheduling: assign k tasks with lowest rank/remaining time to k workers.
        This implements CENTRALIZED (c-) Gittins OR c-SRJF scheduling (mutually exclusive).
        
        Only used when global_queue=True (centralized queueing).
        For distributed versions (d-Gittins, d-SRJF), workers handle scheduling locally.
        """
        
        current_time = self.state.timer.get_time()

        # Step 1: Collect all waiting tasks from global queue AND currently running tasks
        all_tasks = []
        while self.state.main_queue.length() > 0:
            task = self.state.main_queue.dequeue()
            all_tasks.append(task)
        
        # Add currently running tasks
        for worker in self.state.workers:
            if worker.is_busy():
                all_tasks.append(worker.current_task)
                worker.current_task = None
                worker.preemption_timer_on = False
                worker.preemption_timer.reset()
        
        logging.debug(f"[RESCHEDULE CHECK]: Total tasks: {len(all_tasks)}")

        # Step 2: Compute ranks for all tasks (either Gittins OR SRJF)
        task_ranks = []
        for task in all_tasks:
            if self.config.gittins_index:
                rank = self.compute_gittins_rank(task, current_time)
                logging.debug(f"\t > Task {task} has Gittins rank {rank:.2f}")
            elif self.config.shortest_remaining_job_first:
                rank = task.time_left
                logging.debug(f"\t > Task {task} has SRJF rank {rank:.2f}")
            else:
                # Should not reach here - global_reschedule should only be called for Gittins or SRJF
                rank = task.time_left
            # Use (rank, task_id, task) to avoid comparing Task objects when ranks are equal
            task_ranks.append((rank, id(task), task))
        
        # Step 3: Sort tasks by rank using heapsort (lowest rank = highest priority)
        heapq.heapify(task_ranks)

        # Step 4: Assign k lowest-rank tasks to k workers
        k = len(self.state.workers)
        k_lowest = heapq.nsmallest(k, task_ranks)
        
        for rank, _, task in k_lowest:
            # Find an idle worker (they should all be idle since we cleared them)
            for worker in self.state.workers:
                if not worker.is_busy():
                    worker.current_task = task
                    logging.debug(f"[RESCHEDULE]: Assigned task {task} with rank {rank:.2f} → worker {worker.id}")
                    if self.config.preemption:
                        worker.preemption_timer_on = True
                        worker.preemption_timer.reset()
                    break
        
        # Step 5: Put remaining tasks back in global queue
        assigned_tasks = set(task for _, _, task in k_lowest)
        remaining_tasks = [task for _, _, task in task_ranks if task not in assigned_tasks]
        
        for task in remaining_tasks:
            self.state.main_queue.enqueue(task)
        
        self.need_reschedule = False

    def choose_enqueue(self):
        """Choose the queue with the shortest length - Join Shortest Queue (JSQ) policy."""
        
        # Find the queue with minimum length (queue length + 1 if worker is busy)
        chosen_queue = self.state.available_queues[0]
        min_length = self.state.queues[chosen_queue].length() + (1 if self.state.workers[chosen_queue].is_busy() else 0)
        
        for queue_id in self.state.available_queues[1:]:
            current_length = self.state.queues[queue_id].length() + (1 if self.state.workers[queue_id].is_busy() else 0)
            
            if current_length < min_length:
                min_length = current_length
                chosen_queue = queue_id

        return chosen_queue

    def run(self):
        """Run the simulation."""

        # Initialize data
        self.state.initialize_state(self.config)
        
        # Store reference to simulation in state for workers to access Gittins cache
        self.state.simulation = self

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

            # Flag to track if any event occurred
            event_occurred = False

            # Put new task arrivals in queues
            while task_number < self.state.tasks_scheduled and \
                    self.state.tasks[task_number].arrival_time <= self.state.timer.get_time():
                
                # Queue assignment policy (decoupled from scheduling policy)
                if self.config.global_queue:
                    # Global queue: all tasks go to single shared queue
                    chosen_queue = self.state.main_queue
                    self.state.main_queue.enqueue(self.state.tasks[task_number])
                    logging.debug("[ARRIVAL]: {} onto global queue".format(self.state.tasks[task_number]))
                    
                elif self.config.join_shortest_queue:
                    # JSQ: choose queue with minimum length
                    chosen_queue = self.choose_enqueue()
                    self.state.queues[chosen_queue].enqueue(self.state.tasks[task_number])
                    logging.debug("[ARRIVAL]: {} onto queue {}".format(self.state.tasks[task_number], chosen_queue))
                    
                else:
                    # Default: random queue assignment
                    chosen_queue = random.choice(self.state.available_queues)
                    self.state.queues[chosen_queue].enqueue(self.state.tasks[task_number])
                    logging.debug("[ARRIVAL]: {} onto queue {}".format(self.state.tasks[task_number], chosen_queue))

                task = self.state.tasks[task_number]
                task.process(time_increment=0)
                task_number += 1
                
                # Mark event for CENTRALIZED scheduling (c-Gittins or c-SRJF)
                if self.config.global_queue and \
                   (self.config.gittins_index or self.config.shortest_remaining_job_first):
                    event_occurred = True

            # For CENTRALIZED scheduling: check and reschedule BEFORE workers process
            if self.config.global_queue and \
               (self.config.gittins_index or self.config.shortest_remaining_job_first) and \
               self.should_reschedule():
                self.global_reschedule()

            # Schedule workers
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