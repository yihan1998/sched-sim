#!/usr/bin/env python
"""Object to maintain simulation state."""

import math
import datetime
import logging
import random

from timer import Timer
# from work_search_state import WorkSearchState
from task import SubTask, Job
from worker import Worker
# from sim_thread import Thread
from queue import Queue
# from rack.scheduler import CentralScheduler
import progress_bar as progress
from rack import Rack


class SimulationState:
    """Object to maintain simulation state as time passes."""
    def __init__(self, config):
        # Simulation Global Variables
        self.timer = Timer()
        self.racks = []
        self.workers = []
        self.queues = []
        self.jobs = []
        self.tasks = []
        self.available_queues = []
        self.main_queue = None

        self.tasks_scheduled = 0
        self.jobs_scheduled = 0
        self.end_time = None
        self.sim_end_time = None

        self.complete_task_count = 0
        self.complete_job_count = 0

        self.config = config
        
        # Initialize central scheduler
        self.central_scheduler = None

    def any_incomplete(self):
        """Return true if there are any incomplete tasks for the entire simulation."""
        return self.complete_job_count < self.jobs_scheduled
    
    def initialize_state(self, config):
        """Initialize the simulation state based on the configuration."""
        if config.progress_bar:
            print("\nInitializing...")

        # Input validation
        if not config.validate():
            print("Invalid configuration")
            return

        random.seed(42)

        # Initialize queues
        for i in range(len(set(config.mapping))):
            self.queues.append(Queue(i, config, self))
        self.available_queues = list(set(config.mapping))

        if config.join_shortest_queue or config.global_queue:
            self.main_queue = Queue(-1, config, self)

        # Initialize workers
        for i in range(config.num_workers):
            queue = self.queues[config.mapping[i]]
            self.workers.append(Worker(queue, i, config, self))
            queue.set_worker(i)
            
        for i in range(config.num_racks):
            rack_workers = self.workers[i * (config.num_workers // config.num_racks):(i + 1) * (config.num_workers // config.num_racks)]
            rack = Rack(i, rack_workers, config, self)
            self.racks.append(rack)

        # Initialize central scheduler when JSQ is enabled (either regular or capacity-aware)
        # if config.join_shortest_queue or config.join_shortest_estimated_delay_queue:
        #     self.central_scheduler = CentralScheduler(config, self)

        # Set tasks and arrival times
        request_rate = config.avg_system_load * config.num_workers * config.num_racks / (config.AVERAGE_SERVICE_TIME * config.num_subtasks_per_job)
        next_job_time = int(random.expovariate(request_rate))
        i = 0
        identifier = 1
        while (config.sim_duration is None or next_job_time < config.sim_duration) and \
            (config.num_tasks is None or i < config.num_tasks):
            
            # Create job
            job = Job(
                identifier=identifier,
                time=0,
                arrival_time=next_job_time,
                config=config,
                state=self
            )
            
            # Create subtasks for this job
            for j in range(config.num_subtasks_per_job):
                service_time = None
                
                # Generate service time according to distribution
                while service_time is None or service_time == 0:
                    if config.constant_service_time:
                        service_time = config.AVERAGE_SERVICE_TIME
                    elif config.bimodal_service_time:
                        # 95% are 500, 5% are 10500
                        distribution = [500] * 95 + [10500] * 5
                        service_time = random.choice(distribution)
                    elif config.pareto_service_time:
                        alpha = 2.5
                        target_mean = config.AVERAGE_SERVICE_TIME
                        x_min = target_mean * (alpha - 1) / alpha
                        u = random.random()
                        service_time = x_min / (u ** (1/alpha))
                    elif config.lognormal_service_time:
                        sigma = 0.25
                        mu = math.log(config.AVERAGE_SERVICE_TIME) - (sigma**2) / 2
                        service_time = random.lognormvariate(mu, sigma)
                    else:
                        service_time = int(random.expovariate(1 / config.AVERAGE_SERVICE_TIME))

                # Create subtask
                subtask = SubTask(
                    time=int(service_time),
                    arrival_time=next_job_time,
                    config=config,
                    state=self,
                    parent_job=job,
                    rack_id=i % config.num_racks
                )
                job.subtasks.append(subtask)
                self.tasks.append(subtask)
                
                logging.debug(f"[INIT]: Job {identifier} SubTask {j} (service_time={service_time})")
            
            self.jobs.append(job)
            logging.debug(f"[INIT]: Job {identifier} with {len(job.subtasks)} subtasks at time {next_job_time}")
            
            next_job_time += int(random.expovariate(request_rate))
            i += 1
            identifier += 1

    def get_shortest_queue(self):
        """Get the index of the queue with the shortest length (JSQ policy)."""
        if not self.queues:
            return 0
        
        # Check if we should use central scheduler
        if hasattr(self, 'central_scheduler') and self.central_scheduler:
            # Use capability-aware scheduling if enabled
            if self.config.join_shortest_estimated_delay_queue:
                return self.central_scheduler.get_shortest_capable_queue()
            # Use simple shortest queue with central scheduler tracking
            elif self.config.join_shortest_queue:
                return self.central_scheduler.get_shortest_queue_simple()
        
        # Fallback to original logic when no central scheduler
        return min(range(len(self.queues)), key=lambda i: self.queues[i].length())
    
    def add_final_stats(self):
        """Add final global stats to to the simulation state."""
        self.end_time = self.timer.get_time()
        self.tasks_scheduled = len(self.tasks)
        self.jobs_scheduled = len(self.jobs)
        self.sim_end_time = datetime.datetime.now().strftime("%y-%m-%d_%H:%M:%S")

    def results(self):
        """Create a dictionary of important statistics for saving."""
        stats = {"Completed Jobs": self.complete_job_count,"Completed Tasks": self.complete_task_count,
                 "Job Scheduled": self.jobs_scheduled, "Tasks Scheduled": self.tasks_scheduled,
                 "Simulation End Time": self.sim_end_time, "End Time": self.end_time}
        return stats
