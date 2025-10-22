#!/usr/bin/env python
"""Object to maintain simulation state."""

import math
import datetime
import logging
import random

from timer import Timer
# from work_search_state import WorkSearchState
from task import Task
from worker import Worker
# from sim_thread import Thread
from queue import Queue
import progress_bar as progress


class SimulationState:
    """Object to maintain simulation state as time passes."""
    def __init__(self, config):
        # Simulation Global Variables
        self.timer = Timer()
        self.workers = []
        self.queues = []
        self.tasks = []
        self.available_queues = []
        self.main_queue = None

        self.tasks_scheduled = 0
        self.end_time = None
        self.sim_end_time = None

        self.complete_task_count = 0

        self.config = config

    def any_incomplete(self):
        """Return true if there are any incomplete tasks for the entire simulation."""
        return self.complete_task_count < self.tasks_scheduled
    
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

        # Set tasks and arrival times
        request_rate = config.avg_system_load * config.num_workers / config.AVERAGE_SERVICE_TIME
        next_task_time = int(random.expovariate(request_rate))
        i = 0
        identifier = 1
        while (config.sim_duration is None or next_task_time < config.sim_duration) and \
            (config.num_tasks is None or i < config.num_tasks):

            service_time = None
            
            # Generate service time according to distribution
            while service_time is None or service_time == 0:
                if config.constant_service_time:
                    service_time = config.AVERAGE_SERVICE_TIME
                elif config.bimodal_service_time:
                    # 95% are 500, 5% are 5500
                    distribution = [500] * 95 + [5500] * 5
                    service_time = random.choice(distribution)
                elif config.pareto_service_time:
                    alpha = 1.5
                    target_mean = config.AVERAGE_SERVICE_TIME
                    x_min = target_mean * (alpha - 1) / alpha
                    u = random.random()
                    service_time = x_min / (u ** (1/alpha))
                else:
                    service_time = int(random.expovariate(1 / config.AVERAGE_SERVICE_TIME))

            # Create task
            task = Task(
                time=int(service_time),
                arrival_time=next_task_time,
                config=config,
                state=self
            )
            self.tasks.append(task)

            logging.debug(f"[INIT]: Task {identifier} (service_time={service_time})")

            next_task_time += int(random.expovariate(request_rate))
            i += 1
            identifier += 1

    def get_shortest_queue(self):
        """Get the index of the queue with the shortest length (JSQ policy)."""
        if not self.queues:
            return 0
        return min(range(len(self.queues)), key=lambda i: self.queues[i].length())
    
    def add_final_stats(self):
        """Add final global stats to to the simulation state."""
        self.end_time = self.timer.get_time()
        self.tasks_scheduled = len(self.tasks)
        self.sim_end_time = datetime.datetime.now().strftime("%y-%m-%d_%H:%M:%S")

    def results(self):
        """Create a dictionary of important statistics for saving."""
        stats = {"Completed Tasks": self.complete_task_count,
                 "Tasks Scheduled": self.tasks_scheduled,
                 "Simulation End Time": self.sim_end_time,
                 "End Time": self.end_time}
        return stats
        