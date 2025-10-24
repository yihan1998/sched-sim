import logging
import scipy.stats as stats
from scipy.integrate import quad
from queue import Queue
from timer import Timer

class Worker:
    def __init__(self, given_queue, identifier, config, state):
        self.queue = given_queue
        self.id = identifier
        self.current_task = None

        self.time_busy = 0
        self.task_time = 0

        self.preemption_timer_on = False
        self.preemption_timer = Timer()

        self.config = config
        self.state = state

    def is_busy(self, search_spin_idle=False):
        """Return true if the thread has any task."""
        return self.current_task is not None
    
    def process_task(self, time_increment=1):
        current = self.current_task
        self.current_task.process(time_increment=time_increment)
        if self.current_task.complete:
            self.current_task = None
            if self.config.preemption and self.preemption_timer_on:
                self.preemption_timer_on = False
                self.preemption_timer.reset()

        # If the task just completed took no time, schedule again
        if current.service_time == 0:
            self.schedule(time_increment=time_increment)

        # Otherwise, account for the time spent
        else:
            self.time_busy += time_increment
            self.task_time += time_increment
    
    def compute_gittins_rank(self, task, current_time):
        """
        Compute the Gittins index for a task.
        (Same logic as in Simulation class)
        """
        if task.complete:
            return float('inf')
        
        # Age s = work done = original service time - remaining time  
        s = task.service_time - task.time_left
        
        if self.config.pareto_service_time:
            alpha = self.config.PARETO_SHAPE_PARAMETER
            x_m = self.config.AVERAGE_SERVICE_TIME * (alpha - 1) / alpha  # Scale parameter
            
            # For Pareto distribution, we use the conditional Gittins formula
            s_eff = max(s, x_m)
            t = 2.0 * s_eff  # Choose quantum t = 2 * s_eff
            
            if alpha <= 1:
                rank = s_eff
            else:
                # Compute the Gittins rank using the formula:
                # numerator = s/(α-1) * [1 - (s/t)^(α-1)]
                # denominator = 1 - (s/t)^α
                numerator = s_eff / (alpha - 1) * (1 - (s_eff / t) ** (alpha - 1))
                denominator = 1 - (s_eff / t) ** alpha
                
                if denominator > 1e-10:
                    rank = numerator / denominator
                else:
                    rank = numerator / 1e-10

        elif self.config.normal_service_time or self.config.bimodal_service_time:
            # For these distributions, use the pre-computed lookup table from simulation
            s_eff = max(s, 0.1)
            
            # Access the pre-computed cache from the simulation object (via state)
            if hasattr(self.state, 'simulation') and hasattr(self.state.simulation, 'gittins_cache'):
                cache = self.state.simulation.gittins_cache
                if cache:
                    age_key = min(cache.keys(), key=lambda k: abs(k - s_eff))
                    rank = cache[age_key]
                else:
                    # Fallback: use age-based approximation if cache not available
                    rank = s_eff + 1.0
            else:
                # Fallback: use age-based approximation if cache not available
                rank = s_eff + 1.0
        else:
            # For exponential (constant service time), use remaining time (SRPT)
            rank = task.time_left
        
        return rank

    def dequeue_by_gittins_rank(self, queue):
        """
        Dequeue the task with the lowest Gittins rank from the queue.
        This implements the distributed Gittins (d-Gittins) policy.
        """
        if queue.length() == 0:
            return None
        
        current_time = self.state.timer.get_time()
        
        # Find task with minimum Gittins rank
        best_task = None
        best_rank = float('inf')
        best_position = -1
        
        for i, task in enumerate(queue.queue):
            rank = self.compute_gittins_rank(task, current_time)
            if rank < best_rank:
                best_rank = rank
                best_task = task
                best_position = i
        
        if best_task is not None:
            # Remove the task from the queue
            queue.queue.pop(best_position)
            logging.debug(f"[d-GITTINS]: Worker {self.id} selected task with rank {best_rank:.2f}")
        
        return best_task

    def dequeue_by_shortest_remaining(self, queue):
        """
        Dequeue the task with the shortest remaining time from the queue.
        This implements the distributed Shortest Remaining Job First (d-SRJF) policy.
        """
        if queue.length() == 0:
            return None
        
        # Find task with minimum remaining time
        best_task = None
        best_remaining = float('inf')
        best_position = -1
        
        for i, task in enumerate(queue.queue):
            if task.time_left < best_remaining:
                best_remaining = task.time_left
                best_task = task
                best_position = i
        
        if best_task is not None:
            # Remove the task from the queue
            queue.queue.pop(best_position)
            logging.debug(f"[d-SRJF]: Worker {self.id} selected task with remaining time {best_remaining}")
        
        return best_task

    def schedule(self, time_increment=1):
        if self.is_busy():
            # Process the current task
            self.process_task(time_increment=time_increment)

            # Handle preemption (timer-based, not policy-specific)
            if self.config.preemption:
                if self.preemption_timer_on:
                    self.preemption_timer.increment(time_increment)
                    if self.preemption_timer.get_time() >= self.config.PREEMPTION_TIME:
                        logging.debug("[PREEMPT]: Worker {} preempting {}".format(self.id, self.current_task))
                        
                        # Return task to appropriate queue based on queue assignment policy
                        if self.config.global_queue and self.state.main_queue is not None:
                            self.state.main_queue.enqueue(self.current_task)
                        elif self.queue is not None:
                            self.queue.enqueue(self.current_task)
                        
                        self.current_task = None
                        self.preemption_timer_on = False
                        self.preemption_timer.reset()

        else:
            # When idle, select next task based on SCHEDULING POLICY
            # For CENTRALIZED policies (c-Gittins, c-SRJF): tasks are assigned centrally, don't dequeue here
            if self.config.global_queue and \
               (self.config.gittins_index or self.config.shortest_remaining_job_first):
                return
            
            # First determine which queue to use (based on queue assignment policy)
            queue_to_use = None
            if self.config.global_queue and self.state.main_queue.length() > 0:
                queue_to_use = self.state.main_queue
            elif self.queue is not None and self.queue.length() > 0:
                queue_to_use = self.queue
            
            if queue_to_use is None:
                return
            
            # Then apply scheduling policy to select task from queue
            if self.config.gittins_index:
                # d-Gittins: distributed Gittins - select task with lowest Gittins rank from local queue
                self.current_task = self.dequeue_by_gittins_rank(queue_to_use)
                
            elif self.config.shortest_remaining_job_first:
                # d-SRJF: distributed SRJF - select task with minimum remaining time from local queue
                self.current_task = self.dequeue_by_shortest_remaining(queue_to_use)
                
            else:
                # FCFS and PS: simple FIFO dequeue
                # (The difference is in preemption: PS uses preemption, FCFS doesn't)
                self.current_task = queue_to_use.dequeue()
            
            if self.current_task is not None:
                if self.config.preemption and self.config.process_sharing:
                    self.preemption_timer_on = True
                logging.debug("[START]: Worker {} starting {}".format(self.id, self.current_task))
                self.process_task(time_increment=time_increment)
        
    def get_stats(self):
        stats = [self.id, self.time_busy, self.task_time]
        stats = [str(x) for x in stats]
        return stats

    @staticmethod
    def get_stat_headers(config):
        headers = ["Worker ID", "Busy Time", "Task Time"]
        return headers

    def __str__(self):
        if self.is_busy():
            return "Thread {} (queue {}): busy on {}".format(self.id, self.queue.id, self.current_task)
        else:
            return "Thread {} (queue {}): idle".format(self.id, self.queue.id)

    def __repr__(self):
        return str(self)