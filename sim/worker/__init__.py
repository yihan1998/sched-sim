import logging
from queue import Queue
from timer import Timer
from collections import defaultdict, deque

class Worker:
    def __init__(self, given_queue, identifier, config, state):
        self.queue = given_queue
        self.identifier = identifier
        self.current_task = None

        self.time_busy = 0
        self.task_time = 0

        self.preemption_timer_on = False
        self.preemption_timer = Timer()
        self.deadline_signal = False

        # Track requeue events per worker
        self.requeue_history = deque()
        self.preemption_counts = 0

        self.requeue_history_window = config.REQUEUE_HISTORY_WINDOW if hasattr(config, 'REQUEUE_HISTORY_WINDOW') else 10000
        self.recent_requeue_counts = 0

        # Log to track preemption times for each task
        self.preemption_log = defaultdict(list)

        self.config = config
        self.state = state

    def is_busy(self):
        """Return true if the thread has any task."""
        return self.current_task is not None
    
    def process_task(self, time_increment=1):
        current = self.current_task
        self.current_task.process(time_increment=time_increment)
        if self.current_task.complete:
            # Record successful completion in central scheduler
            if hasattr(self.state, 'central_scheduler') and self.state.central_scheduler:
                self.state.central_scheduler.record_task_completion(self.identifier, self.current_task)
            
            self.current_task = None
            if self.config.local_preemption and self.preemption_timer_on:
                self.preemption_timer_on = False
                self.preemption_timer.reset()

        # If the task just completed took no time, schedule again
        if current.service_time == 0:
            self.schedule(time_increment=time_increment)

        # Otherwise, account for the time spent
        else:
            self.time_busy += time_increment
            self.task_time += time_increment

    def record_requeue(self, task):
        """Record a requeue event for a worker and update the preemption log."""
        current_time = self.state.timer.get_time()

        # Add to requeue history
        self.requeue_history.append(current_time)
        self.preemption_counts += 1

        # Update preemption log
        self.preemption_log[task.identifier].append(current_time)

        # Clean old history outside the window
        while (self.requeue_history and
               current_time - self.requeue_history[0] > self.requeue_history_window):
            self.requeue_history.popleft()

        # Update recent requeue count
        self.recent_requeue_counts = len(self.requeue_history)

        logging.debug(f"[REQUEUE]: Worker {self.identifier} requeued task {task.identifier}, "
                     f"recent requeues: {self.recent_requeue_counts}, "
                     f"preemption log: {self.preemption_log[task.identifier]}")

    def get_preemptions_within_window(self, time_window):
        """Get the count of preemptions within a given time window."""
        current_time = self.state.timer.get_time()
        for task_id, timestamps in self.preemption_log.items():
            logging.debug(f"[PREEMPTION LOG]: Worker {self.identifier}, Task {task_id}, Timestamps: {timestamps}")
        count = 0
        for task_id, timestamps in self.preemption_log.items():
            # Filter timestamps within the time window
            count += len([t for t in timestamps if current_time - t <= time_window])
        return count

    def schedule(self, time_increment=1):
        if self.is_busy():
            # Check for deadline signal - preempt current task if deadline-critical task is waiting
            if self.deadline_signal and self.has_priority_task_waiting():
                logging.debug("[PREEMPT DEADLINE]: Worker {} preempting {} for deadline-critical task".format(self.identifier, self.current_task))
                
                # Record local preemption stats
                if self.config.local_preemption:
                    self.record_requeue(self.current_task)

                if self.config.global_queue and self.state.main_queue is not None:
                    self.state.main_queue.enqueue(self.current_task)
                elif (self.config.join_shortest_queue or self.config.join_shortest_estimated_delay_queue) and self.queue is not None:
                    self.queue.enqueue(self.current_task)
                
                self.current_task = None
                self.deadline_signal = False  # Reset signal
                if self.preemption_timer_on:
                    self.preemption_timer_on = False
                    self.preemption_timer.reset()
            
            # Process current task if not preempted
            if self.current_task:
                self.process_task(time_increment=time_increment)
                
            # Handle regular preemption timer
            if self.preemption_timer_on:
                self.preemption_timer.increment(time_increment)
                if self.preemption_timer.get_time() >= self.config.PREEMPTION_TIME:
                    logging.debug("[PREEMPT]: Worker {} preempting {}".format(self.identifier, self.current_task))
                    
                    # Record local preemption stats
                    if self.config.local_preemption:
                        self.record_requeue(self.current_task)

                    if self.config.global_queue and self.state.main_queue is not None:
                        self.state.main_queue.enqueue(self.current_task)
                        self.current_task = None
                        self.preemption_timer_on = False
                        self.preemption_timer.reset()
                    elif (self.config.join_shortest_queue or self.config.join_shortest_estimated_delay_queue) and self.queue is not None:
                        self.queue.enqueue(self.current_task)
                        self.current_task = None
                        self.preemption_timer_on = False
                        self.preemption_timer.reset()

        else:
            if self.config.global_queue and self.state.main_queue.length() > 0:
                self.current_task = self.dequeue_with_priority(self.state.main_queue)
                if self.current_task is not None:  # Safety check
                    if self.config.local_preemption:
                        self.preemption_timer_on = True
                    logging.debug("[START]: Worker {} starting {}".format(self.identifier, self.current_task))
                    self.process_task(time_increment=time_increment)
                else:
                    logging.debug("[ERROR]: Worker {} got None task from non-empty main queue".format(self.identifier))
            elif (self.config.join_shortest_queue or self.config.join_shortest_estimated_delay_queue) and self.queue.length() > 0:
                self.current_task = self.dequeue_with_priority(self.queue)
                if self.current_task is not None:  # Safety check
                    if self.config.local_preemption:
                        self.preemption_timer_on = True
                    logging.debug("[START]: Worker {} starting {}".format(self.identifier, self.current_task))
                    self.process_task(time_increment=time_increment)
                else:
                    logging.debug("[ERROR]: Worker {} got None task from non-empty queue".format(self.identifier))

    def dequeue_with_priority(self, queue):
        """Dequeue priority task if exists, otherwise normal FIFO."""
        if not self.config.deadline_aware_preemption:
            return queue.dequeue()
        
        # Look for priority task
        for i, task in enumerate(queue.queue):  # Changed from queue.tasks to queue.queue
            if hasattr(task, 'parent_job') and task.parent_job and task.parent_job.priority_boost:
                logging.debug("[PRIORITY DEQUEUE]: Worker {} dequeuing priority task {}".format(self.identifier, task))
                return queue.queue.pop(i)
        
        # No priority task, normal dequeue
        return queue.dequeue()

    def notify_deadline_critical_task(self):
        """Notify worker that a deadline-critical task is waiting in the queue."""
        self.deadline_signal = True
        logging.debug(f"[DEADLINE SIGNAL]: Worker {self.identifier} notified of deadline-critical task")

    def has_priority_task_waiting(self):
        """Check if queue has a priority-boosted subtask waiting."""
        queue_to_check = self.state.main_queue if self.config.global_queue else self.queue
        if not queue_to_check:
            return False

        for task in queue_to_check.queue:
            logging.debug(f"[CHECK PRIORITY]: Worker {self.identifier} checking task {task}")
            if getattr(task, 'priority_boost', False):
                # Only return True if not currently processing this boosted subtask
                if self.current_task is None or self.current_task != task:
                    logging.debug(f"[PRIORITY FOUND]: Worker {self.identifier} found priority-boosted subtask {task}")
                    return True
        return False
        
    def get_stats(self):
        stats = [self.identifier, self.time_busy, self.task_time]
        stats = [str(x) for x in stats]
        return stats

    @staticmethod
    def get_stat_headers(config):
        headers = ["Worker ID", "Busy Time", "Task Time"]
        return headers

    def __str__(self):
        if self.is_busy():
            return "Thread {} (queue {}): busy on {}".format(self.identifier, self.queue.identifier, self.current_task)
        else:
            return "Thread {} (queue {}): idle".format(self.identifier, self.queue.identifier)

    def __repr__(self):
        return str(self)