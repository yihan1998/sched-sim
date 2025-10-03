import logging
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

    def schedule(self, time_increment=1):
        if self.is_busy():
            self.process_task(time_increment=time_increment)
            if self.preemption_timer_on:
                self.preemption_timer.increment(time_increment)
                if self.preemption_timer.get_time() >= self.config.PREEMPTION_TIME:
                    logging.debug("[PREEMPT]: Worker {} preempting {}".format(self.id, self.current_task))
                    if self.config.global_queue and self.state.main_queue is not None:
                        self.state.main_queue.enqueue(self.current_task)
                        self.current_task = None
                        self.preemption_timer_on = False
                        self.preemption_timer.reset()
                    elif self.config.join_shortest_queue and self.queue is not None:
                        self.queue.enqueue(self.current_task)
                        self.current_task = None
                        self.preemption_timer_on = False
                        self.preemption_timer.reset()

        else:
            if self.config.global_queue and self.state.main_queue.length() > 0:
                self.current_task = self.dequeue_with_priority(self.state.main_queue)
                if self.config.local_preemption:
                    self.preemption_timer_on = True
                logging.debug("[START]: Worker {} starting {}".format(self.id, self.current_task))
                self.process_task(time_increment=time_increment)
            elif self.config.join_shortest_queue and self.queue.length() > 0:
                self.current_task = self.dequeue_with_priority(self.queue)
                if self.config.local_preemption:
                    self.preemption_timer_on = True
                logging.debug("[START]: Worker {} starting {}".format(self.id, self.current_task))
                self.process_task(time_increment=time_increment)

    def dequeue_with_priority(self, queue):
        """Dequeue priority task if exists, otherwise normal FIFO."""
        if not self.config.deadline_aware_preemption:
            return queue.dequeue()
        
        # Look for priority task
        for i, task in enumerate(queue.queue):  # Changed from queue.tasks to queue.queue
            if hasattr(task, 'parent_job') and task.parent_job and task.parent_job.priority_boost:
                logging.debug("[PRIORITY DEQUEUE]: Worker {} dequeuing priority task {}".format(self.id, task))
                return queue.queue.pop(i)
        
        # No priority task, normal dequeue
        return queue.dequeue()

    def has_priority_task_waiting(self):
        """Check if queue has a priority task."""
        queue_to_check = self.state.main_queue if self.config.global_queue else self.queue
        if not queue_to_check:
            return False
        
        for task in queue_to_check.queue:  # Changed from queue.tasks to queue.queue
            if hasattr(task, 'parent_job') and task.parent_job and task.parent_job.priority_boost:
                if hasattr(self.current_task, 'parent_job') and self.current_task.parent_job:
                    return not self.current_task.parent_job.priority_boost
        return False
        
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