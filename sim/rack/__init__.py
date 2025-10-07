import logging
import random
from worker import Worker
from rack.scheduler import RackScheduler

class Rack:
    """Represents a rack containing multiple workers with replicated database."""
    def __init__(self, identifier, workers, config, state):
        self.identifier = identifier
        self.workers = workers
        self.scheduler = RackScheduler(workers, config, state)
        self.config = config
        self.state = state

    def __str__(self):
        return f"Rack {self.identifier} with {len(self.workers)} workers: {self.workers}"

    def __repr__(self):
        return self.__str__()

    def schedule(self, subtask):
        """Schedule a subtask to a worker's queue based on the selected policy."""
        if self.config.join_shortest_queue or self.config.join_shortest_estimated_delay_queue:
            workers = self.state.racks[subtask.rack_id].workers
            logging.debug(f"[SCHEDULE]: Workers in Rack {self.identifier}: {workers}")
            chosen_queue = self.scheduler.get_shortest_queue(workers)
            self.state.queues[chosen_queue].enqueue(subtask)
            logging.debug(f"[SCHEDULE]: Subtask of job {subtask.identifier} assigned to queue {chosen_queue} on Rack {self.identifier}")
        elif self.config.global_queue:
            self.state.main_queue.enqueue(subtask)
            logging.debug(f"[SCHEDULE]: Subtask of job {subtask.identifier} assigned to main queue on Rack {self.identifier}")
        else:
            raise ValueError("No valid scheduling policy selected.")
