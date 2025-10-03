class Queue:
    """Queue with locking capabilities."""
    def __init__(self, identifier, config, state):
        self.identifier = identifier
        self.queue = []
        self.worker_ids = []

    def set_worker(self, worker_id):
        """Add a thread to the set of threads corresponding to this queue."""
        self.worker_ids.append(worker_id)

    def head(self):
        """Return the first element of the queue."""
        return self.queue[0] if len(self.queue) > 0 else None

    def tail(self):
        """Return the last element of the queue."""
        return self.queue[-1] if len(self.queue) > 0 else None

    def enqueue(self, task):
        """Add a task to the end of the queue."""
        self.queue.append(task)

    def dequeue(self):
        """Remove and return turn task from the front of the queue."""
        return self.queue.pop(0) if len(self.queue) > 0 else None
    
    def length(self):
        """Return the length of the queue."""
        return len(self.queue)

    def __str__(self):
        return "Queue {} (len: {})".format(self.identifier, len(self.queue))

    def __repr__(self):
        return str(self)