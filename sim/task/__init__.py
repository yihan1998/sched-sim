import logging

class Task:
    def __init__(self, time, arrival_time, config, state):
        self.service_time = time
        self.complete = False
        self.time_left = time
        self.arrival_time = arrival_time
        self.start_time = None
        self.completion_time = 0
        self.preempted = False

        self.config = config
        self.state = state

    def process(self, time_increment=1, stop_condition=None):
        """Process the task for given time step.
        :param time_increment: Time step.
        :param stop_condition: Additional condition that must be met for the task to complete other than no more time
        left.
        """
        if self.time_left == self.service_time:
            self.start_time = self.state.timer.get_time()
        self.time_left -= time_increment

        # Any processing that must be done with the decremented timer but before the time left is checked
        self.process_logic()

        # If no more time left and stop condition is met, complete the task
        if self.time_left <= 0 and (stop_condition is None or stop_condition()):
            self.complete = True
            self.completion_time = self.state.timer.get_time()
            self.on_complete()

    def process_logic(self):
        """Any processing that must be done with the decremented timer but before the time left is checked."""
        pass

    def on_complete(self):
        self.state.complete_task_count += 1

    def time_in_system(self):
        """Returns time that the task has been in the system (arrival to completion)."""
        return self.completion_time - self.arrival_time + 1
    
    def descriptor(self):
        return "Task (arrival {}, service time {}, original queue: {})".format(
            self.arrival_time, self.service_time, self.original_queue)

    def get_stats(self):
        stats = [self.arrival_time, self.time_in_system(), self.service_time]
        stats = [str(x) for x in stats]
        return stats

    @staticmethod
    def get_stat_headers(config):
        headers = ["Arrival Time", "Time in System", "Request Service Time"]
        return headers

    def __str__(self):
        if not self.complete:
            return self.descriptor() + ": time left of {}".format(self.time_left)
        else:
            return self.descriptor() + ": done at {}".format(self.completion_time)

    def __repr__(self):
        return str(self)

class SubTask(Task):
    def __init__(self, time, arrival_time, config, state, parent_job=None, rack_id=None):
        super().__init__(time, arrival_time, config, state)
        self.identifier = parent_job.identifier if parent_job is not None else None
        self.parent_job = parent_job
        self.priority_boost = False
        self.rack_id = rack_id
        self.worker_id = None

    def process(self, time_increment=1, stop_condition=None):
        """Process the task for given time step.
        :param time_increment: Time step.
        :param stop_condition: Additional condition that must be met for the task to complete other than no more time
        left.
        """
        if self.time_left == self.service_time:
            self.start_time = self.state.timer.get_time()
        self.time_left -= time_increment

        # Any processing that must be done with the decremented timer but before the time left is checked
        self.process_logic()

        # If no more time left and stop condition is met, complete the task
        if self.time_left <= 0 and (stop_condition is None or stop_condition()):
            self.complete = True
            self.completion_time = self.state.timer.get_time()
            logging.debug("[COMPLETE]: Subtask of job {} completed".format(self.identifier))
            self.parent_job.on_complete()
            self.on_complete()

    def descriptor(self):
        return "Subtask (id {}, arrival {}, duration {})".format(
            self.identifier, self.arrival_time, self.service_time)

    def get_stats(self):
        stats = [self.identifier, self.arrival_time, self.time_in_system(), self.service_time, self.rack_id, self.worker_id]
        stats = [str(x) for x in stats]
        return stats

    @staticmethod
    def get_stat_headers(config):
        headers = ["Job ID", "Arrival Time", "Time in System", "Request Service Time", "Rack ID", "Worker ID"]
        return headers

class Job(Task):
    def __init__(self, identifier, time, arrival_time, config, state):
        super().__init__(time, arrival_time, config, state)
        self.identifier = identifier
        self.subtasks = []
        self.finished_subtasks = 0
        
        # Deadline tracking
        self.priority_boost = False
        self.deadline = None

    def process(self, time_increment=0):
        # Set deadline based on total work
        if self.config.deadline_aware_preemption and self.deadline is None:
            total_work = sum(st.service_time for st in self.subtasks)
            self.deadline = self.arrival_time + self.config.GLOBAL_PREEMPTION_TIME
            logging.debug("[DEADLINE SET]: Job {} deadline at {} (total work: {})".format(
                self.identifier, self.deadline, total_work))

        # Enqueue subtasks using Rack's schedule method
        for subtask in self.subtasks:
            if subtask.rack_id is not None:
                rack = self.state.racks[subtask.rack_id]
                rack.schedule(subtask)
                logging.debug("[SCHEDULE]: Subtask of job {} assigned to Rack {}".format(
                    self.identifier, subtask.rack_id))
            else:
                logging.warning("[WARNING]: Subtask of job {} has no rack assigned".format(self.identifier))

    def on_complete(self):
        self.finished_subtasks += 1
        if self.finished_subtasks == len(self.subtasks):
            self.complete = True
            self.completion_time = self.state.timer.get_time()
            logging.debug("[COMPLETE]: Job {} completed".format(self.identifier))
            self.state.complete_job_count += 1

    def descriptor(self):
        return "Job (id {}, arrival {})".format(
            self.identifier, self.arrival_time)

    def get_stats(self):
        stats = [self.identifier, self.arrival_time, self.time_in_system()]
        stats = [str(x) for x in stats]
        return stats

    @staticmethod
    def get_stat_headers(config):
        headers = ["Job ID", "Arrival Time", "Time in System"]
        return headers