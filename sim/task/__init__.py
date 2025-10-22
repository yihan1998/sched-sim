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
        return "Task (arrival {}, service time {})".format(
            self.arrival_time, self.service_time)

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