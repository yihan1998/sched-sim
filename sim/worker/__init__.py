class Worker:
    def __init__(self, num_cores, scheduling_policy):
        self.num_cores = num_cores
        self.scheduling_policy = scheduling_policy
        self.queues = [[] for _ in range(num_cores)]  # Local queues for each core

    def add_task(self, task):
        if self.scheduling_policy == 'FCFS':
            self.queues[0].append(task)  # Add to the first core's queue for simplicity
        elif self.scheduling_policy == 'Processor Sharing':
            # Distribute tasks among all cores
            for i in range(self.num_cores):
                self.queues[i].append(task)

    def run(self):
        for i in range(self.num_cores):
            while self.queues[i]:
                task = self.queues[i].pop(0)
                self.execute_task(task)

    def execute_task(self, task):
        print(f"Executing task: {task}")
