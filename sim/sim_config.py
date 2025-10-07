import random

class SimConfig:
    """Object to hold all configuration state of the simulation. Remains constant."""
    def __init__(self, name=None, num_racks=None, num_queues=None, num_workers=None, mapping=[], 
                 avg_system_load=None, initial_num_tasks=None, sim_duration=None,
                 constant_service_time=False, bimodal_service_time=False, pareto_service_time=False, lognormal_service_time=False,
                 pb_enabled=True, num_subtasks_per_job=None,
                 join_shortest_queue=False, join_shortest_estimated_delay_queue=False, global_queue=False, 
                 local_preemption=False, deadline_aware_preemption=False):
        # Basic simulation parameters
        self.name = name
        self.num_racks = num_racks
        self.num_queues = num_queues
        self.num_workers = num_workers
        self.mapping = list(mapping)
        self.avg_system_load = avg_system_load
        self.num_tasks = initial_num_tasks
        self.sim_duration = sim_duration
        self.num_subtasks_per_job = num_subtasks_per_job

        self.constant_service_time = constant_service_time
        self.bimodal_service_time = bimodal_service_time
        self.pareto_service_time = pareto_service_time
        self.lognormal_service_time = lognormal_service_time
        self.progress_bar = pb_enabled
        self.join_shortest_queue = join_shortest_queue
        self.join_shortest_estimated_delay_queue = join_shortest_estimated_delay_queue
        self.global_queue = global_queue
        self.local_preemption = local_preemption
        self.deadline_aware_preemption = deadline_aware_preemption

        self.AVERAGE_SERVICE_TIME = 1000
        self.PREEMPTION_TIME = 1000
        self.GLOBAL_PREEMPTION_TIME = 2000
        
        # Central scheduler parameters
        self.REQUEUE_HISTORY_WINDOW = 5000  # Time window for tracking requeue history
        self.STABILITY_DECAY_FACTOR = 0.995  # How quickly worker stability decays
        self.CAPABILITY_WEIGHT = 0.7  # Weight of capability vs queue length in scheduling

    def validate(self):
        """Validate configuration parameters."""
        # TODO: Update this for accuracy
        if self.num_queues == 0 or self.num_workers == 0:
            print("There must be nonzero queues and workers")
            return False
        return True
        
    def __str__(self):
        return str(self.__dict__)

    @staticmethod
    def decode_object(o):
        a = SimConfig()
        a.__dict__.update(o)
        return a
