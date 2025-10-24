import random

class SimConfig:
    """Object to hold all configuration state of the simulation. Remains constant."""
    def __init__(self, name=None, num_queues=None, num_workers=None, mapping=[], 
                 avg_system_load=None, initial_num_tasks=None, sim_duration=None,
                 constant_service_time=False, bimodal_service_time=False, pareto_service_time=False, normal_service_time=False,
                 pb_enabled=True,
                 gittins_index=False, process_sharing=False, shortest_remaining_job_first=False, first_come_first_serve=False,
                 global_queue=False, preemption=False, join_shortest_queue=False):
        # Basic simulation parameters
        self.name = name
        self.num_queues = num_queues
        self.num_workers = num_workers
        self.mapping = list(mapping)
        self.avg_system_load = avg_system_load
        self.num_tasks = initial_num_tasks
        self.sim_duration = sim_duration

        self.constant_service_time = constant_service_time
        self.bimodal_service_time = bimodal_service_time
        self.pareto_service_time = pareto_service_time
        self.normal_service_time = normal_service_time
        self.progress_bar = pb_enabled

        self.gittins_index = gittins_index
        self.process_sharing = process_sharing
        self.shortest_remaining_job_first = shortest_remaining_job_first
        self.first_come_first_serve = first_come_first_serve

        self.global_queue = global_queue
        self.join_shortest_queue = join_shortest_queue
        self.preemption = preemption

        self.AVERAGE_SERVICE_TIME = 1000
        self.PREEMPTION_TIME = 1000
        self.GLOBAL_PREEMPTION_TIME = 2000
        self.PARETO_SHAPE_PARAMETER = 1.5
        self.NORMAL_STD_DEV = 300

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
