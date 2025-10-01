import random

class SimConfig:
    """Object to hold all configuration state of the simulation. Remains constant."""
    def __init__(self, name=None, num_workers=None, mapping=[], 
                 avg_system_load=None, sim_duration=None):
        # Basic simulation parameters
        self.name = name
