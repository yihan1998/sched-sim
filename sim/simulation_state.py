#!/usr/bin/env python
"""Object to maintain simulation state."""

import math
import datetime
import random

from timer import Timer
# from work_search_state import WorkSearchState
# from tasks import EnqueuePenaltyTask, Task
# from sim_thread import Thread
# from sim_queue import Queue
import progress_bar as progress


class SimulationState:
    """Object to maintain simulation state as time passes."""
    def __init__(self, config):
        # Simulation Global Variables
        self.timer = Timer()
