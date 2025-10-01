#!/usr/bin/env python
"""Creates a runs a simulation."""

import logging
import random
import os
import json
import math
import sys
import datetime
import pathlib

# from simulation_state import SimulationState
# from sim_thread import Thread
# from tasks import Task
import progress_bar as progress
from sim_config import SimConfig

SINGLE_THREAD_SIM_NAME_FORMAT = "{}_{}"
MULTI_THREAD_SIM_NAME_FORMAT = "{}_{}_t{}"
RESULTS_DIR = "{}/results/"
META_LOG_FILE = "{}/results/meta_log"
CONFIG_LOG_DIR = "{}/config_records/"


class Simulation:
    """Runs the simulation based on the simulation state."""
    def __init__(self, configuration, sim_dir_path):
        self.config = configuration
        self.state = SimulationState(configuration)
        self.sim_dir_path = sim_dir_path