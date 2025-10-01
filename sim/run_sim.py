from simulation import Simulation, RESULTS_DIR, META_LOG_FILE, CONFIG_LOG_DIR, SINGLE_THREAD_SIM_NAME_FORMAT, \
    MULTI_THREAD_SIM_NAME_FORMAT
from sim_config import SimConfig

import logging
import sys
import os
from datetime import datetime
import multiprocessing
import json
import pathlib

class SimProcess(multiprocessing.Process):
    def run(self):
        print("Starting " + self.name)
        simulation = Simulation(self.config, self.sim_path)
        simulation.run()
        simulation.save_stats()
        print("Exiting " + self.name)

if __name__ == "__main__":
    time = datetime.now().strftime("%y-%m-%d_%H:%M:%S")
    loads = list(range(10, 110, 10))
    threads = []
    cores = None
    description = ""

    path_to_sim = os.path.relpath(pathlib.Path(__file__).resolve().parents[1], start=os.curdir)

    if os.path.isfile(sys.argv[1]):
        cfg_json_fp = open(sys.argv[1], "r")
        cfg_json = cfg_json_fp.read()
        cfg_json_fp.close()

        if "-d" in sys.argv:
            logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(message)s', filename='debug.log', filemode='w')
            sys.argv.remove("-d")

    if cores is not None:
        for i, load in enumerate(loads):
            name = MULTI_THREAD_SIM_NAME_FORMAT.format(os.uname().nodename, time, i)

            if os.path.isfile(sys.argv[1]):
                cfg = json.loads(cfg_json, object_hook=SimConfig.decode_object)
                if cfg.reallocation_replay:
                    name_parts = cfg.reallocation_record.split("_", 1)
                    cfg.reallocation_record = MULTI_THREAD_SIM_NAME_FORMAT.format(name_parts[0], name_parts[1], i)
                cfg.avg_system_load = load / 100
                cfg.name = name
                cfg.progress_bar = (i == 0)
                cfg.description = description

            else:
                print("Missing or invalid argument")
                exit(1)

            threads.append(SimProcess(i, name, cfg, path_to_sim))