# ---------------------------------------------------------------------
# Copyright (c) 2023 Wagomu project.
#
# This program and the accompanying materials are made available to you under
# the terms of the Eclipse Public License 1.0 which accompanies this
# distribution,
# and is available at https://www.eclipse.org/legal/epl-v20.html
#
# SPDX-License-Identifier: EPL-2.0
# ---------------------------------------------------------------------
from elastisim_python import JobState, JobType, NodeState
import csv
import os.path
from enum import Enum


# event-type to log
class EventType(Enum):
    START = 0
    EXPAND = 1
    SHRINK = 2
    STOP = 3
    KILL = 4
    AGREEMENT_ADDED = 5
    AGREEMENT_FULLFILLED = 6


# Logger class to log events, debug and print the system state
class Logger:
    print_debug_messages = False
    print_events = False
    print_system_state_on_change = False

    def inject(system, wait_for_input=False):
        Logger.time = int(system["time"])
        Logger.wait_for_input = wait_for_input

    def __print(string, print_flag=True):
        if not print_flag:
            return

        print(string)
        if Logger.wait_for_input:
            try:
                input()
            except:
                Logger.wait_for_input = False

    def log_debug_message(message):
        Logger.__print(message, Logger.print_debug_messages)

    def __write_next_row_in_event_csv(row, file="data/output/event.csv"):
        if not os.path.isfile(file):
            with open(file, "x") as f:
                writer = csv.writer(f)
                writer.writerow(["Time", "Event", "Jobs", "Nodes"])
                f.close()

        with open(file, "a") as f:
            writer = csv.writer(f)
            writer.writerow(row)
            f.close()

    def log_event(event: EventType, job, nodes, *args):
        time = str(Logger.time) if Logger.time is not None else ""
        job_string = f"{job[0].__repr__()} -> {job[1].__repr__()}" if type(job) in (tuple, list) else job.__repr__()

        new_event_row = [time, event.name] + [job_string, f"{nodes}"] + list(args)
        Logger.__write_next_row_in_event_csv(new_event_row)
        Logger.__print(new_event_row, Logger.print_events)

    def log_system(jobs, nodes, system, additional_information="", seperation_line="-" * 100):
        # log jobs
        out = f"\nJobs{seperation_line[:-4]}\n"
        jobs_by_state = {state: [] for state in JobState}
        for job in jobs:
            jobs_by_state[job.state].append(job)

        for state, state_jobs in jobs_by_state.items():
            if len(state_jobs) > 0:
                out += f"{state.name}: {str(state_jobs)}\n"

        # log nodes
        out += f"\nNODES{ separate_symbol*(separation_len-5)}\n"
        nodes_by_state = {state: [] for state in NodeState}
        for node in nodes:
            nodes_by_state[node.state].append(node)

        for state, state_nodes in nodes_by_state.items():
            if len(state_nodes) > 0:
                out += f"{state.name}: {str(state_nodes)}\n"

        # log additional information
        out += f"additional_information\n"

        if Logger.system_output != out:
            Logger.system_output = out
            system_row = f"\nSystem State(t={round(Logger.time)}):"
            out = f"{system_row}{seperation_line[:-len(system_row)]}\n{out}\n"
            Logger.__print(out, Logger.print_system_state_on_change)
