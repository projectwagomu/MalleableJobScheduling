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
# Scheduling Algorithm Easy Backfilling:
# schedules jobs in order of their submission time, fills remaining space with first fitting job if they do not delay the first job in queue.
# Jobs are assigned with their maximum possible amount of nodes and will not be reassigned

from extension.ElastiSimExtension import *
from elastisim_python import JobState, NodeState, pass_algorithm
from elastisim_python import Job as ElastiSimJob, Node as ElastiSimNode, InvocationType

# checks if starting this job delays queue head.
def delays_head(job, req_nodes, head, running_jobs, free_nodes, system):
    if job == head:
        return False
    time = float(system["time"])
    remaining_runtime = lambda j: j.start_time + j.get_estimated_runtime() - time
    nodes_needed, head_start_time = req_nodes - len(free_nodes), time
    for rj in sorted(running_jobs, key=remaining_runtime):
        if nodes_needed <= 0:
            break
        nodes_needed -= len(rj.assigned_nodes)
        head_start_time = time + remaining_runtime(rj)
    return nodes_needed <= 0 and head_start_time < head.get_estimated_runtime()


def schedule(jobs: list[ElastiSimJob], nodes: list[ElastiSimNode], system: dict):
    injectExtension(jobs, nodes, system)
    free_nodes = [node for node in nodes if node.state == NodeState.FREE]
    p_jobs = [job for job in jobs if job.state is JobState.PENDING]
    r_jobs = [job for job in jobs if job.state is JobState.RUNNING]

    for job in p_jobs:
        if len(free_nodes) == 0:
            break

        req_nodes = job.num_nodes_pref
        if req_nodes <= len(free_nodes):
            if delays_head(job, req_nodes, p_jobs[0], r_jobs, free_nodes, system):
                continue
            job.assign(free_nodes[:req_nodes])
            job.assign_num_gpus_per_node(job.num_gpus_per_node_max)
            del free_nodes[:req_nodes]
            p_jobs.remove(job)
            Logger.log_event(EventType.START, job, job.assigned_nodes)


if __name__ == "__main__":
    url = "ipc:///tmp/elastisim.ipc"
    try:
        pass_algorithm(schedule, url)
    except Exception as e:
        print("\nScheduler Error for rigid_easy_backfill.py")
        raise e
