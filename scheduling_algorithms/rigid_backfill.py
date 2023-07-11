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
# Scheduling Algorithm FCFS with Backfilling:
# schedules jobs in order of their submission time, fills remaining space with first fitting job.
# Jobs are assigned with their maximum possible amount of nodes and will not be reassigned

from extension.ElastiSimExtension import *
from elastisim_python import JobState, NodeState, pass_algorithm
from elastisim_python import Job as ElastiSimJob, Node as ElastiSimNode, InvocationType


def schedule(jobs: list[ElastiSimJob], nodes: list[ElastiSimNode], system: dict):
    injectExtension(jobs, nodes, system)
    free_nodes = [node for node in nodes if node.state == NodeState.FREE]
    pending_jobs = [job for job in jobs if job.state is JobState.PENDING]
    running_jobs = [job for job in jobs if job.state is JobState.RUNNING]

    for job in pending_jobs:
        if len(free_nodes) == 0:
            break

        if job.num_nodes_pref <= len(free_nodes):
            nodes_to_assign = min(job.num_nodes_pref, len(free_nodes))
            job.assign(free_nodes[:nodes_to_assign])
            job.assign_num_gpus_per_node(job.num_gpus_per_node_max)
            del free_nodes[:nodes_to_assign]
            pending_jobs.remove(job)
            Logger.log_event(EventType.START, job, job.assigned_nodes)


if __name__ == "__main__":
    url = "ipc:///tmp/elastisim.ipc"
    try:
        pass_algorithm(schedule, url)
    except Exception as e:
        print("\nScheduler Error for rigid_backfill.py")
        raise e
