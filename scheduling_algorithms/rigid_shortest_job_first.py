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
# Scheduling Algorithm Shortest-Job-First:
# schedules jobs strictly in order of their estimated runtime of calculations.
# Jobs are assigned with their maximum possible amount of nodes and will not be reassigned

from elastisim_python import JobState, JobType, NodeState, pass_algorithm
from elastisim_python import Job as ElastiSimJob, Node as ElastiSimNode
from extension.ElastiSimExtension import *


def schedule(jobs: list[ElastiSimJob], nodes: list[ElastiSimNode], system: dict):
    injectExtension(jobs, nodes, system)

    free_nodes = [node for node in nodes if node.state == NodeState.FREE]
    pending_jobs = [job for job in jobs if job.state is JobState.PENDING]
    sorted_pending_jobs = sorted(pending_jobs, key=lambda job: job.get_estimated_runtime())

    for job in sorted_pending_jobs:
        if len(free_nodes) < job.num_nodes_min:
            break

        nodes_to_assign = min(len(free_nodes), job.num_nodes_max)
        job.assign(free_nodes[:nodes_to_assign])
        job.assign_num_gpus_per_node(job.num_gpus_per_node_max)
        del free_nodes[:nodes_to_assign]
        Logger.log_event(EventType.START, job, job.assigned_nodes)


if __name__ == '__main__':
    url = 'ipc:///tmp/elastisim.ipc'
    try:
        pass_algorithm(schedule, url)
    except Exception as e:
        print("\nScheduler Error for rigid_shortest_job_first.py")
        raise e
