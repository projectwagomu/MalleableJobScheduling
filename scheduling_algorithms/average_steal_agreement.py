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
# Scheduling Algorithm Average-Steal-Agreement:
# 1. schedules job FCFS with backfilling.
# 2. if jobs are still pending, shrink malleable jobs to start more pending jobs (FCFS)
#    agreement jobs can use free nodes and steal nodes from other agreements
#    malleable jobs with most assigned nodes above pref_nodes will be shrunk first to average malleable job nodes
# 3. if nodes are unused, expand malleable jobs with most assigned nodes below perf_nodes first to average malleable job nodes
from elastisim_python import JobState, JobType, NodeState, pass_algorithm
from elastisim_python import Job as ElastiSimJob, Node as ElastiSimNode
from extension.ElastiSimExtension import *
from extension.AgreementHandler import AgreementHandler, StealAgreementHandler


agreements = StealAgreementHandler()


# priority to expand job
def get_average_job_priority(job, adjust_assigned_nodes=0):
    node_range = job.num_nodes_max - job.num_nodes_min
    current_amount = len(job.assigned_nodes) - adjust_assigned_nodes
    return (current_amount - job.num_nodes_min) / node_range


# initial allocation is based on FCFS with backfilling
def initial_allocation(p_jobs, r_jobs, f_nodes, system, easy=True):
    # checks if starting this job delays queue head.
    def delays_head(job, req_nodes, head, running_jobs, free_nodes, system):
        if job == head:
            return False
        time = float(system["time"])
        def remaining_runtime(j): return j.start_time + j.get_estimated_runtime() - time
        nodes_needed, head_start_time = req_nodes - len(free_nodes), time
        for rj in sorted(running_jobs, key=remaining_runtime):
            if nodes_needed <= 0:
                break
            nodes_needed -= len(rj.assigned_nodes)
            head_start_time = time + remaining_runtime(rj)
        return nodes_needed <= 0 and head_start_time < head.get_estimated_runtime()

    for job in p_jobs:
        if len(f_nodes) == 0:
            break

        req_nodes = job.num_nodes_min
        if req_nodes <= len(f_nodes):
            if easy and delays_head(job, req_nodes, p_jobs[0], r_jobs, f_nodes, system):
                continue
            job.assign(f_nodes[:req_nodes])
            job.assign_num_gpus_per_node(job.num_gpus_per_node_max)
            del f_nodes[:req_nodes]
            p_jobs.remove(job)
            Logger.log_event(EventType.START, job, job.assigned_nodes)


# returns a node that can be reallocated from this running malleable job and was not selected previously
def available_node(job: Job, shrink_nodes: dict, agreements):
    pre_shrunk_nodes = [n for vs in shrink_nodes.values() for n in vs]
    nodes = [n for n in job.assigned_nodes if not n in pre_shrunk_nodes]
    nodes = [n for n in nodes if not agreements.has_agreement(n)]
    return nodes[job.num_nodes_min] if len(nodes) > job.num_nodes_min else None


# calculate a list of nodes with a size of required_nodes that can by reallocated from running
# malleable jobs with highest percentage node usage will be selected first
def select_shrink_jobs(rm_jobs: list[Job], required_nodes: int, agreements):
    shrink_nodes = {j: [] for j in rm_jobs}
    for _ in range(required_nodes):
        job = max(
            filter(
                lambda j: available_node(j, shrink_nodes, agreements) is not None,
                rm_jobs,
            ),
            key=lambda j: get_average_job_priority(j, -len(shrink_nodes[j])),
            default=None,
        )
        if job is None:  # cancel if no more malleable jobs can be shrunk
            return dict()

        node = available_node(job, shrink_nodes, agreements)
        shrink_nodes[job] += [node] if node is not None else []
    return shrink_nodes


# shrinks running malleable jobs if those nodes can run pending jobs
# malleable jobs with highest percentage node usage will be selected first
def schedule_pending_job(p_jobs: list[Job], rm_jobs: list[Job], agreements):
    # start pending jobs with min node amount, backfilling
    for p_job in p_jobs:
        shrink_jobs = select_shrink_jobs(rm_jobs, p_job.num_nodes_min, agreements)
        for shrink_job, nodes in shrink_jobs.items():
            if len(nodes) == 0:
                continue
            agreements.add_agreement(p_job, nodes)
            Logger.log_event(EventType.AGREEMENT_ADDED, (shrink_job, p_job), nodes)
            shrink_job.remove(nodes)
            Logger.log_event(EventType.SHRINK, shrink_job, nodes)


# expands malleable jobs with all remaining free nodes. tries to average out the node usage between all
# malleable jobs with lowest percentage node usage will be selected first
def expand_running_malleable_jobs(rm_jobs: list[Job], free_nodes: list[Node]):
    # calculate node expand amount per job
    expand_amount = {j: 0 for j in rm_jobs}
    for _ in range(len(free_nodes)):
        job = min(rm_jobs, key=lambda j: get_average_job_priority(j, expand_amount[j]))
        if len(job.assigned_nodes) == job.num_nodes_max:
            break
        expand_amount[job] += 1

    # apply calculated node expand amount per job
    for job, node_amount in expand_amount.items():
        if node_amount == 0:
            continue
        amount = min(job.num_nodes_max - len(job.assigned_nodes), expand_amount[job])
        node_to_assign = free_nodes[:amount]
        job.assign(node_to_assign)
        del free_nodes[:amount]
        Logger.log_event(EventType.EXPAND, job, node_to_assign)


def schedule(jobs: list[ElastiSimJob], nodes: list[ElastiSimNode], system: dict):
    injectExtension(jobs, nodes, system)
    global agreements

    # filter jobs and nodes
    p_jobs = [j for j in jobs if j.state is JobState.PENDING]
    r_jobs = [j for j in jobs if j.state is JobState.RUNNING]
    rm_jobs = [j for j in r_jobs if j.type is JobType.MALLEABLE]
    f_nodes = [n for n in nodes if n.state is NodeState.FREE]

    # schedule jobs with agreements first
    agreements.resolve_agreements(p_jobs, f_nodes)

    # remove pending jobs and free nodes with existing agreements
    free_nodes = [n for n in f_nodes if not agreements.has_agreement(n)]
    pending_jobs = [j for j in p_jobs if not agreements.has_agreement(j)]

    # schedule initial allocation
    initial_allocation(pending_jobs, r_jobs, free_nodes, system)

    # schedule pending jobs by shrinking malleable jobs
    if len(pending_jobs) > 0 and len(rm_jobs) > 0:
        schedule_pending_job(pending_jobs, rm_jobs, agreements)

    # expand running malleable jobs if possible
    if len(free_nodes) > 0 and len(rm_jobs) > 0:
        expand_running_malleable_jobs(rm_jobs, free_nodes)


if __name__ == "__main__":
    url = "ipc:///tmp/elastisim.ipc"
    try:
        pass_algorithm(schedule, url)
    except Exception as e:
        print("\nScheduler Error for average_steal_agreement.py")
        raise e
