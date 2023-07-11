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
# Scheduling Algorithm Pref-Common-Pool:
# 1. schedules job FCFS with backfilling, tries to assign pref_nodes first.
# 2. if jobs are still pending, shrink malleable jobs to start more pending jobs (FCFS), tries to assign pref_nodes first
#    Uses a global pool of reserved nodes to be used by the reserved jobs
# 3. if nodes are unused, expand malleable jobs, expands all jobs to pref nodes first, than further to max nodes
from elastisim_python import JobState, JobType, NodeState, pass_algorithm
from elastisim_python import Job as ElastiSimJob, Node as ElastiSimNode
from extension.ElastiSimExtension import *
from extension.AgreementHandler import AgreementHandler, PoolAgreementHandler


agreements = PoolAgreementHandler()


# priority to expand job
def get_pref_job_priority(job):
    return len(job.assigned_nodes) - job.num_nodes_pref


# initial allocation is based on FCFS with backfilling
# tries to assign pref num of nodes else closest amount to pref
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

        if job.num_nodes_min <= len(f_nodes):
            # use all available nodes up to pref
            req_nodes = min(job.num_nodes_pref, len(f_nodes))
            if easy and delays_head(job, req_nodes, p_jobs[0], r_jobs, f_nodes, system):
                continue

            job.assign(f_nodes[:req_nodes])
            job.assign_num_gpus_per_node(job.num_gpus_per_node_max)
            del f_nodes[:req_nodes]
            p_jobs.remove(job)
            Logger.log_event(EventType.START, job, job.assigned_nodes)


# returns a list of nodes with a maximum size of required_nodes that can be reallocated from this running malleable job
# keeps at least keep_node amount for each malleable job
def allocate_resources(job: Job, required_nodes, node_target, agreements):
    if node_target(job) >= len(job.assigned_nodes):
        return []

    allocated_nodes = []
    for node in job.assigned_nodes[node_target(job):]:
        if required_nodes == 0:
            break

        if not agreements.has_agreement(node):
            allocated_nodes.append(node)
            required_nodes -= 1
    return allocated_nodes


# calculate a list of nodes with a maximum size of required_nodes that can by reallocated from running malleable jobs
# malleable jobs with the most nodes above pref_nodes will be shrunk first
def select_shrink_jobs(rm_jobs: list[Job], required_nodes, n_amount, agreements):
    jobs_to_shrink = dict()
    for job in sorted(rm_jobs, key=get_pref_job_priority, reverse=True):
        nodes_to_shrink = allocate_resources(job, required_nodes, n_amount, agreements)
        if len(nodes_to_shrink) > 0:
            required_nodes -= len(nodes_to_shrink)
            jobs_to_shrink[job] = nodes_to_shrink
    return jobs_to_shrink if required_nodes == 0 else dict()


# shrinks running malleable jobs if those nodes can run pending jobs
# First tries to allocate pref_nodes to the pending job and keep pref_nodes for each malleable job
# If this is not possible, try allocating min_nodes and keep pref_nodes
# else try allocating min_nodes and keeping only min_nodes
def schedule_pending_job(pending_jobs: list[Job], rm_jobs: list[Job], agreements):
    for job in pending_jobs:
        shrinkables = (
            select_shrink_jobs(
                rm_jobs, job.num_nodes_pref, lambda j: j.num_nodes_pref, agreements
            )
            or select_shrink_jobs(
                rm_jobs, job.num_nodes_min, lambda j: j.num_nodes_pref, agreements
            )
            or select_shrink_jobs(
                rm_jobs, job.num_nodes_min, lambda j: j.num_nodes_min, agreements
            )
            or dict()
        )
        for s_job, nodes in shrinkables.items():
            agreements.add_agreement(job, nodes)
            Logger.log_event(EventType.AGREEMENT_ADDED, (s_job, job), nodes)
            s_job.remove(nodes)
            Logger.log_event(EventType.SHRINK, s_job, nodes)


# expands malleable jobs with all remaining free nodes to pref nodes
# and to max_nodes if all malleable jobs are already expanded to pref.
# jobs with the highest difference to num_nodes_pref will be expanded first
def expand_running_malleable_jobs(rm_jobs: list[Job], free_nodes, n_amount):
    for rm_job in sorted(rm_jobs, key=get_pref_job_priority):
        if len(free_nodes) == 0:
            break

        new_nodes = n_amount(rm_job) - len(rm_job.assigned_nodes)
        if new_nodes > 0:
            node_amount_to_assign = min(new_nodes, len(free_nodes))
            nodes_to_assign = free_nodes[:node_amount_to_assign]
            rm_job.assign(nodes_to_assign)
            del free_nodes[:node_amount_to_assign]
            Logger.log_event(EventType.EXPAND, rm_job, nodes_to_assign)


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
        expand_running_malleable_jobs(rm_jobs, free_nodes, lambda j: j.num_nodes_pref)
        expand_running_malleable_jobs(rm_jobs, free_nodes, lambda j: j.num_nodes_max)


if __name__ == "__main__":
    url = "ipc:///tmp/elastisim.ipc"
    try:
        pass_algorithm(schedule, url)
    except Exception as e:
        print("\nScheduler Error for pref_common_pool.py")
        raise e
