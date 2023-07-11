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
from extension.ElastiSimExtension import *


# Used to enable asynchronous node reassignment
class AgreementHandler:
    def __init__(self):
        self.job_dict = dict()
        self.node_dict = dict()

    # Adds an agreement to resolve later
    def add_agreement(self, job: Job, nodes: list[Node]):
        if job.identifier not in self.job_dict:
            self.job_dict[job.identifier] = set()
        self.job_dict[job.identifier].update({n.identifier for n in nodes})
        for node in nodes:
            self.node_dict[node.identifier] = job.identifier

    # Removes an agreement
    def remove_agreement(self, job: Job, node_ids=None):
        if job.identifier in self.job_dict:
            node_ids = node_ids or self.job_dict.pop(job.identifier)
            for node_id in node_ids:
                self.node_dict.pop(node_id)

    # Applies the agreement by starting the job with the specified amount of nodes
    def apply_agreement(self, job_to_start, nodes_to_assign, p_jobs, f_nodes, node_ids_to_remove=None):
        job_to_start.assign(nodes_to_assign)
        job_to_start.assign_num_gpus_per_node(job_to_start.num_gpus_per_node_max)

        p_jobs.remove(job_to_start)
        for node in nodes_to_assign:
            f_nodes.remove(node)
        Logger.log_event(EventType.AGREEMENT_FULLFILLED, job_to_start, job_to_start.assigned_nodes)

    # Method to resolve the agreement, different strategies are provided below
    def resolve_agreements(self, p_jobs: list[Job], f_nodes: list[Node]):
        raise NotImplementedError("abstract method")

    # Checks if a job/node is part of an agreement
    def has_agreement(self, obj):
        if type(obj) is Job:
            return obj.identifier in self.job_dict
        elif type(obj) is Node:
            return obj.identifier in self.node_dict
        else:
            raise TypeError("unknown agreement-object")

    # Returns the list of nodes that should be used to start the given job
    def get_job_agreement_nodes(self, job: Job):
        return self.job_dict[job.identifier]


# Resolves the agreement exactly in the order and assignment they are stored in
class DirectAgreementHandler(AgreementHandler):
    def resolve_agreements(self, p_jobs: list[Job], f_nodes: list[Node]):
        target_jobs = [j for j in p_jobs if self.has_agreement(j)]
        for job in target_jobs:
            free_agreement_nodes = [n for n in f_nodes if n.identifier in self.job_dict[job.identifier]]
            if len(self.get_job_agreement_nodes(job)) == len(free_agreement_nodes):
                self.apply_agreement(job, free_agreement_nodes, p_jobs, f_nodes)
                self.remove_agreement(job)


# Resolves the agreement exactly in the order and assignment they are stored in
# Allows a job to used free nodes of other jobs with assignment by stealing them
class StealAgreementHandler(AgreementHandler):
    def swap_nodes(self, node1_id, node2_id):
        job1_id = self.node_dict[node1_id]
        job2_id = self.node_dict[node2_id]
        # swap nodes
        self.job_dict[job1_id].remove(node1_id)
        self.job_dict[job1_id].add(node2_id)
        self.job_dict[job2_id].remove(node2_id)
        self.job_dict[job2_id].add(node1_id)
        # swap jobs
        self.node_dict[node1_id] = job2_id
        self.node_dict[node2_id] = job1_id

    def steal_agreement_nodes(self, job: Job, free_agreement_nodes: list[Node]):
        agree_node_ids = self.job_dict[job.identifier]
        free_agreement_node_ids = [n.identifier for n in free_agreement_nodes]
        used_agree_node_ids = [nid for nid in agree_node_ids if nid not in free_agreement_node_ids]
        free_other_agree_node_ids = [nid for nid in free_agreement_node_ids if nid not in agree_node_ids]

        # steal/swap nodes
        for used_node_id, free_other_node_id in zip(used_agree_node_ids, free_other_agree_node_ids):
            self.swap_nodes(used_node_id, free_other_node_id)

    def resolve_agreements(self, p_jobs: list[Job], f_nodes: list[Node]):
        target_jobs = [j for j in p_jobs if self.has_agreement(j)]
        for job in target_jobs:
            free_agreement_nodes = [n for n in f_nodes if self.has_agreement(n)]
            if len(free_agreement_nodes) == 0:
                break

            if len(self.get_job_agreement_nodes(job)) <= len(free_agreement_nodes):
                self.steal_agreement_nodes(job, free_agreement_nodes)
                nodes_to_assign = [n for n in free_agreement_nodes if n.identifier in self.get_job_agreement_nodes(job)]
                self.apply_agreement(job, nodes_to_assign, p_jobs, f_nodes)
                self.remove_agreement(job)


# Resolves the agreement by using all available free nodes
# Allows a job to used free nodes that do not have an agreement
class TEMPPoolAgreementHandler(AgreementHandler):
    def get_nodes_from_pool(self, job: Job, free_nodes: list[Node]):
        free_agree_nodes = [n for n in free_nodes if n.identifier in self.node_dict]
        free_agree_node_ids = [n.identifier for n in free_agree_nodes]
        nodes_needed = len(self.get_job_agreement_nodes(job))

        # free nodes with argeements
        nodes_to_assign = free_agree_nodes[:nodes_needed]
        node_ids_to_remove_from_agreement = free_agree_node_ids[:nodes_needed]

        # use free nodes without agreements if required
        additional_nodes_needed = nodes_needed - len(free_agree_nodes)
        if additional_nodes_needed > 0:
            assigned_agree_node_ids = [nid for nid in self.node_dict if nid not in free_agree_nodes]
            free_nodes_without_agreement = [n for n in free_nodes if n not in free_agree_nodes]
            # add free nodes without argeements and free up not free agreement nodes instead
            nodes_to_assign += free_nodes_without_agreement[:additional_nodes_needed]
            node_ids_to_remove_from_agreement += assigned_agree_node_ids[:additional_nodes_needed]
        return nodes_to_assign, node_ids_to_remove_from_agreement

    def resolve_agreements(self, p_jobs: list[Job], f_nodes: list[Node]):
        target_jobs = [j for j in p_jobs if self.has_agreement(j)]
        for job in target_jobs:
            if len(f_nodes) == 0:
                break

            if len(self.get_job_agreement_nodes(job)) <= len(f_nodes):
                nodes_to_assign, node_ids_to_remove = self.get_nodes_from_pool(job, f_nodes)
                self.apply_agreement(job, nodes_to_assign, p_jobs, f_nodes, node_ids_to_remove)


# Resolves the agreement by using all available free nodes
# Allows a job to used free nodes that do not have an agreement
class PoolAgreementHandler(AgreementHandler):
    def get_nodes_from_pool(self, job: Job, free_nodes: list[Node]):
        free_nodes_with_agreement = [n for n in free_nodes if n.identifier in self.node_dict]
        free_node_ids_with_agreement = [n.identifier for n in free_nodes_with_agreement]
        free_nodes_without_agreement = [n for n in free_nodes if n not in free_nodes_with_agreement]
        nodes_needed = len(self.get_job_agreement_nodes(job))

        # use free agreement nodes and if required free nodes without agreement
        nodes_to_assign = free_nodes_with_agreement[:nodes_needed]
        nodes_to_assign += free_nodes_without_agreement[:max(0, nodes_needed - len(nodes_to_assign))]

        # remove job/node agreemnet
        self.job_dict.pop(job.identifier)
        for node in nodes_to_assign:
            if node.identifier in free_node_ids_with_agreement:
                self.node_dict.pop(node.identifier)
            else:
                self.node_dict.popitem()
        return nodes_to_assign

    def resolve_agreements(self, p_jobs: list[Job], f_nodes: list[Node]):
        target_jobs = [j for j in p_jobs if self.has_agreement(j)]
        for job in target_jobs:
            if len(f_nodes) == 0:
                break

            if len(self.get_job_agreement_nodes(job)) <= len(f_nodes):
                nodes_to_assign = self.get_nodes_from_pool(job, f_nodes)
                self.apply_agreement(job, nodes_to_assign, p_jobs, f_nodes)
