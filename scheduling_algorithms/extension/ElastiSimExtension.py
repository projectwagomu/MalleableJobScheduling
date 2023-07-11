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
from .ElastiSimLogger import Logger, EventType
from elastisim_python import JobState, JobType, NodeState
from elastisim_python import Job as ElastiSimJob
from elastisim_python import Node as ElastiSimNode


# extended job-class, adds runtime-argument and pref_node attribute, improves debug printing
class Job(ElastiSimJob):
    def get_estimated_runtime(self):
        if "runtime" not in self.arguments:
            flops = float(self.arguments["flops"])
            iterations = float(self.arguments["iterations"]) if "iterations" in self.arguments else 1
            self.arguments["runtime"] = (flops*iterations) / self.num_nodes_min
        return float(self.arguments["runtime"])

    def __inject_estimated_num_nodes_pref(self):
        if "num_nodes_pref" not in self.arguments:
            num_pref_nodes = (self.num_nodes_min + self.num_nodes_max) // 2
            self.arguments["num_nodes_pref"] = num_pref_nodes
            Logger.log(f"Attribute num_nodes_pref missing for Job{self.identifier}")
        self.num_nodes_pref = int(self.arguments["num_nodes_pref"])

    def __on_inject(self):
        if self.type != JobType.RIGID:
            self.__inject_estimated_num_nodes_pref()
        else:
            self.num_nodes_min = self.num_nodes_max = self.num_nodes_pref = self.num_nodes
        assert self.num_nodes_min <= self.num_nodes_pref
        assert self.num_nodes_max >= self.num_nodes_pref

    def __str__(self):
        out = f"Job{self.identifier}({self.type.name}) is {self.state.name}"
        if len(self.assigned_nodes) != 0:
            out += f" with Nodes {[node.identifier for node in self.assigned_nodes]} assigned"
        return out

    def __repr__(self):
        return f"J{self.identifier}"

    @staticmethod
    def inject(jobs):
        for job in jobs:
            job.__class__ = Job
            job.__on_inject()


# extended node-class, improves debug printing
class Node(ElastiSimNode):
    def __str__(self):
        out = f"Node{self.identifier}({self.state.name})"
        if len(self.assigned_job_ids) != 0:
            out += f"for Job{self.assigned_job_ids}"
        return out

    def __repr__(self):
        return f"N{self.identifier}"

    def __on_inject(self):
        pass

    @staticmethod
    def inject(nodes):
        for node in nodes:
            node.__class__ = Node
            node.__on_inject()


# extends job/node classes provided by elastiSim
def injectExtension(jobs, nodes, system, wait_for_input=False):
    Job.inject(jobs)
    Node.inject(nodes)
    Logger.inject(system, wait_for_input)
