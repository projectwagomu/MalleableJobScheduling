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
from ElastiSim_Statistics import plot_jobs_gantt, plot_node_utilization
import csv


def get_csv_dict(path: str):
    try:
        with open(path) as f:
            return [row for row in csv.DictReader(f)]
    except Exception as error:
        print("Unable to calculate metrics, file %s missing", path)
    return []


def avg(l, div=None, default=None, tuple=None):
    if div is None:
        div = len(l)
    if len(l) == 0 or div == 0:
        return default
    if tuple is not None:
        l = [i[tuple] for i in l]
    return float(sum(l)) / float(div)


def generate_statistics_figures(path, scaling_factor=1):
    def execute(func, path, *in_csv, scaling_factor=1):
        out_path = path + func.__name__ + ".pdf"
        in_path = [path + i + ".csv" for i in in_csv]
        if len(in_path) == 1:
            func(in_path[0]).write_image(out_path, format="pdf", scale=scaling_factor)
        else:
            func(in_path).write_image(out_path, format="pdf", scale=scaling_factor)

    # execute(plot_system_wide_cpu, path, "cpu_utilization")
    # execute(plot_network_activity, path, "network_activity")
    # execute(plot_pfs_utilization, path, "pfs_utilization")
    # execute(plot_utilization, path, "cpu_utilization", "network_activity", "pfs_utilization")
    # execute(plot_cpu_per_node, path, "cpu_utilization")
    # execute(plot_jobs_box, path, "job_statistics")
    # execute(plot_jobs_violin, path, "job_statistics")
    execute(plot_jobs_gantt, path, "job_statistics", scaling_factor=scaling_factor)
    execute(
        plot_node_utilization, path, "node_utilization", scaling_factor=scaling_factor
    )


# calculate job statistics
def generate_job_statistics(path: str, metrics: dict):
    csv_dict = get_csv_dict(path)
    if len(csv_dict) == 0:
        return

    total_runtime = max(float(r["End Time"]) for r in csv_dict)
    metrics["total_runtime"] = total_runtime

    total_wait_time = sum(float(r["Wait Time"]) for r in csv_dict)
    metrics["average_wait_time"] = total_wait_time / len(csv_dict)

    total_turnaround_time = sum(float(r["Turnaround Time"]) for r in csv_dict)
    metrics["average_turnaround_time"] = total_turnaround_time / len(csv_dict)

    total_makespan = sum(float(r["Makespan"]) for r in csv_dict)
    metrics["average_makespan"] = total_makespan / len(csv_dict)

    job_amount = max(int(r["ID"]) for r in csv_dict)
    malleable_job_amount = sum(1 if r["Type"] == "malleable" else 0 for r in csv_dict)
    return job_amount, malleable_job_amount


# calculate node statistics
def generate_node_statistics(path: str, metrics: dict, end_node_utilization_threshold=0.9):
    csv_dict = get_csv_dict(path)
    if len(csv_dict) == 0:
        return

    node_usage = dict()
    node_state = dict()
    for row in csv_dict:
        node = row["Node"]
        time = float(row["Time"])
        current_state = node_state.setdefault(node, (0, "free"))
        if current_state[1] == "allocated":
            allocation_time = time - current_state[0]
            node_usage[node] = node_usage.setdefault(node, 0) + allocation_time
        node_state[node] = (time, row["State"])

    node_amount = len(node_usage)
    avg_node_utilization = (sum(v for v in node_usage.values()) / node_amount if node_amount != 0 else 0)
    if "total_runtime" in metrics and metrics["total_runtime"] is not None:
        total_runtime = metrics["total_runtime"]
        metrics["average_node_utilization"] = avg_node_utilization / total_runtime

    last_max = None
    current_max = node_amount
    for row in csv_dict:
        current_max += 1 if row["State"] != "free" else -1
        if current_max > node_amount * end_node_utilization_threshold:
            last_max = float(row["Time"])
    metrics["end_of_max"] = last_max


# calculate scheduler event amounts, average node amount per event, average event amount per job
def generate_event_statistics(path: str, metrics: dict, job_amount, malleable_amount=None):
    csv_dict = get_csv_dict(path)
    if len(csv_dict) == 0:
        return

    # event: (event_amount : int, avg nodes per event : list, avg events per job : dict)
    event_dict = {"SHRINK": [0, [], dict()], "EXPAND": [0, [], dict()]}
    for row in csv_dict:
        event, nodes, job = (row["Event"], row["Nodes"], row["Jobs"])
        if event not in event_dict:
            continue

        node_amount = nodes.count("N")
        event_dict[event][0] += 1
        event_dict[event][1].append(node_amount)
        if job not in event_dict[event][2]:
            event_dict[event][2][job] = 0
        event_dict[event][2][job] += 1

    for event_type, vals in event_dict.items():
        event_dict[event_type][1] = avg(vals[1], default=0)
        event_dict[event_type][2] = avg(
            vals[2].values(), div=malleable_amount or job_amount, default=0
        )

    metrics["shrink_event"] = event_dict["SHRINK"]
    metrics["expand_event"] = event_dict["EXPAND"]


def generate_statistics(path):
    metrics = dict()
    job_amount, malleable_job_amount = generate_job_statistics(path + "job_statistics.csv", metrics)
    generate_node_statistics(path + "node_utilization.csv", metrics)
    generate_event_statistics(path + "event.csv", metrics, malleable_job_amount)
    return metrics
