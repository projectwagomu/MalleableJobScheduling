# ---------------------------------------------------------------------
# Copyright (c) 2023 Wagomu project.
#
# This program and the accompanying materials are made available to you under
# the terms of the Eclipse Public License 1.0 which accompanies this
# distribution,
# and is available at https://www.eclipse.org/legal/epl-v20.html
#
# SPDX-License-Identifier: EPL-2.0
# Modified version of: https://github.com/elastisim/result-visualizations/blob/main/visualizations.ipynb
# ---------------------------------------------------------------------
import pandas as pd
import plotly.express as px


def read_job_statistics(path):
    job_statistics = pd.read_csv(path)
    job_statistics["Submit Time"] = job_statistics["Submit Time"] / 60
    job_statistics["Start Time"] = job_statistics["Start Time"] / 60
    job_statistics["End Time"] = job_statistics["End Time"] / 60
    job_statistics["Wait Time"] = job_statistics["Wait Time"] / 60
    job_statistics["Turnaround Time"] = job_statistics["Turnaround Time"] / 60
    job_statistics["Makespan"] = job_statistics["Makespan"] / 60
    return job_statistics


def plot_system_wide_cpu(path, output=False, start=False, end=False, window=1):
    cpu_utilization = pd.read_csv(path, index_col="Time")
    cpu_utilization.index = cpu_utilization.index / 60
    if start:
        cpu_utilization = cpu_utilization.loc[cpu_utilization.index >= start].dropna()
    if end:
        cpu_utilization = cpu_utilization.loc[cpu_utilization.index <= end].dropna()
    cpu_utilization = cpu_utilization.sum(axis=1) / len(cpu_utilization.columns)
    cpu_utilization *= 100
    cpu_utilization = pd.DataFrame({"CPU": cpu_utilization})
    if window > 1:
        cpu_utilization = cpu_utilization.rolling(window).mean()
    fig = px.line(
        cpu_utilization,
        labels={
            "Time": "Time (m)",
            "value": "Utilization [%]",
        },
        title="System-wide CPU Utilization",
    )
    fig.update_layout(showlegend=False)
    if output:
        fig.write_image(output)
    return fig


def plot_network_activity(path, output=False, start=False, end=False, window=1):
    network_activity = pd.read_csv(path, index_col="Time")
    network_activity.index = network_activity.index / 60
    if start:
        network_activity = network_activity.loc[
            network_activity.index >= start
        ].dropna()
    if end:
        network_activity = network_activity.loc[network_activity.index <= end].dropna()
    network_activity *= 100
    if window > 1:
        network_activity = network_activity.rolling(window).mean()
    fig = px.line(
        network_activity,
        labels={"Time": "Time (m)", "value": "Activity [%]"},
        title="Network Activity",
    )
    fig.update_layout(showlegend=False)
    if output:
        fig.write_image(output)
    return fig


def plot_pfs_utilization(path, output=False, start=False, end=False, window=1):
    pfs_utilization = pd.read_csv(path, index_col="Time")
    pfs_utilization.index = pfs_utilization.index / 60
    if start:
        pfs_utilization = pfs_utilization.loc[pfs_utilization.index >= start].dropna()
    if end:
        pfs_utilization = pfs_utilization.loc[pfs_utilization.index <= end].dropna()
    pfs_utilization["Read"] = pfs_utilization["Read"] / 1024 / 1024 / 1024
    pfs_utilization = pfs_utilization.drop(columns=["Read (rel.)", "Write (rel.)"])
    if window > 1:
        pfs_utilization["Read"] = pfs_utilization["Read"].rolling(window).mean()
    pfs_utilization["Write"] = pfs_utilization["Write"] / 1024 / 1024 / 1024
    if window > 1:
        pfs_utilization["Write"] = pfs_utilization["Write"].rolling(window).mean()
    fig = px.line(
        pfs_utilization,
        labels={"Time": "Time (m)", "value": "Bandwidth [GiB/s]", "variable": "Link"},
        title="PFS Utilization",
    )
    if output:
        fig.write_image(output)
    return fig


def plot_utilization(path, output=False, start=False, end=False, window=1):
    cpu_utilization_path = path[0]
    network_activity_path = path[1]
    pfs_utilization_path = path[2]
    cpu_utilization = pd.read_csv(cpu_utilization_path, index_col="Time")
    cpu_utilization.index = cpu_utilization.index / 60
    network_activity = pd.read_csv(network_activity_path, index_col="Time")
    network_activity.index = network_activity.index / 60
    pfs_utilization = pd.read_csv(pfs_utilization_path, index_col="Time")
    pfs_utilization.index = pfs_utilization.index / 60

    if start:
        cpu_utilization = cpu_utilization.loc[cpu_utilization.index >= start].dropna()
        network_activity = network_activity.loc[
            network_activity.index >= start
        ].dropna()
        pfs_utilization = pfs_utilization.loc[pfs_utilization.index >= start].dropna()
    if end:
        cpu_utilization = cpu_utilization.loc[cpu_utilization.index <= end].dropna()
        network_activity = network_activity.loc[network_activity.index <= end].dropna()
        pfs_utilization = pfs_utilization.loc[pfs_utilization.index <= end].dropna()

    cpu_utilization = cpu_utilization.sum(axis=1) / len(cpu_utilization.columns)
    cpu_utilization = pd.DataFrame({"CPU": cpu_utilization})

    network_activity = network_activity.rename(columns={"Utilization": "Network"})

    pfs_utilization = pfs_utilization.drop(columns=["Read", "Write"])
    pfs_utilization = pfs_utilization.rename(columns={"Read (rel.)": "PFS Read"})
    pfs_utilization = pfs_utilization.rename(columns={"Write (rel.)": "PFS Write"})

    utilization = pd.concat(
        [cpu_utilization, network_activity, pfs_utilization], axis=1
    )
    utilization = utilization * 100
    if window > 1:
        utilization["CPU"] = utilization["CPU"].rolling(window).mean()
        utilization["Network"] = utilization["Network"].rolling(window).mean()
        utilization["PFS Read"] = utilization["PFS Read"].rolling(window).mean()
        utilization["PFS Write"] = utilization["PFS Write"].rolling(window).mean()

    fig = px.line(
        utilization,
        labels={"Time": "Time (m)", "value": "Utilization [%]", "variable": "Resource"},
        title="System Utilization",
    )
    if output:
        fig.write_image(output)
    return fig


def plot_cpu_per_node(path, output=False, start=False, end=False, window=1):
    cpu_utilization = pd.read_csv(path, index_col="Time")
    cpu_utilization.index = cpu_utilization.index / 60
    if start:
        cpu_utilization = cpu_utilization.loc[cpu_utilization.index >= start].dropna()
    if end:
        cpu_utilization = cpu_utilization.loc[cpu_utilization.index <= end].dropna()
    cpu_utilization = cpu_utilization * 100
    if window > 1:
        cpu_utilization = cpu_utilization.rolling(window).mean()
    fig = px.line(
        cpu_utilization,
        labels={
            "Time": "Time (m)",
            "value": "Utilization [%]",
            "variable": "Node Name",
        },
        title="CPU Utilization",
    )
    if output:
        fig.write_image(output)
    return fig


def plot_jobs_gantt(path, output=False):
    job_statistics = pd.read_csv(path)
    job_statistics["Submit Time"] = job_statistics["Submit Time"] / 60
    job_statistics["Start Time"] = job_statistics["Start Time"] / 60
    job_statistics["End Time"] = job_statistics["End Time"] / 60
    job_statistics["Wait Time"] = job_statistics["Wait Time"] / 60
    job_statistics["Turnaround Time"] = job_statistics["Turnaround Time"] / 60
    job_statistics["Makespan"] = job_statistics["Makespan"] / 60

    job_statistics["Job State"] = "running"
    waiting_times = job_statistics.copy()
    waiting_times["Job State"] = "waiting"
    waiting_times["Start Time"] = job_statistics["Submit Time"]
    waiting_times["End Time"] = job_statistics["Start Time"]
    waiting_times["Makespan"] = job_statistics["Wait Time"]
    job_statistics = job_statistics.drop(
        columns=["Submit Time", "Wait Time", "Turnaround Time", "Status"]
    )
    waiting_times = waiting_times.drop(
        columns=["Submit Time", "Wait Time", "Turnaround Time", "Status"]
    )
    job_statistics = pd.concat([job_statistics, waiting_times])

    fig = px.timeline(
        job_statistics,
        x_start="Start Time",
        x_end="End Time",
        y="ID",
        color="Job State",
        title="Job Runtimes",
    )
    fig.data[0].x = job_statistics["Makespan"].iloc[0 : len(job_statistics) // 2]
    fig.data[1].x = job_statistics["Makespan"].iloc[len(job_statistics) // 2 :]
    fig.data[1].opacity = 0.5
    fig.update_xaxes(type="linear", title="Time (m)")
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(bargap=0, legend={"bgcolor": "rgba(255,255,255,0.2)"})
    fig.update_traces(marker_line_width=0)
    if output:
        fig.write_image(output)
    return fig


def plot_jobs_box(path, output=False):
    job_statistics = pd.read_csv(path, index_col="ID")
    job_statistics["Submit Time"] = job_statistics["Submit Time"] / 60
    job_statistics["Start Time"] = job_statistics["Start Time"] / 60
    job_statistics["End Time"] = job_statistics["End Time"] / 60
    job_statistics["Wait Time"] = job_statistics["Wait Time"] / 60
    job_statistics["Turnaround Time"] = job_statistics["Turnaround Time"] / 60
    job_statistics["Makespan"] = job_statistics["Makespan"] / 60
    fig = px.box(
        job_statistics.drop(
            columns=["Type", "Submit Time", "Start Time", "End Time", "Status"]
        ),
        labels={"value": "Time (m)", "variable": "Variable"},
    )
    if output:
        fig.write_image(output)
    return fig


def plot_jobs_violin(path, output=False):
    job_statistics = pd.read_csv(path, index_col="ID")
    job_statistics["Submit Time"] = job_statistics["Submit Time"] / 60
    job_statistics["Start Time"] = job_statistics["Start Time"] / 60
    job_statistics["End Time"] = job_statistics["End Time"] / 60
    job_statistics["Wait Time"] = job_statistics["Wait Time"] / 60
    job_statistics["Turnaround Time"] = job_statistics["Turnaround Time"] / 60
    job_statistics["Makespan"] = job_statistics["Makespan"] / 60
    fig = px.violin(
        job_statistics.drop(
            columns=["Type", "Submit Time", "Start Time", "End Time", "Status"]
        ),
        box=True,
        labels={"value": "Time (m)", "variable": "Variable"},
    )
    if output:
        fig.write_image(output)
    return fig


def plot_node_utilization(path, output=False):
    node_utilization = pd.read_csv(path, sep=",")
    node_utilization["Time"] = node_utilization["Time"] / 60
    groups = []
    for group in node_utilization.groupby("Node", sort=False):
        node_states = group[1].rename(columns={"Time": "Start Time"})
        node_states["End Time"] = node_states.shift(-1)["Start Time"]
        groups.append(node_states)
    node_utilization = pd.concat(groups)

    job_ids = node_utilization["Running jobs"].unique()
    fig = px.timeline(
        node_utilization,
        x_start="Start Time",
        x_end="End Time",
        y="Node",
        color="Running jobs",
        # use for SmallMotivationExample
        # color_discrete_map = {'0': '#636EFA', '1': '#FFA15A', '2': '#00CC96', '3': '#AB63FA'},
        color_discrete_sequence=px.colors.qualitative.Alphabet,
        opacity=0.8,
        labels={"Node": "Compute Node"},
        title="Node Utilization",
        text="Running jobs",
    )
    i = 0
    for job_id in job_ids:
        if job_id == "none":
            i += 1
            continue
        else:
            job_utilization_per_id = node_utilization[
                node_utilization["Running jobs"] == job_id
            ]
            fig.data[i].x = (
                job_utilization_per_id["End Time"]
                - job_utilization_per_id["Start Time"]
            )
            i += 1
    fig.update_xaxes(type="linear", title="Time (m)")
    fig.update_yaxes(autorange="reversed")
    fig.update_yaxes(categoryorder="category descending")
    fig.update_layout(bargap=0, showlegend=False)
    fig.update_traces(marker_line_width=0, textangle=0)
    fig.update_xaxes(tickvals=[0, 400, 800, 1200])

    # use for SmallMotivationExample
    # fig.update_layout(
    #     font=dict(
    #         size=28,
    #     ),
    #     xaxis_range=[0,1200]
    # )

    if output:
        fig.write_image(output)
    return fig
