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
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
import os, sys

pio.kaleido.scope.mathjax = None
SINGLE_PLOT_DIMENSIONS = (400, 500)
OUTPUT_FILE_TYPE = "pdf"

COLOR_PALETTE = px.colors.qualitative.Bold + px.colors.qualitative.Plotly
HIGHLIGHT_COLOR = "crimson"
SFIG_NO_LEGEND = False
FONT_SIZE = 20

BASELINE_SCHEDULER="rigid_easy"


def adjust_scheduler_names(schedulers):
    def adjust_scheduler_name(s: str):
        replace = [
            ("rigid_easy_backfill", "backfill"),
            ("average", "avg"),
            ("common_pool", "pool"),
            ("steal_agreement", "flex"),
            ("agreement", "agree"),
        ]
        for t in replace:
            s = s.replace(t[0], t[1])
        return s.removesuffix(".py")

    if type(schedulers) == list:
        return [adjust_scheduler_name(s) for s in schedulers]
    return adjust_scheduler_name(schedulers)


def make_dir_if_missing(dir_path):
    if dir_path and type(dir_path) == str and not os.path.exists(dir_path):
        os.makedirs(dir_path)


def darken_color(color, percent=0.75):
    rgb_value = color[4:-1].split(", ")
    r, g, b = [int(float(v) * percent) for v in rgb_value]
    return f"rgb({r}, {g}, {b})"


def plot_subplot(path, file_name, fig, dim=(500, 1300)):
    if not path:
        return
    height, width = dim
    output = f"{path}/{file_name}.{OUTPUT_FILE_TYPE}"
    fig.update_layout(
        title_text="",
        margin={"l": 0, "r": 0, "t": 0, "b": 0},
        font=dict(family="Courier New, monospace", size=FONT_SIZE),
    )
    fig.write_image(output, format=OUTPUT_FILE_TYPE, height=height, width=width)



def plot_min_max_avg_graph(fig, pos, data, schedulers, other, colors, path=None):
    row, column = pos[0] + 1, pos[1] + 1
    s_fig = make_subplots(
        rows=1,
        cols=1,
        subplot_titles=[] if SFIG_NO_LEGEND else adjust_scheduler_names(other),
    )
    for scheduler in schedulers:
        marker_color = darken_color(colors[scheduler])
        min_v, avg_v, max_v = data.loc[scheduler, other].split(", ")

        bar = go.Bar(
            x=adjust_scheduler_names([scheduler]),
            y=[float(avg_v)],
            marker_color=colors[scheduler],
            legendgroup="groupFalse",
            showlegend=False,
        )
        min_line = go.Scatter(
            x=adjust_scheduler_names([scheduler]),
            y=[float(min_v)],
            mode="markers",
            marker_symbol=41,
            marker_line_width=3,
            marker_size=30,
            marker_line_color=marker_color,
            legendgroup="groupFalse",
            showlegend=False,
        )
        max_line = go.Scatter(
            x=adjust_scheduler_names([scheduler]),
            y=[float(max_v)],
            mode="markers",
            marker_symbol=41,
            marker_line_width=3,
            marker_size=30,
            marker_line_color=marker_color,
            legendgroup="groupFalse",
            showlegend=False,
        )

        fig.add_trace(bar, row=row, col=column)
        fig.add_trace(min_line, row=row, col=column)
        fig.add_trace(max_line, row=row, col=column)

        if path: #plot_single_subplot
            s_fig.add_trace(bar, row=1, col=1)
            s_fig.add_trace(min_line, row=1, col=1)
            s_fig.add_trace(max_line, row=1, col=1)
    plot_subplot(path, other, s_fig)


def plot_dict_graph_lines(fig, pos, data, schedulers, other, colors, path=None):
    row, column = pos[0] + 1, pos[1] + 1
    s_fig = make_subplots(
        rows=1,
        cols=1,
        subplot_titles=[] if SFIG_NO_LEGEND else adjust_scheduler_names(other),
    )
    for scheduler in schedulers:
        values = data.loc[scheduler, other].split(", ")
        if "EMPTY" in values:
            continue
        items = [i.split(":") for i in values]
        items = [i for i in items if i[0] != "None" and i[1] != "None"]
        x_values = [int(i[0]) for i in items]
        y_values = [float(i[1]) for i in items]

        if row == 1:
            line = go.Scatter(
                x=x_values,
                y=y_values,
                mode="lines+markers",
                marker_color=colors[scheduler],
                name=adjust_scheduler_names(scheduler),
                legendgroup="groupTrue",
                showlegend=True,
            )
        else:
            line = go.Scatter(
                x=x_values,
                y=y_values,
                mode="lines+markers",
                marker_color=colors[scheduler],
                name=adjust_scheduler_names(scheduler),
                legendgroup="groupFalse",
                showlegend=False,
            )
        fig.add_trace(line, row=row, col=column)
        if path: #plot_single_subplot
            line = go.Scatter(
                x=x_values,
                y=y_values,
                mode="lines+markers",
                marker_color=colors[scheduler],
                name=adjust_scheduler_names(scheduler),
                legendgroup="groupTrue",
                showlegend=True,
            )
            s_fig.update_xaxes(dtick=10)
            s_fig.add_trace(line, row=1, col=1)
    plot_subplot(path, other, s_fig)


def plot_dict_graph_bars(fig, pos, data, schedulers, other, colors, path=None):
    row, column = pos[0] + 1, pos[1] + 1
    s_fig = make_subplots(
        rows=1,
        cols=1,
        subplot_titles=[] if SFIG_NO_LEGEND else adjust_scheduler_names(other),
    )
    s_fig.update_layout(legend_x=1)
    s_fig.update_xaxes(title_text="Percentage of Malleable Jobs")
    y_title = (
        "Average Node Utilization in Percent"
        if "node" in other
        else ("Event Count" if "event" in other else "Time in Hours")
    )
    s_fig.update_yaxes(title_text=y_title)

    for scheduler in schedulers:
        values = data.loc[scheduler, other].split(", ")
        if "EMPTY" in values:
            continue
        items = [i.split(":") for i in values]
        items = [i for i in items if i[0] != "None" and i[1] != "None"]
        x_values = [int(i[0]) for i in items]
        y_values = [float(i[1]) for i in items]
        if "node" not in other and "event" not in other:
            y_values = [i / 3600 for i in y_values]
        elif "node" in other:
            y_values = [i * 100 for i in y_values]

        if row == 1:
            line = go.Bar(
                x=x_values,
                y=y_values,
                marker_color=colors[scheduler],
                name=adjust_scheduler_names(scheduler),
                legendgroup="groupTrue",
                showlegend=True,
            )
        else:
            line = go.Bar(
                x=x_values,
                y=y_values,
                marker_color=colors[scheduler],
                name=adjust_scheduler_names(scheduler),
                legendgroup="groupFalse",
                showlegend=False,
            )
        fig.add_trace(line, row=row, col=column)
        if path:
            if BASELINE_SCHEDULER in scheduler:
                s_fig.add_hline(
                    y=y_values[0],
                    line_color=colors[scheduler],
                    annotation_text=adjust_scheduler_names(scheduler),
                    annotation_position="bottom right",
                )
                line = go.Bar(
                    x=[x_values[0]],
                    y=[y_values[0]],
                    marker_color=colors[scheduler],
                    name=adjust_scheduler_names(scheduler),
                    legendgroup="groupFalse",
                    showlegend=not SFIG_NO_LEGEND or "total_runtime" in other,
                )
                s_fig.add_trace(line, row=1, col=1)
                continue
            if "end_of_max" in other or "total_runtime" in other:
                s_fig.update_yaxes(range=[720, 850])
            if "node" in other:
                s_fig.update_yaxes(range=[75, 100])

            line = go.Bar(
                x=x_values,
                y=y_values,
                marker_color=colors[scheduler],
                name=adjust_scheduler_names(scheduler),
                legendgroup="groupFalse",
                showlegend=not SFIG_NO_LEGEND or "total_runtime" in other,
            )
            s_fig.update_xaxes(dtick=10)
            s_fig.add_trace(line, row=1, col=1)
            s_fig.update_annotations(font_size=16)
    plot_subplot(path, other, s_fig)


def plot_simple_graph(fig, pos, data, schedulers, other, colors, highlight=None, rev=False, path=None):
    row, column = pos[0] + 1, pos[1] + 1
    s_fig = make_subplots(
        rows=1,
        cols=1,
        subplot_titles=[] if SFIG_NO_LEGEND else adjust_scheduler_names(other),
    )
    for scheduler in schedulers:
        value = data.loc[scheduler, other] if rev else data.loc[other, scheduler]

        bar = go.Bar(
            x=[adjust_scheduler_names(scheduler)],
            y=[value],
            marker_color=colors[scheduler],
            legendgroup="groupFalse",
            showlegend=False,
        )
        if (
            other in data.index
            and "best_scheduler" in data.columns
            and highlight is not None
        ):
            best_schedulers = data.loc[other, "best_scheduler"]
            if type(best_schedulers) is str and scheduler in best_schedulers:
                bar.marker.line.color = highlight
                bar.marker.line.width = 2
        fig.add_trace(bar, row=row, col=column)
        if path:
            s_fig.add_trace(bar, row=1, col=1)
    plot_subplot(path, other, s_fig)


def plot_event_graph(fig, pos, data, schedulers, other, colors, highlight, path=False):
    row, column = pos[0] + 1, pos[1] + 1
    s_fig = make_subplots(
        rows=1,
        cols=1,
        subplot_titles=[] if SFIG_NO_LEGEND else adjust_scheduler_names(other),
    )
    for scheduler in schedulers:
        value = 0
        events = 0
        if other in data.index and scheduler in data.columns:
            if str(data.loc[other, scheduler]).count(",") > 1:
                value_tuple = data.loc[other, scheduler].split(", ")
                events = float(value_tuple[0])
                value = events * float(value_tuple[1])
        bar = go.Bar(x=[scheduler], y=[value], marker_color=colors[scheduler])
        marker_color = darken_color(colors[scheduler])
        line = go.Scatter(
            x=[scheduler],
            y=[events],
            mode="markers",
            marker_symbol=41,
            marker_line_width=3,
            marker_size=35,
            marker_line_color=marker_color,
        )

        if other in data.index and "best_scheduler" in data.columns:
            best_schedulers = data.loc[other, "best_scheduler"]
            if type(best_schedulers) is str and scheduler in best_schedulers:
                bar.marker.line.color = highlight
                bar.marker.line.width = 2
        fig.add_trace(bar, row=row, col=column)
        fig.add_trace(line, row=row, col=column)
        if path:
            s_fig.add_trace(bar, row=1, col=1)
            s_fig.add_trace(line, row=1, col=1)
    plot_subplot(path, other, s_fig)


def plot_meta_statistic(path, metrics, scheduler_blacklist=[], output=False, plot_single=False):
    if plot_single == True:
        plot_single = path[: path.rfind("/")]
    make_dir_if_missing(plot_single)
    data = pd.read_csv(path, index_col="scheduler")
    headers = data.columns.to_list()
    schedulers = [s for s in data.index.to_list() if s not in scheduler_blacklist]
    used_metrics = {m for h in headers for m in metrics if m in h}
    used_metrics = [m for m in metrics if m in used_metrics]

    row_amount = len(used_metrics)
    column_amount = max(len([h for h in headers if m in h]) for m in metrics)
    subplot_titles = []
    for metric in used_metrics:
        sub_metrics = [col for col in headers if metric in col]
        subplot_titles.extend(sub_metrics)
        subplot_titles.extend([" " for _ in range(column_amount - len(sub_metrics))])

    fig = make_subplots(
        rows=row_amount,
        cols=column_amount,
        subplot_titles=subplot_titles,
        row_titles=used_metrics,
        shared_xaxes=False,
    )

    sched_colors = {
        s: COLOR_PALETTE[i % len(COLOR_PALETTE)] for i, s in enumerate(schedulers)
    }
    for row, metric in enumerate(metrics):
        sub_metrics = [col for col in headers if metric in col]
        for column, header in enumerate(sub_metrics):
            pos = (row, column)
            if "Malleable-Percent-Change" in header or "Event" in header:
                plot_dict_graph_bars(
                    fig, pos, data, schedulers, header, sched_colors, path=plot_single
                )
            elif "Performance" in header:
                plot_min_max_avg_graph(
                    fig, pos, data, schedulers, header, sched_colors, path=plot_single
                )
            elif "Rigid-Change" in header:
                plot_simple_graph(
                    fig,
                    pos,
                    data,
                    schedulers,
                    header,
                    sched_colors,
                    rev=True,
                    path=plot_single,
                )

    fig.update_layout(title_text="Meta-Statistics", showlegend=True)
    if output:
        fig_height, fig_width = SINGLE_PLOT_DIMENSIONS
        height = (1 + row_amount) * fig_height + 100
        width = (1 + column_amount) * fig_width + 200
        fig.write_image(output, format=OUTPUT_FILE_TYPE, height=height, width=width)


def plot_multiple_graphs(path, metric_settings, output=False, plot_single=False):
    def sort_data_by_seed(data):
        scheds = [s for s in data.columns if s.endswith(".py")]
        colors = {
            scheds[i]: COLOR_PALETTE[i % len(COLOR_PALETTE)] for i in range(len(scheds))
        }

        index_list = data.index.to_list()
        i_seed_dict = {s: s[s.find("#") : s.find("{", s.find("#"))] for s in index_list}
        seeds = sorted(list(set(i_seed_dict.values())))
        seed_dict = {s: [] for s in seeds}
        for i, s in i_seed_dict.items():
            seed_dict[s].append(i)
        return seed_dict, seeds, scheds, colors

    graph_plotter, title, unit = metric_settings

    data = pd.read_csv(path, index_col="input_data")
    seed_dict, seeds, schedulers, colors = sort_data_by_seed(data)

    rows, columns = len(seeds), max(len(s) for s in seed_dict.values())
    sub_titles = [f"Input-Data: {i}" for i_d in seed_dict.items() for i in i_d[1]]

    fig = make_subplots(
        rows=rows,
        cols=columns,
        subplot_titles=sub_titles,
        row_titles=seeds,
        x_title="scheduler",
        y_title=unit,
    )
    for row, seed in enumerate(seeds):
        input_datas = seed_dict[seed]
        for col, input_data in enumerate(input_datas):
            pos = (row, col)
            graph_plotter(
                fig,
                pos,
                data,
                schedulers,
                input_data,
                colors,
                HIGHLIGHT_COLOR,
                plot_single,
            )

    fig.update_layout(title_text=title, showlegend=False)
    if output:
        fig_height, fig_width = SINGLE_PLOT_DIMENSIONS
        height = (1 + rows) * fig_height + 100
        width = (1 + columns) * fig_width + 200
        fig.write_image(output, format=OUTPUT_FILE_TYPE, height=height, width=width)


def plot_evaluation(path, metrics, only_plot_meta=False, meta_scheduler_blacklist=["rigid_backfill.py"]):
    metric_settings = {
        "total_runtime": (plot_simple_graph, "Total Runtime", "time (s)"),
        "average_wait_time": (plot_simple_graph, "Average Wait-Time", "time (s)"),
        "average_turnaround_time": (
            plot_simple_graph,
            "Average Turnaround-Time",
            "time (s)",
        ),
        "average_makespan": (plot_simple_graph, "Average Makespan", "time (s)"),
        "average_node_utilization": (
            plot_simple_graph,
            "Average Node Utilization",
            "utilization (%)",
        ),
        "end_of_max": (
            plot_simple_graph,
            "End of full cluster utilization",
            "time (s)",
        ),
        "expand_event": (plot_event_graph, "Expand Events", "Amount"),
        "shrink_event": (plot_event_graph, "Shrink Events", "Amount"),
    }

    if not only_plot_meta:
        for metric in metrics:
            if metric in metric_settings:
                csv = path + metric + ".csv"
                out = path + metric + "." + OUTPUT_FILE_TYPE
                print(f"Generating Plots for {metric} metric")
                plot_multiple_graphs(csv, metric_settings[metric], out)

    csv = path + "statistics.csv"
    out = path + "statistics." + OUTPUT_FILE_TYPE
    single_out = path + "single_plots"
    print(f"Generating Plot for meta-statistic")
    # plot_single: False=no-single-plots, True=single-plot-into-output_path, "dir"=single-plots-into-dir-folder
    plot_meta_statistic(
        csv,
        metric_settings.keys(),
        scheduler_blacklist=meta_scheduler_blacklist,
        output=out,
        plot_single=single_out,
    )


if __name__ == "__main__":
    assert len(sys.argv) > 1
    assert os.path.isdir(sys.argv[1])
    path = str(sys.argv[1]).removesuffix("/") + "/"
    metrics = [
        "total_runtime",
        "average_wait_time",
        "average_turnaround_time",
        "average_makespan",
        "average_node_utilization",
        "end_of_max",
        "expand_event",
        "shrink_event",
    ]
    plot_evaluation(path, metrics)
