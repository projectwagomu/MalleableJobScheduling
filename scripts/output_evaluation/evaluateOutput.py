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
from generateStatistic import generate_statistics, generate_statistics_figures
from plotEvaluation import plot_evaluation
import sys
import os
import csv
import re
import multiprocessing as mp
import pathlib


regex_artifical = "^\d+D\[\d+\|\d+\|\d+\]#.*\{\d+,\d+\|.*,.*\}$"
basic_scheduler = "rigid_easy_backfill.py"


def f_float(n, lenght=4):
    return format(n, f".{lenght}f") if n is not None else ""


def f_int(n):
    return format(n, ".0f") if n is not None else ""


def f_tri_tuple(t, d=None, d1=0, d2=1, d3=1):
    if d != None:
        d1 = d2 = d3 = d
    return f"{f_float(t[0], d1)}, {f_float(t[1], d2)}, {f_float(t[2], d3)}"


def f_dict(d, default="EMPTY", f_value=f_float):
    if d is None:
        return default
    return ", ".join(f"{k}:{f_value(v)}" for k, v in d.items())


def avg(l, div=None, default=None, tuple=None):
    if div is None:
        div = len(l)
    if len(l) == 0 or div == 0:
        return default
    if tuple is not None:
        l = [i[tuple] for i in l]
    return float(sum(l)) / float(div)


def perc(base, other, default=None):
    if base is None or other is None or base == 0:
        return default
    return float(other) / float(base)


def get_malleable_amount_from_path(path):
    assert re.match(regex_artifical, path)
    return int(path[1 + path.rfind("|", 0, path.rfind("]")): path.rfind("]")])


def get_csv_dict(path: str):
    try:
        with open(path) as f:
            return [row for row in csv.DictReader(f)]
    except Exception as error:
        print("Unable to calculate metrics, file %s missing", path)
    return []


def get_metric_meta_overview(metric_data, basic_scheduler, is_event_metric=False):
        scheduler_dict = dict()
        for input_data, schedulers in metric_data.items():
            for scheduler, value in schedulers.items():
                scheduler_dict.setdefault(scheduler, []).append(value)

        out_dict = {}
        if is_event_metric:
            for scheduler, tri_values in scheduler_dict.items():
                tri_array = list()
                for i in (0, 1, 2):
                    values = [v[i] for v in tri_values if v[i] is not None]
                    min_value = min(values) if len(values) > 0 else 0
                    avg_value = avg(values, default=0)
                    max_value = max(values) if len(values) > 0 else 0
                    tri_array.append((min_value, avg_value, max_value))
                out_dict[scheduler] = tuple(tuple(v) for v in tri_array)

            sched_malleable_dict = dict()
            for input_data, schedulers in metric_data.items():
                malleable_amount = get_malleable_amount_from_path(input_data)
                for scheduler, value in schedulers.items():
                    d = sched_malleable_dict.setdefault(scheduler, dict())
                    d.setdefault(malleable_amount, []).append(value)
            for (scheduler, mall_amount_dict) in sched_malleable_dict.items():
                sorted_mal = sorted(list(mall_amount_dict.keys()))
                sched_tri = list()
                # convert dict[sched] = dict[percentage]([(v1,v2,v3), (v1,v2,v3)]) -> out_dict[sched] = (dict[percentage](avg(v1)), dict[percentage](avg(v2)), dict[percentage](avg(v3)))
                for i in (0, 1, 2):
                    sched_tri.append(
                        {m: avg(mall_amount_dict[m], tuple=i) for m in sorted_mal}
                    )
                out_dict[scheduler] = tuple(v for v in sched_tri)

        else:
            # Performance
            for scheduler, values in scheduler_dict.items():
                min_value = min(values) if len(values) > 0 else None
                avg_value = avg(values)
                max_value = max(values) if len(values) > 0 else None

                out_dict[scheduler] = (min_value, avg_value, max_value)

            # Rigid Change
            basic_values = (None, None, None)
            if basic_scheduler in out_dict:
                basic_values = out_dict[basic_scheduler]
            for scheduler, values in out_dict.items():
                perc_v = perc(basic_values[1], values[1])
                out_dict[scheduler] = (values, perc_v)

            sched_malleable_dict = dict()
            for input_data, schedulers in metric_data.items():
                malleable_amount = get_malleable_amount_from_path(input_data)
                for scheduler, value in schedulers.items():
                    d = sched_malleable_dict.setdefault(scheduler, dict())
                    d.setdefault(malleable_amount, []).append(value)
            for scheduler, malleable_amount_dict in sched_malleable_dict.items():
                sorted_mal = sorted(list(malleable_amount_dict.keys()))
                mal_change = {m: avg(malleable_amount_dict[m]) for m in sorted_mal}
                performance, rigid_change = out_dict[scheduler]
                out_dict[scheduler] = (performance, rigid_change, mal_change)
        return out_dict


def generate_meta_statistics_overview(data, metrics, out_path):
    with open(out_path, "w") as file:
        writer = csv.writer(file)
        header = ["scheduler"]
        scheduler_dict = dict()
        for metric, metric_data in data.items():
            is_event_metric = "event" in metric
            if is_event_metric:
                header.append(metric + "-Event-Amount")
                header.append(metric + "-Nodes-Per-Event")
                header.append(metric + "-Event-Per-Job")
                metric_meta = get_metric_meta_overview(metric_data, None, is_event_metric)
                for scheduler, v in metric_meta.items():
                    values = [f_dict(i) for i in v]
                    scheduler_dict.setdefault(scheduler, []).extend(values)
            else:
                header.append(metric + "-Performance")
                header.append(metric + "-Rigid-Change")
                header.append(metric + "-Malleable-Percent-Change")

                metric_meta = get_metric_meta_overview(metric_data, basic_scheduler)
                for scheduler, v in metric_meta.items():
                    values = [f_tri_tuple(v[0], d1=1)]
                    values.append(f_float(v[1]))
                    values.append(f_dict(v[2]))
                    scheduler_dict.setdefault(scheduler, []).extend(values)
        writer.writerow(header)
        for scheduler, values in scheduler_dict.items():
            writer.writerow([scheduler] + values)


def reorder_statistics_for_meta_evaluation(statistics):
    data = dict()
    all_schedulers = set()
    for k in statistics:
        scheduler = k[k.rfind("(") + 1: -1]
        input_data = k[: k.rfind("(")]

        # override rigid scheduler outputs with 100 percent rigid outputs
        if "rigid" in scheduler:
            rigid = k[: k.find("[") + 1] + "100|0|0" + k[k.find("]"):]
            statistics[k] = statistics[rigid].copy()

        all_schedulers.add(scheduler)
        for metric, value in statistics[k].items():
            input_dict = data.setdefault(metric, dict())
            scheduler_dict = input_dict.setdefault(input_data, dict())
            scheduler_dict[scheduler] = value  # metric,input,scheduler = v

    def rigid_amount(d): return int(d[0][d[0].find("[") + 1: d[0].find("|")])
    def sort_by_rigid_amount(i_d): return dict(sorted(i_d.items(), key=rigid_amount, reverse=True))
    d = {m: sort_by_rigid_amount(i_d) for m, i_d in data.items()}
    return d, len(all_schedulers)


def generate_meta_statistic_for_metric(data, formatting, scheduler_size, out_path):
        order, f = formatting
        with open(out_path, "w") as file:
            writer = csv.writer(file)
            unique_schedulers = {s for i, schedulers in data.items() for s in schedulers}
            sorted_schedulers = sorted(list(unique_schedulers))

            header = ["input_data"] + sorted_schedulers
            if order is not None: #show best scheduler for metric
                header.append("best_scheduler")
            writer.writerow(header)

            for input_data, schedulers in data.items():
                values = [f(v) for v in schedulers.values()]
                row = [input_data] + values
                row += ["" for _ in range(scheduler_size - len(values))]
                if order is not None:
                    best_contendors = [v for v in schedulers.values() if v is not None]
                    best = order(best_contendors) if len(best_contendors) > 0 else None
                    row.append(", ".join(s for s, v in schedulers.items() if v == best))
                writer.writerow(row)


def generate_meta_statistics(statistics, out_path, plot=False):
    # metrics with the value order, max = larger value is better etc
    metrics = {
        "total_runtime": (min, f_int),
        "average_wait_time": (min, f_float),
        "average_turnaround_time": (min, f_float),
        "average_makespan": (min, f_float),
        "average_node_utilization": (max, f_float),
        "end_of_max": (min, f_int),
        "expand_event": (None, f_tri_tuple),
        "shrink_event": (None, f_tri_tuple),
    }

    data, scheduler_size = reorder_statistics_for_meta_evaluation(statistics)
    for metric, formatting in metrics.items():
        print(f"Generating Metric Data for {metric} metric")
        out_file = out_path + metric + ".csv"
        generate_meta_statistic_for_metric(data[metric], formatting, scheduler_size, out_file)

    print(f"Generating Meta Statistic")
    generate_meta_statistics_overview(data, metrics, out_path + "statistics.csv")

    if plot:
        plot_evaluation(out_path, list(metrics.keys()))


def get_args():
    statistics_flag = figure_flag = False
    paths = []
    for arg in sys.argv[1:]:
        if arg in ("-f", "-fs", "-sf"):
            figure_flag = True
        elif arg in ("-s", "-fs", "-sf"):
            statistics_flag = True
        else:
            if not arg.endswith("/"):
                arg += "/"
            if os.path.isdir(arg):
                paths.append(arg)
            else:
                print("Not a path", arg)
    return figure_flag, statistics_flag, paths


def generate_statistic(path):
#    name = path.removesuffix("/") #could be used for python >=3.9
    p_file = pathlib.Path(path)
    name = str(p_file)
    name = name[name.rfind("/") + 1:]
    statistic = generate_statistics(path)
    return {name: statistic}


def generate_statistics_for_each_output(paths, cpu_usage=0.75):
    print("Generate", len(paths), "Statistics")
    pool = mp.Pool(int(mp.cpu_count() * cpu_usage))
    results = pool.map(generate_statistic, paths)
    statistics = dict()
    for r in results:
        statistics.update(r)
    return statistics


def generate_figures_for_each_output(statistics, paths, img_scaling_factor=float(1_000_000)):
    print("Generate", len(paths), "Figures")
    for statistic, path in zip(statistics.values(), paths):
        scale = (
            statistic["total_runtime"] / img_scaling_factor
            if "total_runtime" in statistic
            else 1
        )
        print("Generate", path, "Figures")
        generate_statistics_figures(path, scaling_factor=max(1, min(scale, 20)))


def evaluate_output():
    figure_flag, statistics_flag, paths = get_args()
    statistics = generate_statistics_for_each_output(paths)
    if figure_flag:
        generate_figures_for_each_output(statistics, paths)

    if statistics_flag:
        if len(statistics) <= 1:
            print(statistics)
        else:
            meta_output = os.path.commonpath(paths)
            if not meta_output.endswith("/"):
                meta_output += "/"
            if not meta_output.endswith("/output_files/"):
                meta_output = os.getcwd() + "/output_files/"
            generate_meta_statistics(statistics, meta_output, True)


if __name__ == "__main__":
    evaluate_output()
