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
import random
import math

def static_vars(**kwargs):
    def decorate(func):
        for k in kwargs:
            setattr(func, k, kwargs[k])
        return func
    return decorate


# clips values if they are above/below the given threshold
def clip_value(value, min_n=None, max_n=None, min_max=None):
    if min_max is not None:
        min_n = min(min_n, min_max.start) if min_n else min_max.start
        max_n = max(max_n, min_max.stop) if max_n else min_max.stop
    if max_n is not None:
        value = min(max_n, value)
    if min_n is not None:
        value = max(min_n, value)
    return value


# rounds the given value to the nearest power of 2.
def log_round_value(n, rounding=None, max_n=None, min_n=None, min_max=None):
    log_value = math.log2(n) if rounding is None else rounding(math.log2(n))
    return clip_value(2 ** int(log_value), min_n, max_n, min_max)


# Randomizer used to generate random job values
class Randomizer:
    def __init__(self, seed):
        self.randomizer = random.Random()
        self.randomizer.seed(seed)

    def get_random_value(self, values):
        if type(values) is range:
            return self.get_value_in_range(values)
        elif type(values) in (list, tuple, set):
            return self.get_value_in_iterable(values)
        else:
            raise TypeError("Unknown Random Type: " + str(type(values)))

    def get_random_value_in_weighted_dict(self, weighted_dict: dict):
        values = list(weighted_dict.keys())
        weights = list(weighted_dict.values())
        return self.randomizer.choices(values, weights, k=1)[0]

    def get_value_in_range(self, range: range):
        if range.start == range.stop:
            return range.start
        return self.randomizer.choice(range)

    def get_value_in_iterable(self, iterable):
        return self.randomizer.choice(iterable)

    def get_normal_distributed_value(self, mu, sigma, min_max=None):
        return clip_value(self.randomizer.gauss(mu, sigma), min_max=min_max)


@static_vars(memo_dict=dict())
def get_scaling_factor(formula, num_nodes, parallel_percentage):
    key = f"{num_nodes}|{parallel_percentage}"
    if key not in get_scaling_factor.memo_dict:
        value_map = {"parallel_percentage": parallel_percentage, "num_nodes": num_nodes}
        value = eval(formula, {}, value_map) / num_nodes
        get_scaling_factor.memo_dict[key] = value
    return get_scaling_factor.memo_dict[key]


@static_vars(memo_dict=dict())
def get_parallel_percentage(pref_nodes, job_dict):
    if pref_nodes not in get_parallel_percentage.memo_dict:
        pref_threshold = job_dict["pref_node_efficiency_threshold"]
        formula = job_dict["scaling_formula"]
        closest_pp = (None, 1)
        for parallel_percentage in job_dict["parallel_percentage"]:
            factor = get_scaling_factor(formula, pref_nodes, parallel_percentage)
            if factor > pref_threshold and factor - pref_threshold < closest_pp[1]:
                closest_pp = (parallel_percentage, factor - pref_threshold)
        pp = closest_pp[0] or max(job_dict["parallel_percentage"])
        get_parallel_percentage.memo_dict[pref_nodes] = pp
    return get_parallel_percentage.memo_dict[pref_nodes]


@static_vars(memo_dict=dict())
def get_min_max_nodes(pref_n, p_percentage, job_dict, min_scaling=0.2):
    key = f"{pref_n}|{p_percentage}"
    if key not in get_min_max_nodes.memo_dict:
        min_r, max_r = job_dict["node_range"].start, job_dict["node_range"].stop
        min_n = max_n = min_r
        for nodes in range(min_r, max_r + 1):
            efficiency = get_scaling_factor(
                job_dict["scaling_formula"], nodes, p_percentage
            )
            if efficiency > job_dict["min_node_efficiency_threshold"]:
                min_n = nodes
            if efficiency > job_dict["max_node_efficiency_threshold"]:
                max_n = nodes
            else:
                break

        min_n = log_round_value(min_n, rounding=math.floor, min_n=min_r, max_n=pref_n)
        max_n = log_round_value(max_n, rounding=math.floor, min_n=pref_n, max_n=max_r)
        get_min_max_nodes.memo_dict[key] = (min_n, max_n)
    return get_min_max_nodes.memo_dict[key]


def get_divide_amount(job_type, pref_nodes, flops_per_node, flops, jd):
    if job_type == "RIGID":
        return 1

    total_flops = pref_nodes * flops_per_node
    seconds = flops // total_flops
    calulated_div = max(1, seconds // jd["dividation_split_time"])
    return min(calulated_div, jd["malleable_dividation_amount"])


def calculate_flops(pref_nodes, node_range, flop_range, randomizer):
    def convert_range(from_value: int, from_range: range, to_range: range):
        min_from, max_from = (from_range.start, from_range.stop)
        min_to, max_to = (to_range.start, to_range.stop)
        from_size = max_from - min_from
        to_size = max_to - min_to
        to_value = (((from_value - min_from) * to_size) / from_size) + min_to
        return to_value

    # scale flops range to node range
    mu = convert_range(pref_nodes, node_range, flop_range)
    sigma = (flop_range.stop - flop_range.start) / 100
    return randomizer.get_normal_distributed_value(mu, sigma, min_max=flop_range)


# encodes job with given parameters
def encode_job(
    id, job_type, submit_time, node_range, p_percentage, flops, divide_amount, jd
):
    application_model = jd["application_model"]
    min_nodes, pref_nodes, max_nodes = node_range

    arguments = {
        "divide": divide_amount,
        "parallel_percentage": p_percentage,
        "flops": flops,
        "num_nodes_pref": pref_nodes,
        "id": id,
    }

    if job_type == "RIGID":
        return {
            "type": job_type,
            "submit_time": submit_time,
            "num_nodes": pref_nodes,
            "application_model": application_model,
            "arguments": arguments,
        }
    else:
        return {
            "type": job_type,
            "submit_time": submit_time,
            "num_nodes_min": min_nodes,
            "num_nodes_max": max_nodes,
            "application_model": application_model,
            "arguments": arguments,
        }


# generates one job with random values
def generate_job(id, jd, flops_per_node, given_flops=None):
    seed = jd["seed"]
    randomizer = Randomizer(seed + str(id))
    job_type = randomizer.get_random_value_in_weighted_dict(jd["type_probabilities"])
    submit_time = randomizer.get_random_value(jd["submit_range"])
    pref_nodes = log_round_value(randomizer.get_random_value(jd["node_range"]))

    p_percentage = get_parallel_percentage(pref_nodes, jd)
    min_nodes, max_nodes = get_min_max_nodes(pref_nodes, p_percentage, jd)
    node_range = (min_nodes, pref_nodes, max_nodes)

    flops = calculate_flops(pref_nodes, jd["node_range"], jd["flops_range"], randomizer)
    scaling_factor = get_scaling_factor(jd["scaling_formula"], pref_nodes, p_percentage)
    estimated_flops = flops / scaling_factor
    divide_amount = get_divide_amount(job_type, pref_nodes, flops_per_node, flops, jd)

    encoded = encode_job(
        id, job_type, submit_time, node_range, p_percentage, flops, divide_amount, jd
    )
    return encoded, estimated_flops


# generates as many jobs as the cluster can calculate in the given amount of time
def generate_jobs(total_time, cluster_dict, job_dict):
    flops_per_node = cluster_dict["flops_per_cluster_node"]
    cluster_flops = flops_per_node * cluster_dict["num_cluster_nodes"]
    total_flops = total_time * cluster_flops

    current_flops = 0
    jobs_to_generate = []
    max_job_flops = job_dict["flops_range"].stop
    while current_flops < total_flops - max_job_flops:
        job, pref_flops = generate_job(len(jobs_to_generate), job_dict, flops_per_node)
        current_flops += pref_flops
        jobs_to_generate.append(job)

    missing_flops = total_flops - current_flops
    job, _ = generate_job(len(jobs_to_generate), job_dict, missing_flops)
    jobs_to_generate.append(job)

    first_submit = min(job["submit_time"] for job in jobs_to_generate)
    for job in jobs_to_generate:
        job["submit_time"] = int(job["submit_time"] - first_submit)
    jobs_to_generate.sort(key=lambda j: j["submit_time"])
    for id, jd in enumerate(jobs_to_generate):
        jd["arguments"]["id"] = id

    if len(jobs_to_generate) not in range(100, 10000):
        print("Warning, %d jobs generated" % (len(jobs_to_generate)))

    return jobs_to_generate
