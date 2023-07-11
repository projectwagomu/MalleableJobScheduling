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
import json
import os
import getopt
import sys
import jobGenerator


def write_to_file(path, file_name, json):
    path += "/" + file_name
    f = open(path, "w")
    f.write(json)
    f.close()


# generates application_model.json
def generate_application_model(scaling_formula):
    am = {
        "phases": [
            {
                "iterations": "divide",
                "tasks": [
                    {
                        "type": "cpu",
                        "name": "Compute",
                        "flops": "(flops/divide)/" + scaling_formula,
                        "computation_pattern": "UNIFORM",
                    }
                ],
            }
        ]
    }
    return json.dumps(am, indent=4)


# generates configuration.json
def generate_configuration(
    scheduling_interval=60,
    schedule_on_job_submit=True,
    schedule_on_job_finalize=True,
    schedule_on_scheduling_point=True,
    sensing=False,
    sensing_interval=60,
):
    conf = {
        "scheduling_interval": scheduling_interval,
        "min_scheduling_interval": 0,
        "schedule_on_job_submit": schedule_on_job_submit,
        "schedule_on_job_finalize": schedule_on_job_finalize,
        "schedule_on_scheduling_point": schedule_on_scheduling_point,
        "allow_oversubscription": False,
        "sensing": sensing,
        "sensing_interval": sensing_interval,
        "zmq_url": "ipc:///tmp/elastisim.ipc",
        "pfs_read_links": ["PFS_read"],
        "pfs_write_links": ["PFS_write"],
        "jobs_file": "data/input/jobs.json",
        "platform_file": "data/input/crossbar.xml",
        "job_statistics": "data/output/job_statistics.csv",
        "cpu_utilization": "data/output/cpu_utilization.csv",
        "node_utilization": "data/output/node_utilization.csv",
        "network_activity": "data/output/network_activity.csv",
        "pfs_utilization": "data/output/pfs_utilization.csv",
        "gpu_utilization": "data/output/gpu_utilization.csv",
    }
    return json.dumps(conf, indent=4)


# generates crossbar.xml
def generate_crossbar(cluster_dict):
    out = str = r"""<?xml version='1.0'?>
    <!DOCTYPE platform SYSTEM "https://simgrid.org/simgrid.dtd">
        <platform version="4.1">
            <zone id="MySyntheticCluster" routing="Full">
                <zone id="Batch-system_zone" routing="Full">
                    <host id="Batch_system" speed="0Gf">
                        <prop id="batch_system" value="true"/>
                    </host>
                </zone>
                <cluster id="Crossbar" prefix="Syn_" radical="{cluster_nodes_min}-{cluster_nodes_max}" suffix=""
                        speed="{flops_per_node}f" bw="100Gbps" lat="50us">
                    <prop id="node_local_bb" value="false"/>
                    <prop id="pfs_targets" value="PFS"/>
                </cluster>
                <zone id="PFS_zone" routing="Full">
                    <host id="PFS" speed="0Gf">
                        <prop id="pfs_host" value="true"/>
                    </host>
                </zone>
                <link id="PFS_read" bandwidth="300GBps" latency="500us"/>
                <link id="PFS_write" bandwidth="300GBps" latency="500us"/>
                <zoneRoute src="PFS_zone" dst="Crossbar" gw_src="PFS"
                        gw_dst="Syn_Crossbar_router" symmetrical="NO">
                    <link_ctn id="PFS_read"/>
                </zoneRoute>
                <zoneRoute src="Crossbar" dst="PFS_zone" gw_src="Syn_Crossbar_router"
                        gw_dst="PFS" symmetrical="NO">
                    <link_ctn id="PFS_write"/>
                </zoneRoute>
            </zone>
        </platform>"""
    return out.format(
        cluster_nodes_min=0,
        cluster_nodes_max=cluster_dict["num_cluster_nodes"] - 1,
        flops_per_node=cluster_dict["flops_per_cluster_node"],
    )


# generates jobs.json
def generate_job_json(total_time, cluster_dict, job_dict):
    jobs_to_generate = jobGenerator.generate_jobs(total_time, cluster_dict, job_dict)
    jobs_json = {
        "jobs_generated": len(jobs_to_generate),
        "total_time": total_time,
        "generation values": str(job_dict),
        "jobs": jobs_to_generate,
    }
    return json.dumps(jobs_json, indent=4)


def generate_json_files(path, total_time, cluster_dict, job_dict):
    input_config = locals().copy()

    jobs_json = generate_job_json(total_time, cluster_dict, job_dict)
    am_json = generate_application_model(job_dict["scaling_formula"])
    configuration_json = generate_configuration()
    crossbar_xml = generate_crossbar(cluster_dict)

    write_to_file(path, "jobs.json", jobs_json)
    write_to_file(path, "application_model.json", am_json)
    write_to_file(path, "configuration.json", configuration_json)
    write_to_file(path, "crossbar.xml", crossbar_xml)
    return f"Generating json file with {input_config}"


def get_arguments(argv, vals):
    arg_dict = dict()
    path = None
    quiet = False

    arg_names = ["quiet", "directory="] + [f"{v}=" for v in vals]
    opts, args = getopt.getopt(argv, "qd:", arg_names)
    for opt, arg in opts:
        if opt in ("-q", "--quiet"):
            quiet = True
        elif opt in ("-d", "--directory"):
            path = arg
            if not os.path.exists(path):
                os.makedirs(path)
        elif opt == "--seed":
            arg_dict["seed"] = arg
        elif opt == "--total_time":
            arg_dict["total_time"] = int(eval(arg))
        elif opt == "--type_probabilities":
            probs = [int(n) for n in arg.split(",")]
            assert len(probs) == 3
            arg_dict["type_probabilities"] = {
                "RIGID": probs[0],
                "MOLDABLE": probs[1],
                "MALLEABLE": probs[2],
            }
        elif opt == "--flops_range":
            flops = [int(eval(f)) for f in arg.split(",")]
            assert len(flops) == 2 and flops[0] <= flops[1]
            arg_dict["flops_range"] = range(flops[0], flops[1])
        elif opt == "--node_range":
            nodes = [int(n) for n in arg.split(",")]
            assert len(nodes) == 2 and nodes[0] <= nodes[1]
            arg_dict["node_range"] = range(nodes[0], nodes[1])
        elif opt == "--submit_range":
            arg_dict["submit_range"] = float(arg)
        elif opt == "--malleable_dividation_amount":
            arg_dict["malleable_dividation_amount"] = float(arg)
        elif opt == "--dividation_split_time":
            arg_dict["dividation_split_time"] = float(arg)
        elif opt == "--application_model":
            arg_dict["application_model"] = str(arg)
        elif opt == "--parallel_percentage":
            probs = [float(n) for n in arg.split(",")]
            arg_dict["parallel_percentage"] = probs
        elif opt == "--min_node_efficiency_threshold":
            arg_dict["min_node_efficiency_threshold"] = float(arg)
        elif opt == "--pref_node_efficiency_threshold":
            arg_dict["pref_node_efficiency_threshold"] = float(arg)
        elif opt == "--max_node_efficiency_threshold":
            arg_dict["max_node_efficiency_threshold"] = float(arg)
        elif opt == "--scaling_formula":
            arg_dict["scaling_formula"] = str(arg)
        elif opt == "--flops_per_cluster_node":
            arg_dict["flops_per_cluster_node"] = float(arg)
        elif opt == "--num_cluster_nodes":
            arg_dict["num_cluster_nodes"] = int(arg)
    return path, quiet, arg_dict


def get_arg(name, value, args, quiet=True):
    if name in args:
        return args[name]
    else:
        if not quiet:
            print(f"{name} not defined, falling back to default value: {value}")
        return value


def get_default_generation_values():
    total_time = 60 * 60 * 24 * 30
    job_dict = {
        "seed": "DefaultSeed",
        "type_probabilities": {"RIGID": 50, "MOLDABLE": 20, "MALLEABLE": 30},
        "flops_range": range(int(1e13), int(5e16)),
        "node_range": range(1, 16),
        "submit_range": 0.9,
        "malleable_dividation_amount": 1000,
        "dividation_split_time": 60,
        "application_model": "data/input/application_model.json",
        "parallel_percentage": tuple(float(i) / 1000.0 for i in range(950, 1000, 5)),
        "min_node_efficiency_threshold": 0.95,
        "pref_node_efficiency_threshold": 0.8,
        "max_node_efficiency_threshold": 0.5,
        "scaling_formula": "(1/((1-parallel_percentage) + parallel_percentage/num_nodes))",
    }
    cluster_dict = {"flops_per_cluster_node": 100e9, "num_cluster_nodes": 32}
    return total_time, job_dict, cluster_dict


# loads given parameters or default value if missing
def start_generation(sys_args):
    total_time, job_dict, cluster_dict = get_default_generation_values()
    arg_names = ["total_time"] + list(job_dict.keys()) + list(cluster_dict.keys())
    path, quiet, args = get_arguments(sys_args, arg_names)
    assert path != None

    total_time = get_arg("total_time", total_time, args)
    for k, v in cluster_dict.items():
        cluster_dict[k] = get_arg(k, v, args)
    for k, v in job_dict.items():
        job_dict[k] = get_arg(k, v, args)
    job_dict["submit_range"] = range(0, int(job_dict["submit_range"] * total_time))
    out = generate_json_files(path, total_time, cluster_dict, job_dict)
    if not quiet:
        print(out)


if __name__ == "__main__":
    start_generation(sys.argv[1:])
