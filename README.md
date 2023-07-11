# Malleable Job Scheduling Strategies

This repository proposes several malleable job scheduling strategies (see directory [scheduling_algorithms](scheduling_algorithms)) and evaluates them by running simulations via [ElastiSim](#acknowledgement).

## Dependencies:
- (U)Docker
- Following Python packages (not for simulation, but for analysis and plotting data)
```
pip install pandas
pip install plotly
pip install kaleido
pip install packaging
```

## Execution

### Single Simulation: `runElastisim.sh`

The script [runElastisim.sh](runElastisim.sh) starts *one* simulation for a given input. Requires Docker or Udocker.

#### Show help:
```
./runElastisim.sh -h
```

#### Small example:
```
./runElastisim.sh -e -s -f -i input_files/small -a scheduling_algorithms/rigid_easy_backfill.py
```
Attention: The first run builds the Docker image of ElastiSim, which takes some time.

After the simulation has been finished, the directory [output_files](output_files) contains a new directory `small(rigid_easy_backfill.py)` that contains statistics and figures.

---

### Multiple Simulations: `runSimulations.sh`

The script [runSimulations.sh](runSimulations.sh) starts *multiple* simulations. First, all inputs are generated using [jsonGenerator.py](scripts/input_generation/jsonGenerator.py). Then, the simulations are started using [runElastisim.sh](runElastisim.sh) . When executed locally, after all simulations have finished, statistics and figures are automatically generated using [plotEvaluation.py](scripts/output_evaluation/plotEvaluation.py).

#### Show help:
```
./runSimulations.sh -h
```

When calling [runSimulations.sh](runSimulations.sh) some parameters can be passed, but more parameters can be configured inside the file.

#### Small example:
The script [runSimulations.sh](runSimulations.sh) is configured as follows:
- Generate jobs for three days:
  ```
  days_to_simulate=3
  ```
- Generate the jobs three times using three seeds:
  ```
  seeds=("S1" "S2" "S3")
  ```
- Currently, we ignore moldable jobs:
  ```
  type_probabilities=("100,0,0" "80,0,20" "60,0,40" "40,0,60" "20,0,80" "0,0,100") #(Rigid, Moldable, Malleable)
  ```
- Each job has one of the following parallel_percentage for its application model:
  ```
  parallel_percentage="0.9999,0.999,0.995,0.99,0.985,0.98,0.975,0.97,0.965,0.96,0.955,0.95"
  ```
- Small cluster:
  ```
  num_cluster_nodes=32
  ```
- Our scheduling strategies:
  ```
  SCHEDULING_FILES=("average_agreement.py"
  "average_common_pool.py"
  "average_steal_agreement.py"
  "min_agreement.py"
  "min_common_pool.py"
  "min_steal_agreement.py"
  "pref_agreement.py"
  "pref_common_pool.py"
  "pref_steal_agreement.py"
  "rigid_easy_backfill.py")
  ```

This configuration leads to a total of *180* simulations (i.e., running docker containers): `#seeds` * `#type_probabilities` * `#SCHEDULING_FILES` = 3 * 6 * 10 = 180! Per default, half the number of available processor cores are used.

Just start it:

```
./runSimulations.sh
```

After all simulations have been finished, the directory [output_files](output_files) contains statistics and figures.

Be careful about defining configurations that are too large, start small!

#### Clusters with Slurm

Simulations can also be started using uDocker, which does not require root access (e.g. for clusters):

```
./runSimulations.sh -u
```

Installs uDocker as well as exports and imports the docker image of ElastiSim to be used with uDocker. Then it starts the simulations using uDocker. To start uDocker on, e.g., a cluster the ElastiSim image, `elastisim.tar`, must be copied to the cluster manually into the directory [data](data).

To start simulations using Slurm:
```
./runSimulations.sh -s public
```
This way, Slurm jobs using partition `public` are submitted using udocker. The script [runSlurmElastisim.sh](scripts/runSlurmElastisim.sh) must be individually adjusted for the used cluster.

After all jobs have been finished, the generation of statistics and figures must be started by hand:
```
python3 scripts/output_evaluation/evaluateOutput.py -s output_files/*
```
- `-f` generates figures for the distribution of jobs among the nodes (plot node utilization) and for the job processing (plot jobs gantt)
- `-s` generates statistics of the simulations.

## Acknowledgement

This repository heavily utilizes the software *Elastisim*, available at https://github.com/elastisim. We would like to express our sincere thanks to the developer Taylan Özden for his support.

## License

This software is released under the terms of the [Eclipse Public License v2.0](LICENSE.txt), though it also uses third-party packages with their own licensing terms.

## Publications

- Jonas Posner, Fabian Hupfeld, and Patrick Finnerty. *Enhancing Supercomputer Performance with Malleable Job Scheduling
  Strategies*. In Euro‐Par Parallel Processing, Workshop on Performance and Energy-efficiency in Concurrent and Distributed Systems (PECS), 2023. to appear.
  - Artefact: https://github.com/ProjectWagomu/ArtefactPECS23

## Contributors

In alphabetical order:

- Patrick Finnerty
- Fabian Hupfeld
- Jonas Posner
