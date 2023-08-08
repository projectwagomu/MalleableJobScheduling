#!/bin/bash
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
CWD="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
INPUT_FOLDER="$CWD/input_files/"
SCHEDULING_FOLDER="$CWD/scheduling_algorithms/"
OUTPUT_FOLDER="$CWD/output_files/"
TEMP_FOLDER="$CWD/temp"
DATA_FOLDER="$CWD/data"
SCRIPT_FOLDER="$CWD/scripts"

ELASTISIM="$CWD/runElastisim.sh"
ELASTISIM_FLAGS="-esqy"
EVALUATION_FLAGS="-s"	
JSON_GENERATOR="$SCRIPT_FOLDER/input_generation/jsonGenerator.py"
EVALUATE_OUTPUT="$SCRIPT_FOLDER/output_evaluation/evaluateOutput.py"
RUN_SLURM_ELASTISIM="$SCRIPT_FOLDER/runSlurmElastisim.sh"
ELASTISIM_TAR_CONTAINER="${DATA_FOLDER}/elastisim.tar"

#generate input file both args
days_to_simulate=3
seeds=("S1" "S2" "S3")
type_probabilities=("100,0,0" "80,0,20" "60,0,40" "40,0,60" "20,0,80" "0,0,100") #(Rigid, Moldable, Malleable)
malleable_dividation_amount=1000
dividation_split_time=60 #seconds
application_model="data/input/application_model.json"
parallel_percentage="0.999,0.995,0.99,0.985,0.98,0.975,0.97,0.965,0.96,0.955,0.95"
min_node_efficiency_threshold=0.95
pref_node_efficiency_threshold=0.8
max_node_efficiency_threshold=0.5
scaling_formula="(1/((1-parallel_percentage)+parallel_percentage/num_nodes))"

#generate input files artificial args
total_time="60*60*24*$days_to_simulate"
flop_ranges="5E12,4E17" # ("min_flops,max_flops")
node_ranges="1,16" # ("min_nodes,max_nodes")
submit_range=1
num_cluster_nodes=32 #artificial only
flops_per_cluster_node=1E12


#define scheduler files (Comment out definition to use all scheduling files in simulate/scheduling_algorithms/ that do not start with _)
SCHEDULING_FILES=("average_agreement.py" "average_common_pool.py" "average_steal_agreement.py" "min_agreement.py"	"min_common_pool.py" "min_steal_agreement.py" "pref_agreement.py" "pref_common_pool.py" "pref_steal_agreement.py" "rigid_easy_backfill.py")
#INPUT_DATA=("${INPUT_FOLDER}example_project")

#local processor amount used
MAX_PROCESSES=$(($(grep -c ^processor /proc/cpuinfo)/2))

while getopts "hus:p:" OPTION; do
	case "${OPTION}" in
	h) echo "./runSimulations.sh -u (Run Simulation using UDocker) -s (Partition Name) Start Simulations using slurm -p (Max Processes)" ; exit;;
	u) udocker=true;;
	s) slurm=${OPTARG};;
	p) MAX_PROCESSES=$((${OPTARG}));;
	*) echo "Unknown Argument" ; exit 1;;
	esac
done

#set number of processor per node to calculate node amount
function set_partition_nodes() {
	[[ $slurm == "public" ]] && SLURM_SIMULATIONS_PER_NODE=16
	[[ -z $slurm ]] && echo "Unknown partition name $slurm" && exit 1
}

if [ ! -z $udocker ] || [ ! -z $slurm ]; then
	ELASTISIM_FLAGS=${ELASTISIM_FLAGS}u
fi

function generateInputData() {
	if [ -z $INPUT_DATA ]; then
		echo "Generating input data"
		INPUT_DATA=()
    #artificial job generation
    for seed in "${seeds[@]}"; do
      for tp in "${type_probabilities[@]}"; do
        input="$INPUT_FOLDER${days_to_simulate}D[${tp//[,]/|}]#${seed}{${node_ranges}|${flop_ranges}}"
        INPUT_DATA+=($input)
        [[ -d "$input" ]] && { echo "Skipping $input, already existing" ; continue; }
        general_parameters="-q -d $input --seed $seed --total_time $total_time"
        job_parameters="--type_probabilities $tp --flops_range $flop_ranges --node_range $node_ranges --submit_range $submit_range --malleable_dividation_amount $malleable_dividation_amount --dividation_split_time $dividation_split_time --application_model $application_model --parallel_percentage $parallel_percentage --min_node_efficiency_threshold $min_node_efficiency_threshold --pref_node_efficiency_threshold $pref_node_efficiency_threshold --max_node_efficiency_threshold $max_node_efficiency_threshold --scaling_formula $scaling_formula"
        cluster_parameters="--flops_per_cluster_node $flops_per_cluster_node --num_cluster_nodes $num_cluster_nodes"
        python3 $JSON_GENERATOR $general_parameters $job_parameters $cluster_parameters
      done
    done
	fi
	#if no scheduling_files are provided, use all scheduling-algorithmus in $SCHEDULING_FOLDER
	if [ -z $SCHEDULING_FILES ]; then
		SCHEDULING_FILES=($(cd $SCHEDULING_FOLDER && ls [!-]*.py | cut -f1))
	else
		for i in "${!SCHEDULING_FILES[@]}"; do
			SCHEDULING_FILES[$i]="$SCHEDULING_FOLDER${SCHEDULING_FILES[$i]}"
		done
	fi
	echo "${#INPUT_DATA[@]} Input configurations generated for ${#SCHEDULING_FILES[@]} schedulers"
}

function secs_to_human() {
    echo "$(( ${1} / 3600 ))h $(( (${1} / 60) % 60 ))m $(( ${1} % 60 ))s"
}

function runSimulations() {
	#check required installations
	OUTPUT_DATA=()
	ARGS=()
	process_count=0
	process_len=$((${#SCHEDULING_FILES[@]}*${#INPUT_DATA[@]}))
	#generate runElastiSim arguments
	for scheduler in "${SCHEDULING_FILES[@]}"; do
		[[ -f "$scheduler" ]] || { echo "ERROR: scheduler-file $scheduler not found" ; exit 1; }
		for input in "${INPUT_DATA[@]}"; do
			[[ -d "$input" ]] || { echo "ERROR: input-file $input not found" ; exit 1; }
			folder="$(basename $input)($(basename $scheduler))"
			OUTPUT_DATA+=("${OUTPUT_FOLDER}/${folder}")
			process_count=$(($process_count+1))
			arg="$ELASTISIM_FLAGS -i $input -a $scheduler"
			if [ -z $slurm ]; then
				arg+=" $(echo $process_count/$process_len)"
			fi
			ARGS+=("$arg")
		done
	done
	if [ -z $slurm ]; then # local
		source $ELASTISIM $([[ ! -z $udocker ]] && echo "-u" || echo "") || exit 1
		STARTTIME=$(date +%s)
		echo "Simulations started with $MAX_PROCESSES parallel processes"
		echo "${ARGS[@]}" | xargs -n 6 -P $MAX_PROCESSES bash -c 'echo -en "\rSimulation $5 started" ; ./runElastisim.sh $0 $1 $2 $3 $4 || exit 255; '
		ENDTIME=$(date +%s)
		echo "" ; echo "Simulations completed in $(secs_to_human "$(($ENDTIME - $STARTTIME))") seconds"
	else #using slurm
		[[ ! -d "$CWD/out" ]] && { mkdir $CWD/out; } #create output-directory
		[ ! -d "$TEMP_FOLDER" ] && mkdir $TEMP_FOLDER
		cd $DATA_FOLDER
		[[ -f "udocker-1.3.7.tar.gz" ]] || wget https://github.com/indigo-dc/udocker/releases/download/1.3.7/udocker-1.3.7.tar.gz
		[[ -f "udocker-englib-1.2.8.tar.gz" ]] || curl -L https://github.com/jorge-lip/udocker-builds/raw/master/tarballs/udocker-englib-1.2.8.tar.gz > udocker-englib-1.2.8.tar.gz
		[[ -f "elastisim.tar" ]] || { echo "elastisim.tar NOT found in $DATA_FOLDER, abort!"; exit; }
		
		cd $CWD
		set_partition_nodes
		sims_for_node=()
		rest=$( (( ${#ARGS[@]} % $SLURM_SIMULATIONS_PER_NODE == "0" )) && echo "0" || echo "1" )
		parts=$((${#ARGS[@]}/$SLURM_SIMULATIONS_PER_NODE + $rest))
		counter=1
		for arg in "${ARGS[@]}"; do
			sims_for_node+=("$arg")
			if [[ "${#sims_for_node[@]}" == "$SLURM_SIMULATIONS_PER_NODE" ]]; then  #start sims to fill one node
			  jobName="${days_to_simulate}D-ElastiSim-UDocker-${counter}of${parts}"
				slurmFlags="-J ${jobName} -N 1 --exclusive -p $slurm"
				echo "SLURM-Script ${slurmFlags} Submitted"
				sbatch $slurmFlags $RUN_SLURM_ELASTISIM $CWD ${sims_for_node[@]}
				sims_for_node=()
				counter=$(($counter+1))
			fi
		done
		if [ ! ${#sims_for_node[@]} -eq 0 ]; then #start missing sims
			jobName="${days_to_simulate}D-ElastiSim-UDocker-${counter}of${parts}"
			slurmFlags="-J ${jobName} -N 1 --exclusive -p $slurm"
			echo "SLURM-Script ${slurmFlags} Submitted"
			sbatch $slurmFlags $RUN_SLURM_ELASTISIM $CWD ${sims_for_node[@]}
		fi
		exit
	fi
}

function evaluateOutput() {
	if [[ ! -z $OUTPUT_DATA ]]; then
		echo "Evaluating Output Data"
		python3 -W ignore $EVALUATE_OUTPUT $EVALUATION_FLAGS "${OUTPUT_DATA[@]}"
	fi
}

function finish() {
	#currently not used, use "pkill runElastisim.sh instead
	#kill $xargs_pid &> /dev/null
	#pkill runElastiSim.sh
	exit
}

trap finish EXIT

generateInputData
runSimulations
evaluateOutput