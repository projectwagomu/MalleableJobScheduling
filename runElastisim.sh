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
longuuid=$(cat /proc/sys/kernel/random/uuid)
uuid=${longuuid:0:8}

DATA_FOLDER="$CWD/data"
TEMP_FOLDER="$CWD/temp"
TEMP_FOLDER_UUID=$TEMP_FOLDER/$uuid
TEMP_FOLDER_DATA="$TEMP_FOLDER_UUID/data"
TEMP_FOLDER_INPUT="$TEMP_FOLDER_DATA/input"
TEMP_FOLDER_OUTPUT="$TEMP_FOLDER_DATA/output"
TEMP_FOLDER_ALGORITHM="$TEMP_FOLDER_UUID/algorithm"
TEMP_FOLDER_ALGORITHM_FILE="$TEMP_FOLDER_ALGORITHM/algorithm.py"

OUTPUT_FOLDER="$CWD/output_files"
INPUT_FOLDER="$CWD/input_files"
INPUT_FILES=("application_model.json" "configuration.json" "crossbar.xml" "jobs.json")

ELASTISIM_EXTENSIONS="$CWD/scheduling_algorithms/extension"

SCRIPTS_FOLDER="$CWD/scripts"
DOCKER_FILE="$SCRIPTS_FOLDER/Dockerfile"
GENERATE_INPUT_SCRIPT="$SCRIPTS_FOLDER/input_generation/jsonGenerator.py"
CLEANUP_SCRIPT="$SCRIPTS_FOLDER/output_evaluation/cleanupCsv.py"
GENERATE_FIGURES_SCRIPT="$SCRIPTS_FOLDER/output_evaluation/evaluateOutput.py"
GENERATE_FIGURES_PIP_PACKAGES=("pandas" "plotly" "kaleido")

DOCKERIMAGE_ELASTISIM="elastisim1.2"
DOCKERIMAGE_TAR_SAVE="$DATA_FOLDER/elastisim.tar"

ELASTISIM_GIT_COMMIT="9bc6344b03cacbba556fdc79a91a59f65e36c9f4"
ELASTISIM_PYTHON_GIT_COMMIT="871c191433d54b3b15eaef5ada2706a5a96c77bc"
DOCKERFILE_BUILD_ARGS="-f $DOCKER_FILE --build-arg elastisim_commit=$ELASTISIM_GIT_COMMIT  --build-arg elastisim_python_commit=$ELASTISIM_PYTHON_GIT_COMMIT"

dockerfileLinkFolders="-v $TEMP_FOLDER_DATA:/data -v $TEMP_FOLDER_ALGORITHM:/algorithm"
dockerfileOnlyElastisimEnv="--env=\"STARTUP_COMMAND=/simulation/elastisim/elastisim /data/input/configuration.json --log=root.thresh:warning\""

runElastisim="run $dockerfileLinkFolders $dockerfileOnlyElastisimEnv --name=elastisim -it --rm $DOCKERIMAGE_ELASTISIM"
runScheduler="exec -it elastisim python3 -B /algorithm/algorithm.py"
runSimulation="run $dockerfileLinkFolders --user $(id -u $USER):$(id -g $USER) --name=NAME_PLACEHOLDER --rm $DOCKERIMAGE_ELASTISIM"

#read args
if [[ -z $* ]] || [[ $* == "-u" ]]; then
	echo "Checking $* required installations..."
	installation=true
fi

function parse_path() {
	path=$1
	if [[ "${path}" != /* ]]; then
		path="$(cd "$(dirname "$path")"; pwd)/$(basename "$path")"
	fi
	if [[ "${path}" != */ ]] && [[ -d "$path" ]]; then
		path="${path}/"
	fi
	echo "$path"
}

function get_container_name() {
	para="$(basename "$input_arg")"
			  para=${para/"["/"_"}
			  para=${para//"|"/"_"}
			  para=${para/"]#"/"_"}
			  para=$(echo $para | cut -d'{' -f1)
	algo=$(basename "$algorithm")
    echo "${algo%%.*}-$para"
}

while getopts "hescfgquyi:a:o:" OPTION; do
	case "${OPTION}" in
	h) echo "./runElastisim.sh -e (Run Elastisim) -s (Run Scheduler) -c (Cleanup output data) -f (Generate Figures) -g (Generate input) -q (Quiet Mode) -u (Use udocker) -y (yes to all prompts) -i <Input-Path> -a <Algorithm-Path> -o <Output-Path>" ; exit;;
	e) elastisim=true;;
	s) scheduler=true;;
	c) cleanup=true;;
	f) generateFigures=true;;
	g) generateInput=true;;
	q) quiet=true;;
	u) udocker=true;;
	y) yes_prompts=true;;
	i) input_arg=$(parse_path "$OPTARG");;
	a) algorithm=$(parse_path "$OPTARG");;
	o) output=$(parse_path "$OPTARG");;
	*) echo "Unknown Argument"
	esac
done

output=$([[ ! -z $output ]] && echo "$output" || echo "$OUTPUT_FOLDER/$(basename "$input_arg")($(basename "$algorithm"))")
dockerCommand=$([[ -z $udocker ]] && echo "docker" || echo "udocker -q")
containerName="elastisim-$(get_container_name)-$uuid"

#deletes all files in a given folder, creates the folder if not present
function cleanup_temp_folder() {
	#delete temp folder if present
	if [[ -d "$TEMP_FOLDER_UUID" ]]; then
		rm -rf $TEMP_FOLDER_UUID
	fi
}

#Checks all files that will be needed for this execution
function check_files() {
	#scripts
	if  [ ! -z $cleanup ]; then
		[[ -f "$CLEANUP_SCRIPT" ]] || { echo "ERROR: script $CLEANUP_SCRIPT required"; exit 1; }
	fi
	if  [ ! -z $generateFigures ]; then #check figures scripts
		[[ -f "$GENERATE_FIGURES_SCRIPT" ]] || { echo "ERROR: script $GENERATE_FIGURES_SCRIPT required"; exit 1; }
	fi
	if  [ ! -z $generateInput ]; then
		[[ -f "$GENERATE_INPUT_SCRIPT" ]] || { echo "ERROR: script $GENERATE_INPUT_SCRIPT required" ; exit 1; }
	fi

	#scheduler
	if  [ ! -z $algorithm ]; then
		[[ -f "$algorithm" ]] || { echo "ERROR: scheduling-file $algorithm not found"; exit 1; }
		[[ "$algorithm" = *.py ]] || { echo "ERROR: scheduling-file $algorithm is not a python file"; exit 1; }
	fi

	#input
	if  [ ! -z $input_arg ]; then
		if  [ ! -z $generateInput ]; then
			[ -z $quiet ] && echo "Generating input files at $input_arg"
			python3 $GENERATE_INPUT_SCRIPT -q -d $input_arg || { echo "ERROR: Input Generation failed"; exit 1; }
		fi
		for file in "${INPUT_FILES[@]}"; do
  			[[ -f "$input_arg$file" ]] || { echo "ERROR: input-file $input_arg$file not found" ; exit 1; }
		done
	fi
}

function install_prompt() {
	echo $1
	while [ ! -z $yes_prompts ]; do
		read -p "$2 (yn): "
		case $(echo $REPLY) in
			[yY] ) break;;
			[nN] ) echo "$3"; exit 1;;
			* ) continue;;
		esac
	done
}

function install_udocker_image_if_missing() {
	if [ ! -x "$(command -v udocker)" ]; then
		install_prompt "udocker not installed" "Install udocker?" "Unable to continue without udocker"
		[ -z $quiet ] && echo "Installing udocker..."
		[ ! -d "$DATA_FOLDER" ] && mkdir $DATA_FOLDER
		cd $DATA_FOLDER
		if [[ ! -f "udocker-1.3.7.tar.gz" ]]; then
			wget -q https://github.com/indigo-dc/udocker/releases/download/1.3.7/udocker-1.3.7.tar.gz
		fi
    [ ! -d "$TEMP_FOLDER" ] && mkdir $TEMP_FOLDER
    cd $TEMP_FOLDER
    if [[ ! -d "udocker-1.3.7" ]]; then
      tar zxf ${DATA_FOLDER}/udocker-1.3.7.tar.gz
    fi
		export PATH=${TEMP_FOLDER}/udocker-1.3.7/udocker:$PATH
		cd $CWD
	fi
	if [[ ! $(udocker images) == *"$DOCKERIMAGE_ELASTISIM"* ]]; then
		install_prompt "udocker elastisim image missing" "Build the required udocker image?" "Unable to continue without the required udocker image"
		[ -z $quiet ] && echo "Installing udocker image..."

		if [ ! -f $DOCKERIMAGE_TAR_SAVE ] || [[ $(tar xfO ${DOCKERIMAGE_TAR_SAVE} manifest.json) != *"$DOCKERIMAGE_ELASTISIM"* ]]; then
			[[ $(tar xfO temp/elastisim.tar manifest.json) != *"$DOCKERIMAGE_ELASTISIM"* ]] && echo "Wrong container version for $DOCKERIMAGE_TAR_SAVE"
			install_docker_image_if_missing
			[[ ! -d "$TEMP_FOLDER" ]] && mkdir $TEMP_FOLDER
			[ -z $quiet ] && echo "Exporting docker image..."
			docker save -o $DOCKERIMAGE_TAR_SAVE $DOCKERIMAGE_ELASTISIM
		fi

		[ -z $quiet ] && echo "Importing udocker image..."
		udocker -q load -i $DOCKERIMAGE_TAR_SAVE $DOCKERIMAGE_ELASTISIM
	fi
}

function install_docker_image_if_missing() {
	[ -x "$(command -v docker)" ] || ( echo "Abort, Docker not installed"; exit 1; )
	if [[ "$(docker image inspect $DOCKERIMAGE_ELASTISIM)" == "[]"* ]]; then
		install_prompt "docker elastisim image missing" "Build the required image?" "Unable to continue without the required image"
		[ -z $quiet ] && echo "Installing docker image..."
		if ! [[ -e $DOCKER_FILE ]]; then
			echo "Unable to build docker image elastisim, dockerfile missing" ; exit 1;
		fi
		docker build -t $DOCKERIMAGE_ELASTISIM $DOCKERFILE_BUILD_ARGS .
	fi
}

#Checks if the elastiSim docker container is installed, prompts to install it if not present
function check_installations() {
	[ ! -d $TEMP_FOLDER ] && mkdir $TEMP_FOLDER
	if [ ! -z $elastisim ] || [ ! -z $scheduler ] || [ ! -z $installation ]; then
		if [ ! -z $udocker ]; then
			install_udocker_image_if_missing
		else
			install_docker_image_if_missing
		fi
	fi
	if [ ! -z $generateFigures ]; then
		for file in "${GENERATE_FIGURES_PIP_PACKAGES[@]}"; do
  			if ! [[ $(pip list) == *"$file"* ]] && [ ! -z $generateFigures ]; then
				while [ -z $yes_prompts ]; do
					read -p "Install the required pip package $file? (yn): "
					case $(echo $REPLY) in
						[yY] ) break;;
						[nN] ) echo "Unable to generate figures without $file"; exit 1;;
						* ) continue;;
					esac
				done
				pip install $file
			fi
		done
	fi
}

#copies all files that are needed for the simulation into the temp folder
function copy_files_to_temp() {
	if [ -z $input_arg ] || [ -z $algorithm ]; then
		echo "Unable to copy simulation files"
		if [ -z $input_arg ]; then
			echo "Input file not specified"
		fi
		if [ -z $algorithm ]; then
			echo "algorithm file not specified"
		fi
		exit 1
	fi

	#create folder structure
	cleanup_temp_folder
	mkdir -p $TEMP_FOLDER_INPUT $TEMP_FOLDER_OUTPUT $TEMP_FOLDER_ALGORITHM

	#copy files to temp
	cp -a $input_arg. $TEMP_FOLDER_INPUT || echo "Unable to copy input data to temp folder"
	cp $algorithm $TEMP_FOLDER_ALGORITHM_FILE  || echo "Unable to copy scheduler to temp folder"
	cp -ar $ELASTISIM_EXTENSIONS $TEMP_FOLDER_ALGORITHM || echo "Unable to copy elastisim extension to temp folder"
}

function secs_to_human() {
    echo "$(( ${1} / 3600 ))h $(( (${1} / 60) % 60 ))m $(( ${1} % 60 ))s"
}

#executes the simulation, either elastisim, elastisim and scheduler or only the scheduler
function run_simulation() {
	if [ ! -z $elastisim ]; then
		copy_files_to_temp
		STARTTIME=$(date +%s)
		if [ ! -z $scheduler ]; then
			[ -z $quiet ] && echo "Simulation with input $(basename "$input_arg") and algorithm $(basename "$algorithm") started"
      		runSimulationCommand=${runSimulation/NAME_PLACEHOLDER/$containerName}
        	eval "$dockerCommand $runSimulationCommand" || exit 1;
		else
			if [ ! -z $udocker ]; then
				echo "Unable to start Elastisim without Scheduler with udocker" ; exit 1;
			fi
			[ -z $quiet ] && echo "Running ElastiSim..."
			eval "$dockerCommand $runElastisim" || exit 1;
		fi
		ENDTIME=$(date +%s)
		[ -z $quiet ] && echo "Simulation with input $(basename "$input_arg") and algorithm $(basename "$algorithm") completed in $(secs_to_human "$(($ENDTIME - $STARTTIME))") seconds"

		#[ -z $quiet ] && du -h --max-depth=1 $TEMP_FOLDER_OUTPUT/* | sort -hr
		mkdir -p $output || echo "Unable to create output $output directory"
		cp -af $TEMP_FOLDER_OUTPUT/* $output  || echo "Unable to copy data to output directory"
	else
		if [ ! -z $scheduler ]; then
			if [ ! -z $udocker ]; then
				echo "Unable to start interactive Scheduler with udocker" ; exit 1;
			fi
			[ -z $quiet ] && echo "Running Scheduler..."
			eval "$dockerCommand $runScheduler"
			exit
		fi
	fi
}

#runs python programs to process the output
function evaluate_output() {
	if [ -z $cleanup ] && [ -z $generateFigures ]; then
		return
	fi

	if ! [[ -d "$output" ]]; then
		echo "ERROR: output-directoy $output not found"; exit 1;
	fi

	if [ ! -z $cleanup ]; then
		[ -z $quiet ] && echo "Cleaning Output"
		python3 $CLEANUP_SCRIPT $output
	fi

	if [ ! -z $generateFigures ]; then
		[ -z $quiet ] && echo "Generating Figures"
		python3 -W ignore $GENERATE_FIGURES_SCRIPT $output -fs
	fi
}

function finish() {
	exit_code=$?
	cleanup_temp_folder
	
	if [ ! -z $elastisim ] && [ ! -z $scheduler ]; then
		if [ -z $udocker ]; then
			docker container kill $containerName &>/dev/null
		fi
		if [ ! -z $udocker ] && [[ ! $exit_code -eq "0" ]]; then
			container_ids=$( udocker ps | cut -d' ' -f1 | tail -n +2 )
			echo $container_ids | xargs -n 1 udocker rm -f $0 &> /dev/null #kill all udocker container if exited without Code 0
		fi
	fi
}

trap finish EXIT

check_files
check_installations
[ -z $installation ] && run_simulation
evaluate_output
