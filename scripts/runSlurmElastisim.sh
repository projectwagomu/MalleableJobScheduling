#!/bin/bash
## ---------------------------------------------------------------------
## Copyright (c) 2023 Wagomu project.
##
## This program and the accompanying materials are made available to you under
## the terms of the Eclipse Public License 1.0 which accompanies this
## distribution,
## and is available at https://www.eclipse.org/legal/epl-v20.html
##
## SPDX-License-Identifier: EPL-2.0
## ---------------------------------------------------------------------
#SBATCH --job-name=Test
#SBATCH --output=out/%x.%j.out
#SBATCH --error=out/%x.%j.err
#SBATCH --time=02-00:00:00
#SBATCH --mem=0
##Is set automatically via runSimulations -s partition
##SBATCH --partition=partition

CWD=$1
ARGS="${@:2}"
TEMP_FOLDER=$CWD"/temp"
DATA_FOLDER=$CWD"/data"
#We install uDocker directly into the RAM of the executing node
LOCAL_FOLDER="/dev/shm/1337"
rm -rf $LOCAL_FOLDER
mkdir $LOCAL_FOLDER
LOCAL_UDOCKER_FOLDER=$LOCAL_FOLDER"/udocker-1.3.7"

DOCKERIMAGE_TAR_SAVE="$DATA_FOLDER/elastisim.tar"
DOCKERIMAGE_ELASTISIM="elastisim1.2"

function secs_to_human() {
    echo "$(( ${1} / 3600 ))h $(( (${1} / 60) % 60 ))m $(( ${1} % 60 ))s"
}

function udockerSetup() {
	export UDOCKER_TARBALL=$DATA_FOLDER/udocker-englib-1.2.8.tar.gz
	tar zxvf $DATA_FOLDER/udocker-1.3.7.tar.gz --directory $LOCAL_FOLDER
	export UDOCKER_REPOS=$LOCAL_UDOCKER_FOLDER/udocker/repos
	export UDOCKER_LAYERS=$LOCAL_UDOCKER_FOLDER/udocker/layers
	export UDOCKER_CONTAINERS=$LOCAL_UDOCKER_FOLDER/udocker/containers
	export UDOCKER_DIR=$LOCAL_UDOCKER_FOLDER/udocker
	export PATH=$PATH:$UDOCKER_DIR:$UDOCKER_DIR/bin
	udocker install
	udocker -q load -i $DOCKERIMAGE_TAR_SAVE $DOCKERIMAGE_ELASTISIM
}

function runSimulations() {
	current=""
	c=0
	for arg in $ARGS; do
		current+=" ${arg}"
		c=$(($c+1))
		if [ $c -eq 5 ]; then
			./runElastisim.sh ${current} &
			current=""
			c=0
		fi
	done
	wait
}

function finish() {
	rm -rf $LOCAL_FOLDER
	exit
}

trap finish EXIT

udockerSetup
STARTTIME=$(date +%s)
runSimulations
ENDTIME=$(date +%s)
echo "Simulations completed in $(secs_to_human "$(($ENDTIME - $STARTTIME))") seconds"