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
# Modified version of: https://github.com/elastisim/example-project/blob/main/Dockerfile
# ---------------------------------------------------------------------
FROM ubuntu:22.04

ARG elastisim_commit main
ARG elastisim_python_commit main

RUN echo $elastisim_commit

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update --fix-missing
RUN apt-get install -y build-essential cmake libboost-dev libzmq3-dev python3 python3-zmq git

RUN mkdir /simulation

ADD https://framagit.org/simgrid/simgrid/-/archive/v3.31/simgrid-v3.31.tar.gz /simulation
WORKDIR /simulation
RUN tar -xvzf simgrid-v3.31.tar.gz
WORKDIR /simulation/simgrid-v3.31
RUN cmake -DCMAKE_INSTALL_PREFIX=/simulation/simgrid .
RUN make -j12
RUN make install

RUN git clone https://github.com/elastisim/elastisim.git /simulation/elastisim
WORKDIR /simulation/elastisim
RUN git checkout $elastisim_commit
RUN cmake -DCMAKE_INSTALL_PREFIX=/simulation/elastisim -DSIMGRID_SOURCE_DIR=/simulation/simgrid -DCMAKE_BUILD_TYPE="Release" .
RUN make -j12

WORKDIR /simulation/
RUN git clone https://github.com/elastisim/elastisim-python.git
WORKDIR /simulation/elastisim-python
RUN git checkout $elastisim_python_commit
WORKDIR /simulation/
ENV PYTHONPATH "${PYTHONPATH}:/simulation/elastisim-python"

WORKDIR /
ENV STARTUP_COMMAND="/simulation/elastisim/elastisim /data/input/configuration.json --log=root.thresh:warning & { sleep 1 && python3 -B algorithm/algorithm.py; }; wait"
CMD bash -c "$STARTUP_COMMAND"