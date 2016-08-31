#!/bin/bash

# Starts a test glow cluster of 1 master and 3 agents. Intends to be used in
# tests only. Run this script in a separate shell window under the root
# directory of the git repo. Press ctrl-c to kill all started processes.

declare -r MASTER_PORT=8930
declare -r MASTER_ADDRESS="localhost:${MASTER_PORT}"
declare -r AGENT_PORT1=8931
declare -r AGENT_PORT2=8932
declare -r AGENT_PORT3=8933

declare -r GLOW_BASE_DIR=/tmp/glow

declare -a pids

# Function to be called when user press ctrl-c.
function ctrl_c() {
  for pid in "${pids[@]}"
  do
    echo "Killing ${pid}..."
    kill ${pid}

    # This suppresses the annoying "[pid] Terminated ..." message.
    wait ${pid} &>/dev/null
  done
}

trap ctrl_c SIGINT
echo "You may press ctrl-c to kill all started processes..."

go build glow.go
glow master --address="${MASTER_ADDRESS}" &>/dev/null &
pids+=($!)
echo "Started glow master at ${MASTER_ADDRESS}, pid: $!"

glow agent --dir="${GLOW_BASE_DIR}/agent1" --max.executors=5 --memory=500 \
  --master="${MASTER_ADDRESS}" --port="${AGENT_PORT1}" &>/dev/null &
pids+=($!)
echo "Started glow agent, pid: $!"

glow agent --dir="${GLOW_BASE_DIR}/agent2" --max.executors=5 --memory=500 \
  --master="${MASTER_ADDRESS}" --port="${AGENT_PORT2}" &>/dev/null &
pids+=($!)
echo "Started glow agent, pid: $!"

glow agent --dir="${GLOW_BASE_DIR}/agent3" --max.executors=5 --memory=500 \
  --master="${MASTER_ADDRESS}" --port="${AGENT_PORT3}" &>/dev/null &
pids+=($!)
echo "Started glow agent, pid: $!"

echo
echo "Sleep for 10000 seconds..."
sleep 10000
