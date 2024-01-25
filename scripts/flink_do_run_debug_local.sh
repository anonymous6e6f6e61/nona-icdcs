#!/usr/bin/env bash
# credit XXX

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)

# Flink folder must exist at the top-level repository directory
FLINK_VERSION="1.10.0"
FLINK_PATH="$script_dir/../flink-${FLINK_VERSION}"
START_CLUSTER="$FLINK_PATH/bin/start-cluster.sh"
STOP_CLUSTER="$FLINK_PATH/bin/stop-cluster.sh"
[[ -e $FLINK_PATH ]] || { echo "Error: Flink not found at $FLINK_PATH"; exit 1; }

FORCE_GC_CMD="jcmd | grep org.apache.flink.runtime.taskexecutor.TaskManagerRunner | cut -d ' ' -f 1 | xargs -I {} jcmd {} GC.run"

# CLI ARGS
usage() { echo "Usage: $0 FLINK_COMMAND STAT_FOLDER DURATION_SECONDS" 1>&2; exit 1; }

# Experiment commands
if [ "$#" -ne 3 ]; then
  usage
  exit 1
fi

spe_pid=""
spe_output=""
utilization_pid=""
job_stopper_pid=""

checkStatusAndExit() {
  echo "Checking status and exiting"
  wait "$spe_pid"
  spe_exit_code="$?"
  if [[ $spe_output == *JobCancellationException* ]]; then
    echo "[EXEC] Flink Job Cancelled"
    exit 0
  fi
  if [[ $spe_output == *JobSubmissionException* ]]; then
    echo "$spe_output"
    echo "[EXEC] Failed to submit flink job!"
    exit 1
  fi
  if (( spe_exit_code > 0 && spe_exit_code < 128)); then
    echo "[EXEC] Reproducing SPE output:"
    echo "$spe_output"
    echo "[EXEC] SPE exited with error: $spe_exit_code"
    exit "$spe_exit_code"
  fi
  echo "$spe_output"
  echo "[EXEC] Success (exit code: $spe_exit_code)"
  exit 0
}

stopFlinkCluster() {
  eval "$STOP_CLUSTER"
  sleep 10
  # Make absolutely sure that there is no leftover TaskManager
  pgrep -f "org.apache.flink.runtime.taskexecutor.TaskManagerRunner" | xargs -I {} kill -9 {}
  rm -r /tmp/flink-*  &>/dev/null
  rm -r /tmp/blobStore-*  &>/dev/null
}

startFlinkCluster() {
  eval "$START_CLUSTER"
  sleep 10
}

clearActiveProcs() {
  [[ -n $utilization_pid ]] && kill "$utilization_pid"
  [[ -n $job_stopper_pid ]] && kill "$job_stopper_pid"
  stopFlinkCluster
  REAL_DURATION=$SECONDS
  echo "[EXEC] Experiment Duration: $REAL_DURATION"
  checkStatusAndExit
}

trap clearActiveProcs SIGINT SIGTERM

stopFlinkCluster

startFlinkCluster

echo "Forcing GC"
eval "$FORCE_GC_CMD" > /dev/null


python3 "flink_job_stopper.py" "$2" &
job_stopper_pid="$!"

echo "Executing query..." 1>&2
SECONDS=0
spe_output=$($1 2>&1)
spe_pid="$!"

echo "Query ended without external termination"
clearActiveProcs