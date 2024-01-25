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
FORCE_FLINK_TASKSET="jcmd | grep flink | cut -d ' ' -f 1 | xargs -I {} taskset -apc 4-7 {}"


SCRIPT_NAME="$0"
log () {
  echo "[$SCRIPT_NAME] $1"
}

# CLI ARGS
usage() {
  log "Usage: $0 [start | stop | restart] " 1>&2
}

if [ "$#" -ne 1 ]; then
  usage
  exit 1
fi

MODE="${1}"

#########

stopFlinkCluster() {
  eval "$STOP_CLUSTER"
  sleep 15
  # Make absolutely sure that there is no leftover TaskManager
  pgrep -f "org.apache.flink.runtime.taskexecutor.TaskManagerRunner" | xargs -I {} kill -9 {}
  rm -r /tmp/flink-*  &>/dev/null
  rm -r /tmp/blobStore-*  &>/dev/null
}

startFlinkCluster() {
  eval "$START_CLUSTER"
  sleep 15
  log "Forcing GC"
  eval "$FORCE_GC_CMD" > /dev/null
  log "Forcing taskset"
  eval "$FORCE_FLINK_TASKSET" > /dev/null
}

restartFlinkCluster() {
  stopFlinkCluster
  sleep 3
  startFlinkCluster
}

if [[ "$MODE" == "start" ]]; then
  startFlinkCluster

elif [[ "$MODE" == "stop" ]]; then
  stopFlinkCluster

elif [[ "$MODE" == "restart" ]]; then
  restartFlinkCluster

else
  log "Unknown argument."
  usage
fi


