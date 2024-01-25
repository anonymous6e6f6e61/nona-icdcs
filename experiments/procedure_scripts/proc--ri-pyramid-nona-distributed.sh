SCRIPT_NAME=$0

# this map is used by the query_deployment_helper_remote.sh to keep track of active queries
. queryset_queryable_map.sh
map-initialize
. log_times.sh

help ()
{
	echo "Usage:"
	echo "$SCRIPT_NAME KAFKA_CONTROL_HOST STATS_FOLDER KAFKA_SOURCE_HOST MARKER_FILEPATH WORKER1ROOT WORKER1SSH_PREFIX WORKER1JAR WORKER1FLINK_BIN WORKER2ROOT WORKER2SSH_PREFIX WORKER2JAR WORKER2FLINK_BIN WORKER3ROOT WORKER3SSH_PREFIX WORKER3JAR WORKER3FLINK_BIN"
}

log ()
{
	echo "[$SCRIPT_NAME] $1"
}



if [ "$#" -ne 16 ]; then
    echo "Wrong number of args: $#"
    help
    exit 1
fi

KAFKA_CONTROL_HOST="$1"
STATS_FOLDER="$2"
KAFKA_SOURCE_HOST="$3"
MARKER_LOCAL="$4"
WORKER1ROOT="$5"
WORKER1SSH_PREFIX="$6"
WORKER1JAR="$7"
WORKER1FLINK_BIN="$8"
WORKER2ROOT="${9}"
WORKER2SSH_PREFIX="${10}"
WORKER2JAR="${11}"
WORKER2FLINK_BIN="${12}"
WORKER3ROOT="${13}"
WORKER3SSH_PREFIX="${14}"
WORKER3JAR="${15}"
WORKER3FLINK_BIN="${16}"


TIMELOGGER_FILE="${STATS_FOLDER}/timelogger_bash.csv"
FLINK_DEPLOYMENT_FILE="${STATS_FOLDER}/flink_deployment_log.csv"

start_flink_remote ()
{
  # $1: sshprefix $2: remoteroot
  ssh "${1}" "cd ${2}/scripts; bash control_flink_cluster_odroid.sh restart"
}

stop_flink_remote ()
{
  # $1: sshprefix $2: remoteroot
  ssh "${1}" "cd ${2}/scripts; bash control_flink_cluster_odroid.sh stop"
}

add ()
{
  log-time "addition-start" "${TIMELOGGER_FILE}"
  # $1: query ID $2: classname $3: U $4: flinkpath $5: jar $6: sshprefix $7: remoteroot
  REMOTE_STATS_FOLDER="${7}"/$(basename "${STATS_FOLDER}")
  deployment_output=$(bash query_deployment_helper_remote.sh ${KAFKA_CONTROL_HOST} REQUESTS REPLIES ${4} "${1}" \
   add "${6}" ${7}/${5} ${2} ${3} --outputFile output.out --statisticsFolder ${STATS_FOLDER} \
   --provenanceActivator GENEALOG_UNFOLDED_TO_KAFKA --kafkaSourceBootstrapServer ${KAFKA_SOURCE_HOST} \
   --kafkaSinkBootstrapServer ${KAFKA_CONTROL_HOST})
  if [ $? -ne 0 ]; then
      exit 1
  fi
  log "Added query $1 on worker $6."
  echo "${deployment_output}" | tee -a "${FLINK_DEPLOYMENT_FILE}"
  log-time "addition-end" "${TIMELOGGER_FILE}"
}

remove ()
{
  log-time "removal-start" "${TIMELOGGER_FILE}"
  # $1: query ID $2: flinkpath $3: sshprefix $4: root
  kill_output=$(bash query_deployment_helper_remote.sh ${KAFKA_CONTROL_HOST} REQUESTS REPLIES "${2}" "${1}" \
  remove "${3}" "${MARKER_LOCAL}")
  if [ $? -ne 0 ]; then
        exit 1
  fi
  log "Removed query $1 from worker $3."
  echo "${kill_output}" | tee -a "${FLINK_DEPLOYMENT_FILE}"
}

create_remote_stats_folder ()
{
  # $1: sshprefix $2: remoteroot
  REMOTE_STATS_FOLDER="${2}"/$(basename "${STATS_FOLDER}")
  ssh "${1}" "rm -rf ${REMOTE_STATS_FOLDER}"
  ssh "${1}" "mkdir ${REMOTE_STATS_FOLDER}"
}

copy_results_from_remote ()
{
  # $1: sshprefix $2: remoteroot
  REMOTE_STATS_FOLDER="${2}"/$(basename "${STATS_FOLDER}")
  scp -r "${1}":"${REMOTE_STATS_FOLDER}"/* "${STATS_FOLDER}"
}

finished=false
trap_func ()
{
	if [ "$finished" = true ] ; then
		log "cancelled [was finished]"
		exit 0
	else
		log "cancelled [WAS NOT FINISHED]"
		stop_flink_remote "${WORKER1SSH_PREFIX}" "${WORKER1ROOT}"
    stop_flink_remote "${WORKER2SSH_PREFIX}" "${WORKER2ROOT}"
    stop_flink_remote "${WORKER3SSH_PREFIX}" "${WORKER3ROOT}"
		exit 1
	fi
}

init ()
{
  log "sending init message"
  bash query_deployment_helper_remote.sh "${KAFKA_CONTROL_HOST}" REQUESTS REPLIES "${FLINK_BIN_PATH}" none init
}

trap trap_func SIGTERM

# PROCEDURE BEGINS BELOW THIS LINE #

CLASS="streamingRetention.usecases.riot.queries.RiotStats"
U=15000

#### setup work on remotes ####
create_remote_stats_folder "${WORKER1SSH_PREFIX}" "${WORKER1ROOT}"
create_remote_stats_folder "${WORKER2SSH_PREFIX}" "${WORKER2ROOT}"
create_remote_stats_folder "${WORKER3SSH_PREFIX}" "${WORKER3ROOT}"

start_flink_remote "${WORKER1SSH_PREFIX}" "${WORKER1ROOT}"
start_flink_remote "${WORKER2SSH_PREFIX}" "${WORKER2ROOT}"
start_flink_remote "${WORKER3SSH_PREFIX}" "${WORKER3ROOT}"
##############################

init
sleep 15

add STATS0 "${CLASS}" "${U}" "${WORKER1FLINK_BIN}" "${WORKER1JAR}" "${WORKER1SSH_PREFIX}" "${WORKER1ROOT}"
sleep 60
add STATS1 "${CLASS}" "${U}" "${WORKER2FLINK_BIN}" "${WORKER2JAR}" "${WORKER2SSH_PREFIX}" "${WORKER2ROOT}"
sleep 60
add STATS2 "${CLASS}" "${U}" "${WORKER3FLINK_BIN}" "${WORKER3JAR}" "${WORKER3SSH_PREFIX}" "${WORKER3ROOT}"
sleep 60
add STATS3 "${CLASS}" "${U}" "${WORKER1FLINK_BIN}" "${WORKER1JAR}" "${WORKER1SSH_PREFIX}" "${WORKER1ROOT}"
sleep 60
add STATS4 "${CLASS}" "${U}" "${WORKER2FLINK_BIN}" "${WORKER2JAR}" "${WORKER2SSH_PREFIX}" "${WORKER2ROOT}"
sleep 60
add STATS5 "${CLASS}" "${U}" "${WORKER3FLINK_BIN}" "${WORKER3JAR}" "${WORKER3SSH_PREFIX}" "${WORKER3ROOT}"
sleep 60
add STATS6 "${CLASS}" "${U}" "${WORKER1FLINK_BIN}" "${WORKER1JAR}" "${WORKER1SSH_PREFIX}" "${WORKER1ROOT}"
sleep 60
add STATS7 "${CLASS}" "${U}" "${WORKER2FLINK_BIN}" "${WORKER2JAR}" "${WORKER2SSH_PREFIX}" "${WORKER2ROOT}"
sleep 60
add STATS8 "${CLASS}" "${U}" "${WORKER3FLINK_BIN}" "${WORKER3JAR}" "${WORKER3SSH_PREFIX}" "${WORKER3ROOT}"
sleep 60

remove STATS0 "${WORKER1FLINK_BIN}" "${WORKER1SSH_PREFIX}" "${WORKER1ROOT}"
sleep 60
remove STATS1 "${WORKER2FLINK_BIN}" "${WORKER2SSH_PREFIX}" "${WORKER2ROOT}"
sleep 60
remove STATS2 "${WORKER3FLINK_BIN}" "${WORKER3SSH_PREFIX}" "${WORKER3ROOT}"
sleep 60
remove STATS3 "${WORKER1FLINK_BIN}" "${WORKER1SSH_PREFIX}" "${WORKER1ROOT}"
sleep 60
remove STATS4 "${WORKER2FLINK_BIN}" "${WORKER2SSH_PREFIX}" "${WORKER2ROOT}"
sleep 60
remove STATS5 "${WORKER3FLINK_BIN}" "${WORKER3SSH_PREFIX}" "${WORKER3ROOT}"
sleep 60
remove STATS6 "${WORKER1FLINK_BIN}" "${WORKER1SSH_PREFIX}" "${WORKER1ROOT}"
sleep 60
remove STATS7 "${WORKER2FLINK_BIN}" "${WORKER2SSH_PREFIX}" "${WORKER2ROOT}"
sleep 60
remove STATS8 "${WORKER3FLINK_BIN}" "${WORKER3SSH_PREFIX}" "${WORKER3ROOT}"
sleep 60


### teardown work on remotes ###
stop_flink_remote "${WORKER1SSH_PREFIX}" "${WORKER1ROOT}"
stop_flink_remote "${WORKER2SSH_PREFIX}" "${WORKER2ROOT}"
stop_flink_remote "${WORKER3SSH_PREFIX}" "${WORKER3ROOT}"

copy_results_from_remote "${WORKER1SSH_PREFIX}" "${WORKER1ROOT}"
copy_results_from_remote "${WORKER2SSH_PREFIX}" "${WORKER2ROOT}"
copy_results_from_remote "${WORKER3SSH_PREFIX}" "${WORKER3ROOT}"
################################



finished=true
log "Transition procedure completed."
python3 flink_job_stopper.py 0
