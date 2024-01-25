SCRIPT_NAME=$0

# this map is used by the query_deployment_helper.sh to keep track of active queries
. queryset_queryable_map.sh
map-initialize
. log_times.sh

help ()
{
	echo "Usage:"
	echo "$SCRIPT_NAME PATH_TO_JAR KAFKA_CONTROL_HOST STATS_FOLDER FLINK_BIN_PATH KAFKA_SOURCE_HOST MARKER_FILEPATH"
}

log ()
{
	echo "[$SCRIPT_NAME] $1"
}

finished=false
trap_func () 
{
	if [ "$finished" = true ] ; then
		log "cancelled [was finished]"
		exit 0
	else
		log "cancelled [WAS NOT FINISHED]"
		exit 1
	fi
}

if [ "$#" -ne 6 ]; then
    help
    exit 1
fi

JAR="$1"
KAFKA_CONTROL_HOST="$2"
STATS_FOLDER="$3"
FLINK_BIN_PATH="$4"
KAFKA_SOURCE_HOST="$5"
MARKER_FILEPATH="$6"

TIMELOGGER_FILE="${STATS_FOLDER}/timelogger_bash.csv"
FLINK_DEPLOYMENT_FILE="${STATS_FOLDER}/flink_deployment_log.csv"

add ()
{
  log-time "addition-start" "${TIMELOGGER_FILE}"
  # $1: query ID $2: classname $3: U $4-...: extra args for the class
  EXTRA_ARGS="${*:4}"
  deployment_output=$(bash query_deployment_helper.sh "${KAFKA_CONTROL_HOST}" REQUESTS REPLIES "${FLINK_BIN_PATH}" "${1}" \
  add "${JAR}" "${2}" "${3}" --outputFile output.out --statisticsFolder "${STATS_FOLDER}" \
  --provenanceActivator GENEALOG_UNFOLDED_TO_KAFKA --kafkaSourceBootstrapServer "${KAFKA_SOURCE_HOST}" ${EXTRA_ARGS})
  if [ $? -ne 0 ]; then
      exit 1
  fi
  echo "${deployment_output}" | tee -a "${FLINK_DEPLOYMENT_FILE}"
  log-time "addition-end" "${TIMELOGGER_FILE}"
}

remove ()
{
  log-time "removal-start" "${TIMELOGGER_FILE}"
  # $1: query ID
  kill_output=$(bash query_deployment_helper.sh "${KAFKA_CONTROL_HOST}" REQUESTS REPLIES "${FLINK_BIN_PATH}" "${1}" \
  remove "${MARKER_FILEPATH}")
  if [ $? -ne 0 ]; then
        exit 1
  fi
  echo "${kill_output}" | tee -a "${FLINK_DEPLOYMENT_FILE}"
}

trap trap_func SIGTERM

# PROCEDURE BEGINS BELOW THIS LINE #

CLASS="streamingRetention.usecases.synthetic.queries.SimpleSyntheticQuery"
U=100000

for iteration in 0 1 2 3 4 5 6 7 8 9; do
  add SYNTHETIC"${iteration}" "${CLASS}" "${U}" --synthetic-sleep-between-batches 20 --synthetic-batch-size 4
  sleep 5
done

for iteration in 0 1 2 3 4 5 6 7 8 9; do
  remove SYNTHETIC"${iteration}"
  sleep 15
done

# PROCEDURE ENDS ABOVE THIS LINE #

finished=true
log "Transition procedure completed."
python3 flink_job_stopper.py 0
