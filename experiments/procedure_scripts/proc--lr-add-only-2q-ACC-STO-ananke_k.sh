SCRIPT_NAME=$0

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

add-force ()
{
  # $1: query ID $2: classname $3: U
  bash query_deployment_forcer.sh "${KAFKA_CONTROL_HOST}" REQUESTS REPLIES "${FLINK_BIN_PATH}" "${1}" \
  add "${JAR}" "${2}" "${3}" --outputFile output.out --statisticsFolder "${STATS_FOLDER}" \
  --provenanceActivator GENEALOG_UNFOLDED_TO_KAFKA --kafkaSourceBootstrapServer "${KAFKA_SOURCE_HOST}" \
  || exit 1
}

trap trap_func SIGTERM

# PROCEDURE BEGINS BELOW THIS LINE #

CLASS1="streamingRetention.usecases.linearRoad.queries.LinearRoadAccident"
CLASS2="streamingRetention.usecases.linearRoad.queries.LinearRoadStoppedVehicles"
U1=150000
U2=120000

add-force ACCIDENTS "${CLASS1}" "${U1}"
sleep 5
add-force STOPPED "${CLASS2}" "${U2}"

# PROCEDURE ENDS ABOVE THIS LINE #

finished=true
log "Transition procedure completed."
log "Queries may still be deployed!"

# enter infinite loop to keep queries running
while true ; do
	sleep 1
done