#!/bin/bash

. queryset_queryable_map.sh
. log_times.sh

KAFKA_SERVER_PORT="${1}"
WRITE_TOPIC="${2}"
READ_TOPIC="${3}"
FLINK_PATH="${4}"
QUERY_ID="${5}"
MODE="${6}"

PARTITION=0
FLINK_TRANSITION_LOG="flink_transitions.log"

SCRIPT_NAME="$0"
log ()
{
  echo "[$SCRIPT_NAME] $1"
}

echo "-------------------" >> "$FLINK_TRANSITION_LOG"
date >> "$FLINK_TRANSITION_LOG"

if [[ "$MODE" == "add" ]]; then
  if [ $# -lt 9 ]; then
    log "At least 9 arguments required for mode 'add'"
    exit 1
  fi
  QUERY_JAR="${7}"
  QUERY_CLASS="${8}"
  QUERY_U="${9}"
  EXTRA_ARGS="${*:10}"

  # compose message
  message="${QUERY_ID},${MODE},${QUERY_U}"

  echo "${message}" | kcat -P -t "${WRITE_TOPIC}" -p ${PARTITION} -b "${KAFKA_SERVER_PORT}"
  log "Sent request: $message"

  TIMEOUT=100
  KILL_LENGTH=30
  KEYWORD="${QUERY_ID},accepted"
  reply=$(timeout --kill-after=$KILL_LENGTH "$TIMEOUT" bash kafka_keyword_listener.sh "$READ_TOPIC" "$PARTITION" "$KAFKA_SERVER_PORT" \
           "$KEYWORD")
  listener_exit_code=$?
  if [ "$listener_exit_code" -ne 0 ]; then
    log "Listener process timed out, expected addition confirmation. Probably no confirmation from Kafka. Exiting."
    exit 1
  fi

  log "Response: $reply"

  lower_bound_source=$(echo "${reply}" | awk -F, ' { print $3 } ')
  start=$(log-time "flink-start-deploy" /dev/fd/1)
  flink_output=$("${FLINK_PATH}"/flink run -d  --class "${QUERY_CLASS}" "${QUERY_JAR}" \
                          --queryID $QUERY_ID \
                          --sourceLowerBoundTimestamp $lower_bound_source \
                          --queryU $QUERY_U \
                          ${EXTRA_ARGS})
  end=$(log-time "flink-end-deploy" /dev/fd/1)
  echo "${start}"
  echo "${end}"
  flink_exit_code=$?
  if [ $flink_exit_code -ne 0 ]; then
    log "ERROR: Flink returned non-zero exit code [$flink_exit_code] when trying execute 'flink run'"
    exit 1
  fi

  echo "$flink_output" >> "$FLINK_TRANSITION_LOG"
  # the job_id is the last element of the line containing the word "JobID"
  job_id=$(echo "${flink_output}" | grep JobID | awk '{print $(NF)}' )
  # store combination of query id and job id to file
  map-add "${QUERY_ID}" "${job_id}"
    cat <<EOF
  [$SCRIPT_NAME] Added query:
  [$SCRIPT_NAME]    query_id: ${QUERY_ID}
  [$SCRIPT_NAME]    flink job_id: ${job_id}
  [$SCRIPT_NAME]    class: ${QUERY_CLASS}
  [$SCRIPT_NAME]    jar: ${QUERY_JAR}
  [$SCRIPT_NAME]    source start timestamp [ms]: ${lower_bound_source}
  [$SCRIPT_NAME]    extra args: ${EXTRA_ARGS}
EOF

elif [[ "$MODE" == "remove" ]]; then
  if [ $# -ne 7 ]; then
    log "Exactly 7 arguments required for mode 'remove'"
    exit 1
  fi
  MARKER_FILEPATH="${7}"
  # insert marker
  bash marker_inserter.sh "${KAFKA_SERVER_PORT}" PROVENANCE "${QUERY_ID}" "${MARKER_FILEPATH}" && log "Inserted marker for ${QUERY_ID}" || exit 1
  # find job id from the query id
  job_id=$(map-get "${QUERY_ID}")
  # cancel flink task IN THE BACKGROUND
  "${FLINK_PATH}"/flink cancel "${job_id}" >> "$FLINK_TRANSITION_LOG" &

  TIMEOUT=300
  KILL_LENGTH=30
  KEYWORD="${QUERY_ID},removed"
  confirmation=$(timeout --kill-after=$KILL_LENGTH "$TIMEOUT" bash kafka_keyword_listener.sh "$READ_TOPIC" "$PARTITION" "$KAFKA_SERVER_PORT" \
           "$KEYWORD")
  listener_exit_code=$?
  if [ "$listener_exit_code" -ne 0 ]; then
    log "Listener process timed out, expected removal confirmation. Probably no confirmation from Kafka. Exiting."
    exit 1
  fi

  log "Received: $confirmation"
  confirmation_query_id=$(echo "${confirmation}" | awk -F, ' { print $1 } ')

  if [[ "$confirmation_query_id" != "$QUERY_ID" ]]; then
    log "Expected confirmation for query id [$QUERY_ID], but received answer for query id [$confirmation_query_id]. Exiting."
    exit 1
  fi

  cat <<EOF
[$SCRIPT_NAME] Removed query:
  id: ${QUERY_ID}
EOF

elif [[ "$MODE" == "init" ]]; then
  if [ $# -ne 6 ]; then
      log "Exactly 6 arguments required for mode 'init'"
      exit 1
  fi

  message="none,${MODE},0"
  echo "${message}" | kcat -P -t "${WRITE_TOPIC}" -p ${PARTITION} -b "${KAFKA_SERVER_PORT}"
  log "Sent init message."

  TIMEOUT=100
  KILL_LENGTH=30
  confirmation=$(timeout --kill-after=$KILL_LENGTH "$TIMEOUT" bash kafka_keyword_listener.sh "$READ_TOPIC" "$PARTITION" "$KAFKA_SERVER_PORT" \
             "init,received")
  listener_exit_code=$?
  if [ "$listener_exit_code" -ne 0 ]; then
    log "Listener process timed out, expected init reception confirmation. Probably no confirmation from Kafka. Exiting."
    exit 1
  fi

else
  log "Wrong usage."
  exit 1
fi
