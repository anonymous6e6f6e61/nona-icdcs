#!/bin/bash

KAFKA_SERVER_PORT="${1}"
WRITE_TOPIC="${2}"
READ_TOPIC="${3}"
FLINK_PATH="${4}"
QUERY_ID="${5}"
MODE="${6}"

PARTITION=0

SCRIPT_NAME="$0"

log ()
{
  echo "[$SCRIPT_NAME] $1"
}

if [[ "$MODE" != "add" ]]; then
  log "Unsupported mode!"
  exit 1
fi

QUERY_JAR="${7}"
QUERY_CLASS="${8}"
QUERY_U="${9}"
EXTRA_ARGS="${*:10}"

"${FLINK_PATH}"/flink run -d  --class "${QUERY_CLASS}" "${QUERY_JAR}" \
                        --queryID $QUERY_ID \
                        --sourceLowerBoundTimestamp 0 \
                        --queryU $QUERY_U \
                        ${EXTRA_ARGS} || exit 1
cat <<EOF
[$SCRIPT_NAME] Added query by force:
[$SCRIPT_NAME]    id: ${QUERY_ID}
[$SCRIPT_NAME]    class: ${QUERY_CLASS}
[$SCRIPT_NAME]    jar: ${QUERY_JAR}
[$SCRIPT_NAME]    source start timestamp [ms]: 0
[$SCRIPT_NAME]    extra args: ${EXTRA_ARGS}
EOF