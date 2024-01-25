#!/bin/bash

if [ $# -ne 4 ]; then
    echo "Invalid number of arguments. Expected 4 arguments."
    echo "Usage: $0 KAFKA_SERVER_PORT TOPIC QUERY_ID MARKER_FILEPATH"
    exit 1
fi

log ()
{
  echo "[$SCRIPT_NAME] $1"
}

KAFKA_SERVER_PORT="${1}"
TOPIC="${2}"
QUERY_ID="${3}"
MARKER_FILEPATH="${4}"
PARTITION=0

#echo "$QUERY_ID:" | kcat -b "${KAFKA_SERVER_PORT}" -t "${TOPIC}" -P -Z -K:
kcat -P -b "${KAFKA_SERVER_PORT}" -t "${TOPIC}" -p "${PARTITION}" -k "${QUERY_ID}" "${MARKER_FILEPATH}"
log "Sent marker: port ${KAFKA_SERVER_PORT}, topic ${TOPIC}, partition ${PARTITION}, query ${QUERY_ID}, marker copied from ${MARKER_FILEPATH}"