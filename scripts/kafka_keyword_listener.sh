#!/bin/bash

# starts a Kafka consumer listening on READ_TOPIC from offset end-1
# until a message containing KEYWORD is produced.
# said message is then put to STDOUT

READ_TOPIC=$1
PARTITION=$2
KAFKA_SERVER_PORT=$3
KEYWORD=$4

if [ $# -ne 4 ]; then
  echo "Require 4 args, received $#"
  exit 1
fi

# use kcat unbuffered (-u) to write each line immediately to STDOUT
KAFKA_COMMAND="kcat -C -t ${READ_TOPIC} -p ${PARTITION} -b ${KAFKA_SERVER_PORT} -q -o -1 -u"
while IFS= read -r newline; do
  if [[ "$newline" == *"$KEYWORD"* ]]; then
    echo "$newline"
    break
  fi
done < <(eval "$KAFKA_COMMAND")