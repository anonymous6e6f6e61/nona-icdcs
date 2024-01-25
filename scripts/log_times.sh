#!/bin/bash

init-log-times() {
  	rm "${1}" 2> /dev/null
	  touch "${1}"
}

log-time() {
    logged_time=$(date +%s%3N)   # Get current time in milliseconds
    echo "${1}","${logged_time}" >> "${2}"
}