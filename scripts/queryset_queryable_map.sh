#!/bin/bash

# for portability, we bring our own version of mapfile
. mapfile.sh

F=".query_set"

map-initialize () {
	rm "${F}" 2> /dev/null
	touch "${F}"
}

map-add () {
	if [[ "$#" != 2 ]]; then
		echo "Must be key and value separated by space"!
		return 1
	fi
	echo "${1}" "${2}" >> "${F}"
}

map-get () {
	mapfile TEMP < "${F}"
	local key=""
	local success=false
	for entry in "${TEMP[@]}"; do
		if echo "$entry" | grep -q "$1"; then
			key="${entry}"
			success=true
			break
		fi
	done

	if [[ $success == "false" ]]; then
		return 1
	fi

	local value=$(echo "${key}" | awk '{ print $2 }' )
	echo "${value}"
}

map-remove () {
	grep -v "${1}" "${F}" > ".temp" && mv ".temp" "${F}"
}
