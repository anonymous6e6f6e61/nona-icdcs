#!/usr/bin/env bash
# credit XXX


[[ -z $1 ]] && { echo "[utilization-monitoring.sh] Please provide valid statistics folder"; exit 1; }
[[ ! -e $1 ]] && { echo "[utilization-monitoring.sh] Statistics folder not found"; exit 1; }
[[ -z $2 ]] && { echo "[utilization-monitoring.sh] Please provide a name for the process to find"; exit 1; }

CPU_FILE="$1/cpu-$2.csv"
MEMORY_FILE="$1/memory-$2.csv"
HOSTNAME=$(hostname)

NUMBER_CPUS=$(nproc)
TOTAL_MEMORY=$(awk '/MemTotal/ {print $2 / 1024}' /proc/meminfo)

recordUtilization() {
    CPUMEM=$(top -b -n 1 -p"$(pgrep -d, -f "$1")" 2> /dev/null)
    CPUMEM=$(echo "$CPUMEM" | awk -v ncpu="$NUMBER_CPUS" -v nmem="$TOTAL_MEMORY" 'BEGIN{ cpu = 0; memory = 0; } NR > 7 { cpu+=$9; memory += $10 } END { print cpu/ncpu, memory*nmem/100; }')
    CPU=$(echo "$CPUMEM" | cut -d' ' -f1)
    MEM=$(echo "$CPUMEM" | cut -d' ' -f2)
    TIME_SECONDS=$(date +%s)
    echo "$HOSTNAME,$TIME_SECONDS,$CPU" >> "$CPU_FILE"
    echo "$HOSTNAME,$TIME_SECONDS,$MEM" >> "$MEMORY_FILE"
}

while sleep 1; do
    recordUtilization "$2"
done
