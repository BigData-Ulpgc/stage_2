#!/usr/bin/env bash
#
# Records the resources of the system during an experiment (assignment: CPU and memory of each
# process; plan DP-09). Run it next to the experiment, with the stack already up:
#
#   bash scripts/monitor.sh -l search-3replicas-c64 -d 120     for 120 seconds
#   bash scripts/monitor.sh -l demo                            until Ctrl+C
#
# It writes in <output>/<label>_<UTC time>/:
#   containers.csv   one line per container and sample, from docker stats (new replicas included)
#   host.csv         load and memory of the Docker host, every second
#   meta.txt         date, CPUs and memory of the host, and the limits of each container
#
# The host figures are read inside a container because /proc/loadavg and /proc/meminfo are not
# per container: they describe the kernel that runs Docker. On Linux that is the machine; on
# Windows and macOS it is the Docker Desktop VM, the one that the experiment really loads.
#
# Requires bash and the Docker CLI. Under load the monitor itself uses a little CPU.

set -euo pipefail

label="run"
output="benchmarks/search/monitor"
duration=""
host_container="mongo"

usage() {
    cat <<EOF
usage: bash scripts/monitor.sh [-l label] [-o output_dir] [-d seconds] [-c container]
  -l  name of the experiment (default: run)
  -o  output folder (default: benchmarks/search/monitor)
  -d  stop after these seconds (default: until Ctrl+C)
  -c  running container used to read the host figures (default: mongo)
EOF
}

while getopts "l:o:d:c:h" option; do
    case "$option" in
        l) label="$OPTARG" ;;
        o) output="$OPTARG" ;;
        d) duration="$OPTARG" ;;
        c) host_container="$OPTARG" ;;
        h) usage; exit 0 ;;
        *) usage >&2; exit 2 ;;
    esac
done

if [[ -n "$duration" && ! "$duration" =~ ^[1-9][0-9]*$ ]]; then
    echo "-d must be a positive number of seconds, got '$duration'" >&2
    exit 2
fi
if ! docker inspect -f '{{.State.Running}}' "$host_container" 2>/dev/null | grep -q true; then
    echo "container '$host_container' is not running: start the stack, or choose another one with -c" >&2
    exit 1
fi

started=$(date -u +%Y%m%dT%H%M%SZ)
run_dir="$output/${label}_${started}"
mkdir -p "$run_dir"
containers_csv="$run_dir/containers.csv"
host_csv="$run_dir/host.csv"

{
    echo "label: $label"
    echo "started: $started"
    docker info --format 'host cpus: {{.NCPU}}, host memory bytes: {{.MemTotal}}, docker: {{.ServerVersion}}'
    echo
    echo "container limits (NanoCpus: 1000000000 = 1 CPU; Memory in bytes; 0 = no limit):"
    docker ps --format '{{.Names}}' | sort | while read -r name; do
        docker inspect -f "$name NanoCpus={{.HostConfig.NanoCpus}} Memory={{.HostConfig.Memory}}" "$name"
    done
} > "$run_dir/meta.txt"

# docker stats values such as "301.3MiB / 768MiB" and "1.5kB / 3MB" become plain numbers, so the CSV
# can be plotted directly. A value Docker does not know yet ("--") stays empty.
to_numbers='
function number(text, unit_base,    value, unit, power) {
    if (text !~ /^[0-9.]+/) return ""
    value = text; sub(/[A-Za-z]+$/, "", value)
    unit = text;  sub(/^[0-9.]+/, "", unit)
    power = -2
    if (unit ~ /^[kK]/) power = -1
    else if (unit ~ /^M/) power = 0
    else if (unit ~ /^G/) power = 1
    else if (unit ~ /^T/) power = 2
    return sprintf("%.3f", value * unit_base ^ power)
}
function percent(text) { gsub(/%/, "", text); return text ~ /^[0-9.]+$/ ? text : "" }
{
    gsub(/\r/, "")
    if ($0 == "") next
    split($0, field, ",")
    split(field[3], memory, " / ")
    split(field[5], net, " / ")
    split(field[6], block, " / ")
    print field[1] "," percent(field[2]) "," number(memory[1], 1024) "," number(memory[2], 1024) "," \
          percent(field[4]) "," number(net[1], 1000) "," number(net[2], 1000) "," \
          number(block[1], 1000) "," number(block[2], 1000) "," field[7]
    fflush()
}'

# One docker stats --no-stream per sample, not the continuous mode: on Windows the continuous
# process could not be stopped from the script, and it kept writing after the end. A sample takes
# Docker about a second or two, which sets the real period: compare the timestamps of the CSV.
#
# A sample can fail, for example when a replica disappears in the middle of it (the failure tests
# kill replicas on purpose). That sample is lost and the recording goes on: "|| sleep 1" keeps
# set -e from ending the loop, and avoids a tight loop if Docker stops answering.
record_containers() {
    echo "timestamp,container,cpu_pct,mem_used_mib,mem_limit_mib,mem_pct,net_rx_mb,net_tx_mb,block_read_mb,block_write_mb,pids" > "$containers_csv"
    while true; do
        now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
        docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}' 2>/dev/null \
            | awk "$to_numbers" | while IFS= read -r line; do
                echo "$now,$line"
            done || sleep 1
    done >> "$containers_csv"
}

record_host() {
    echo "timestamp,load1,load5,load15,mem_total_mib,mem_available_mib" > "$host_csv"
    while true; do
        if figures=$(docker exec "$host_container" sh -c 'cat /proc/loadavg; grep -E "^(MemTotal|MemAvailable):" /proc/meminfo' 2>/dev/null); then
            printf '%s,' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
            echo "$figures" | awk '
                NR == 1 { load = $1 "," $2 "," $3 }
                /^MemTotal/ { total = $2 / 1024 }
                /^MemAvailable/ { available = $2 / 1024 }
                END { printf "%s,%.0f,%.0f\n", load, total, available }'
        fi
        sleep 1
    done >> "$host_csv"
}

worker_pids=()
sleep_pid=""
stopped=false
stop() {
    if [[ "$stopped" == true ]]; then return; fi
    stopped=true
    kill ${worker_pids[@]+"${worker_pids[@]}"} $sleep_pid 2>/dev/null || true
    wait 2>/dev/null || true
    echo "containers: $(( $(wc -l < "$containers_csv") - 1 )) lines, host: $(( $(wc -l < "$host_csv") - 1 )) lines, in $run_dir"
}
trap stop EXIT
trap 'exit 0' INT TERM

record_containers &
worker_pids+=($!)
record_host &
worker_pids+=($!)

echo "recording in $run_dir$([[ -n "$duration" ]] && echo " for $duration s" || echo ", Ctrl+C to stop")"
# In the background, with wait: a signal stops the script at once, not when the sleep ends.
if [[ -n "$duration" ]]; then
    sleep "$duration" &
    sleep_pid=$!
    wait "$sleep_pid" || true
else
    while true; do
        sleep 1 &
        sleep_pid=$!
        wait "$sleep_pid" || true
    done
fi
