# Helpers shared by run-load.sh and run-mixed.sh. Sourced, not run.

# The number of books in one state, from GET /control/status ("PUBLISHED":12).
control_count() {
    curl -s -m 10 localhost:7000/control/status | grep -o "\"$1\":[0-9]*" | head -1 | cut -d: -f2
}

# Books the pipeline has finished: INDEXED (waiting for the next publication) or PUBLISHED.
control_done() {
    local status indexed published
    status="$(curl -s -m 10 localhost:7000/control/status)"
    indexed="$(echo "$status" | grep -o '"INDEXED":[0-9]*' | head -1 | cut -d: -f2)"
    published="$(echo "$status" | grep -o '"PUBLISHED":[0-9]*' | head -1 | cut -d: -f2)"
    echo $(( ${indexed:-0} + ${published:-0} ))
}

# Seeds the ids of a file through the Control (in requests of at most 10,000) and waits until
# that many books more are PUBLISHED. Books that end FAILED are reported and do not count.
# control_seed_and_wait FILE TIMEOUT_SECONDS: sets SEEDED, PUBLISHED_NOW and INDEXED_SECONDS (when
# every book was INDEXED or PUBLISHED: the work of the pipeline, without the wait for the next
# publication, which depends on PUBLISH_INTERVAL_S).
control_seed_and_wait() {
    local file="$1" timeout="$2" before ids chunk deadline published failed indexed start=$SECONDS
    before="$(control_done)"
    INDEXED_SECONDS=""
    ids="$(grep -E '^[0-9]+$' "$file" | tr -d '\r')"
    SEEDED="$(echo "$ids" | grep -c . || true)"
    echo "$ids" | split -l 10000 - "$OUT/.seed-"
    for chunk in "$OUT"/.seed-*; do
        local body
        body="{\"book_ids\":[$(paste -sd, "$chunk")]}"
        if ! curl -sf -m 120 -X POST localhost:7000/control/jobs -H 'Content-Type: application/json' -d "$body" > /dev/null; then
            rm -f "$OUT"/.seed-*
            echo "ERROR: POST /control/jobs failed" >&2
            return 1
        fi
    done
    rm -f "$OUT"/.seed-*
    deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        published="$(control_count PUBLISHED)"; failed="$(control_count FAILED)"
        PUBLISHED_NOW="${published:-0}"
        indexed="$(control_done)"
        if [ -z "$INDEXED_SECONDS" ] && [ $((indexed - before + ${failed:-0})) -ge "$SEEDED" ]; then
            INDEXED_SECONDS=$((SECONDS - start))
        fi
        if [ $((PUBLISHED_NOW - before + ${failed:-0})) -ge "$SEEDED" ]; then
            INDEXED_SECONDS="${INDEXED_SECONDS:-$((SECONDS - start))}"
            [ "${failed:-0}" -eq 0 ] || echo "WARNING: ${failed} books FAILED (GET /control/status)" >&2
            return 0
        fi
        sleep 2
    done
    echo "ERROR: after ${timeout} s only $((PUBLISHED_NOW - before)) more books were PUBLISHED, of $SEEDED" >&2
    return 1
}

# Empties a bind-mounted folder from a container: on Linux the services write as root, so a
# plain rm would need sudo.
wipe_folder() {
    local dir="$1" absolute
    mkdir -p "$dir"
    absolute="$(cd "$dir" && { pwd -W 2>/dev/null || pwd; })"
    MSYS_NO_PATHCONV=1 docker run --rm -v "$absolute:/wipe" --entrypoint sh "nginx:${NGINX_VERSION:-1.28-alpine}" \
        -c 'rm -rf /wipe/* /wipe/.[!.]* 2>/dev/null; true'
}
