#!/usr/bin/env bash
#
# Functional validation of the APIs on the running stack (assignment §3.4), without a front-end.
# It shows each check as PASS, FAIL or SKIP and ends with exit code 1 if any check failed.
#
#   docker compose up -d --build
#   (load the sample dataset: see http/README.md)
#   bash scripts/validate-api.sh                    # keep the evidence: ... | tee validation.txt
#
# The expected results are those of the 15 books of the sample dataset. The public API is called
# through nginx and the internal ones inside the Docker network, as the services call each other.
# Run it right after loading the dataset. It does not change any data, except POST /index/publish
# once DR-08 exists: with nothing new to publish, it creates no version, which the script checks.
#
# SKIP means an endpoint declared in its contract that answers 501 (not implemented yet). When it
# is implemented, the check runs without touching this script. Any other answer is a PASS or a
# FAIL: a route that disappears must not go unnoticed. Add the checks of an endpoint with it.
#
# COMPOSE selects the command: by default "docker compose", or "docker-compose" if the first is
# not available.

set -uo pipefail
cd "$(dirname "$0")/.." || exit 2

base="${VALIDATE_URL:-http://localhost:8080}"
if [[ -z "${COMPOSE:-}" ]]; then
    if docker compose version >/dev/null 2>&1; then COMPOSE="docker compose"; else COMPOSE="docker-compose"; fi
fi
body_file="$(mktemp)"
trap 'rm -f "$body_file"' EXIT
passed=0; failed=0; skipped=0

report() {      # report PASS|FAIL|SKIP name detail
    case "$1" in
        PASS) passed=$((passed + 1)) ;;
        FAIL) failed=$((failed + 1)) ;;
        SKIP) skipped=$((skipped + 1)) ;;
    esac
    printf '%-5s %-58s %s\n' "$1" "$2" "$3"
}

# call METHOD PATH [SERVICE PORT]: leaves the body in body_file and the status in "code".
# Without SERVICE, through nginx from the host; with it, inside that container. REQUEST_BODY, if
# set, is sent as the body of the request.
call() {
    if [[ $# -eq 2 ]]; then
        code=$(curl -s -g -m 20 -o "$body_file" -w '%{http_code}' -X "$1" "$base$2")
    else
        local output
        local data=()
        if [[ -n "${REQUEST_BODY:-}" ]]; then data=(-d "$REQUEST_BODY"); fi
        output=$($COMPOSE exec -T "$3" curl -s -g -m 20 -w '\n%{http_code}' -X "$1" "${data[@]+"${data[@]}"}" "http://localhost:$4$2" 2>/dev/null | tr -d '\r')
        code="${output##*$'\n'}"
        code="${code:-000}"       # the container does not answer
        printf '%s' "${output%$'\n'*}" > "$body_file"
    fi
}

book_ids() { grep -o '"book_id":[0-9]*' "$body_file" | cut -d: -f2 | tr '\n' ' ' | sed 's/ $//'; }

# search NAME QUERY_STRING TOTAL "ID ID ...": a 200 with exactly this total and these ids, in order.
search() {
    call GET "/search?$2"
    local ids; ids="$(book_ids)"
    if [[ "$code" == 200 && "$(cat "$body_file")" == *"\"total\":$3,"* && "$ids" == "$4" ]]; then
        report PASS "$1" "total $3: ${ids:-no books}"
    else
        report FAIL "$1" "HTTP $code, ids '$ids', expected total $3: '$4'"
    fi
}

# expect NAME STATUS ERROR_CODE_OR_TEXT METHOD PATH [SERVICE PORT]: status and a text in the body.
expect() {
    local name="$1" status="$2" text="$3"; shift 3
    call "$@"
    if [[ "$code" == "$status" && "$(cat "$body_file")" == *"$text"* ]]; then
        report PASS "$name" "HTTP $code $text"
    else
        report FAIL "$name" "HTTP $code, expected $status '$text': $(head -c 150 "$body_file")"
    fi
}

# expect_error NAME STATUS CODE FIELD METHOD PATH [SERVICE PORT]: the common error format, with this
# code and field (null: none). The field tells a missing book from a route that does not exist,
# which is also a 404 NOT_FOUND.
expect_error() {
    local name="$1" status="$2" error_code="$3" field="$4" wanted; shift 4
    call "$@"
    if [[ "$field" == null ]]; then wanted='"field":null'; else wanted="\"field\":\"$field\""; fi
    if [[ "$code" == "$status" && "$(cat "$body_file")" == *"\"code\":\"$error_code\""* && "$(cat "$body_file")" == *"$wanted"* ]]; then
        report PASS "$name" "HTTP $code $error_code $wanted"
    else
        report FAIL "$name" "HTTP $code, expected $status $error_code $wanted: $(head -c 150 "$body_file")"
    fi
}

# pending: like expect, but SKIP if the endpoint is declared and not implemented yet (501).
pending() {
    local name="$1" status="$2" text="$3"; shift 3
    call "$@"
    if [[ "$code" == 501 ]]; then
        report SKIP "$name" "not implemented yet (HTTP $code)"
    elif [[ "$code" == "$status" && "$(cat "$body_file")" == *"$text"* ]]; then
        report PASS "$name" "HTTP $code $text"
    else
        report FAIL "$name" "HTTP $code, expected $status '$text': $(head -c 150 "$body_file")"
    fi
}

echo "== Before starting"
call GET /status
if [[ "$code" != 200 ]]; then
    echo "The public API does not answer at $base (HTTP $code): start the stack with docker compose up -d" >&2
    exit 2
fi
call GET /search/filters
if [[ "$code" != 200 || "$(cat "$body_file")" != *'"books":15,'* ]]; then
    echo "The published index is not the 15 books of the sample dataset (HTTP $code): see http/README.md" >&2
    exit 2
fi
report PASS "the sample dataset is published" "15 books"

echo; echo "== Search: keywords"
search "one term"                         "q=frankenstein"               1  "84"
search "another term, ordered by book_id" "q=whale"                      4  "76 84 345 2701"
search "several terms are AND"            "q=whale+monster"              3  "84 345 2701"
search "upper case is the same term"      "q=FRANKENSTEIN"               1  "84"
search "a stopword in q is ignored"       "q=the+frankenstein"           1  "84"

echo; echo "== Search: filters, one by one and combined"
search "author, part of the name"         "q=river&author=twain"         2  "74 76"
search "author, any case"                 "q=river&author=TWAIN"         2  "74 76"
search "year"                             "q=river&year=2004"            3  "46 74 76"
search "language"                         "q=frankenstein&language=en"   1  "84"
search "author + year"                    "q=river&author=dickens&year=1994"  1  "98"
search "author + year + language"         "q=river&author=dickens&year=1994&language=en"  1  "98"

echo; echo "== Search: empty results are a 200"
search "a word in no book"                "q=xyzzyqqq"                   0  ""
search "a term in no book makes the AND empty" "q=whale+xyzzyqqq"        0  ""
search "a filter nobody matches"          "q=whale&author=nobody"        0  ""
search "a supported language no book has" "q=whale&language=fr"          0  ""

echo; echo "== Search: pagination"
search "first page"                       "q=river&limit=3"              10 "46 74 76"
search "second page"                      "q=river&limit=3&offset=3"     10 "84 98 174"
search "last page, partial"               "q=river&limit=3&offset=9"     10 "2701"
search "offset beyond the end"            "q=river&offset=10"            10 ""
search "default limit keeps every match"  "q=river"                      10 "46 74 76 84 98 174 345 1342 1661 2701"

echo; echo "== Search: invalid parameters are a 400 and touch nothing"
expect_error "q missing"                     400 MISSING_PARAMETER q        GET "/search"
expect_error "q with only stopwords"         400 INVALID_PARAMETER q        GET "/search?q=the"
expect_error "q is not valid percent-encoding" 400 INVALID_PARAMETER null   GET "/search?q=%ZZ"
expect_error "language not supported"        400 INVALID_PARAMETER language GET "/search?q=whale&language=xx"
expect_error "year is not 4 digits"          400 INVALID_PARAMETER year     GET "/search?q=whale&year=99"
expect_error "offset negative"               400 INVALID_PARAMETER offset   GET "/search?q=whale&offset=-1"
expect_error "limit 0"                       400 INVALID_PARAMETER limit    GET "/search?q=whale&limit=0"
expect_error "limit above the maximum"       400 INVALID_PARAMETER limit    GET "/search?q=whale&limit=101"

echo; echo "== Search: a book and the filter values"
expect       "book published"                200 '"title":"Pride and Prejudice"' GET "/books/1342"
expect_error "book not in the index"         404 NOT_FOUND book_id          GET "/books/999999"
expect_error "book_id 0"                     400 INVALID_PARAMETER book_id  GET "/books/0"
expect_error "book_id with a sign"           400 INVALID_PARAMETER book_id  GET "/books/+84"
expect_error "book_id with leading zeros"    400 INVALID_PARAMETER book_id  GET "/books/0084"
expect_error "book_id not a number"          400 INVALID_PARAMETER book_id  GET "/books/abc"
expect       "languages and years"           200 '"years":{"min":1975,"max":2008}' GET "/search/filters"
expect_error "a route that does not exist"   404 NOT_FOUND null             GET "/nope"
expect       "status of the service"         200 '"service":"search-service"' GET "/status"

echo; echo "== Indexing (internal)"
expect       "status"                        200 '"service":"indexing-service"' GET /status indexing 7002
expect       "index status: books published" 200 '"books_published":15,'   GET /index/status indexing 7002
expect       "index status: books indexed"   200 '"books_indexed":15,'     GET /index/status indexing 7002
expect_error "update: invalid id"            400 INVALID_PARAMETER book_id POST /index/update/abc indexing 7002
expect_error "update: a book not in the datalake" 404 NOT_FOUND book_id    POST /index/update/999999 indexing 7002
pending      "publish with nothing new"      200 '"books_published":0,'    POST /index/publish indexing 7002
expect       "publish created no new version" 200 '"published_version":1,' GET /index/status indexing 7002

echo; echo "== Ingestion (internal)"
expect       "status"                        200 '"service":"ingestion-service"' GET /status ingestion 7001
expect_error "ingest: invalid id"            400 INVALID_PARAMETER book_id POST /ingest/0084 ingestion 7001
expect_error "status: invalid id"            400 INVALID_PARAMETER book_id GET /ingest/status/0084 ingestion 7001
expect       "status of a book not stored"   200 '"status":"not_available"' GET /ingest/status/999999 ingestion 7001
expect       "list of stored books"          200 '"books":['               GET /ingest/list ingestion 7001
expect_error "GET where only POST exists"    404 NOT_FOUND null            GET /ingest/1342 ingestion 7001

echo; echo "== Control Module"
expect       "status"                        200 '"service":"control-module"' GET /status control 7000
expect       "the Control reaches Hazelcast" 200 '"coordination":"connected"' GET /status control 7000
expect       "status of the books and queues" 200 '"ingest-queue":'           GET /control/status control 7000
expect_error "task: invalid id"              400 INVALID_PARAMETER book_id    GET /control/tasks/0084 control 7000
expect_error "task: a book never submitted"  404 NOT_FOUND book_id            GET /control/tasks/999999 control 7000
REQUEST_BODY='{"book_ids":[0]}' \
expect_error "jobs: an invalid id rejects all" 400 INVALID_PARAMETER book_ids POST /control/jobs control 7000
REQUEST_BODY='{"book_ids":[]}' \
expect_error "jobs: an empty list"           400 INVALID_PARAMETER book_ids   POST /control/jobs control 7000
REQUEST_BODY='not json' \
expect_error "jobs: a body that is not JSON" 400 INVALID_PARAMETER body       POST /control/jobs control 7000
expect       "retry-failed"                  200 '"requeued":'                POST /control/retry-failed control 7000

echo; echo "== Interactions that are not HTTP"
health=$($COMPOSE exec -T search curl -s -m 10 http://hz-1:5701/hazelcast/health 2>/dev/null | tr -d '\r')
if [[ "$health" == *'"clusterSize":2}'* ]]; then
    report PASS "Hazelcast: the 2 members form one cluster" "clusterSize 2"
else
    report FAIL "Hazelcast: the 2 members form one cluster" "$health"
fi
call GET /status
search_version=$(grep -o '"index_version":[0-9a-z]*' "$body_file" | cut -d: -f2)
call GET /index/status indexing 7002
indexing_version=$(grep -o '"published_version":[0-9a-z]*' "$body_file" | cut -d: -f2)
if [[ -n "$search_version" && "$search_version" == "$indexing_version" ]]; then
    report PASS "MongoDB: Search serves the version of CURRENT" "version $search_version"
else
    report FAIL "MongoDB: Search serves the version of CURRENT" "Search '$search_version', Indexing '$indexing_version'"
fi
# PASS only if the container answers and the write fails BECAUSE the file system is read-only.
for service in indexing control; do
    name="datalake: $service cannot write"
    if ! $COMPOSE exec -T "$service" true >/dev/null 2>&1; then
        report FAIL "$name" "the container does not answer"
    elif output=$($COMPOSE exec -T "$service" sh -c 'touch /data/datalake/.validate' 2>&1); then
        $COMPOSE exec -T "$service" rm -f /data/datalake/.validate >/dev/null 2>&1
        report FAIL "$name" "it could write"
    elif [[ "$output" == *"Read-only file system"* ]]; then
        report PASS "$name" "read-only"
    else
        report FAIL "$name" "unexpected error: $output"
    fi
done

echo
echo "passed $passed, failed $failed, skipped $skipped"
[[ "$failed" == 0 ]]
