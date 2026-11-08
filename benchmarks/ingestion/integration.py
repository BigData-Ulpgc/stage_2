#!/usr/bin/env python3
"""One point of the integration benchmarks of Ingestion (CF-13), on a system that is already up.

run-integration.sh starts each configuration (replicas, workers, dataset) with Docker Compose
and calls this once per point. It:

  1. waits until the Control and every Ingestion replica are connected to Hazelcast, and checks
     that book-state is empty (a fresh cluster);
  2. seeds the ids through POST /control/jobs and polls GET /control/status every 0.2 s;
  3. stops when every book is past ingestion (--target ingested) or PUBLISHED (--target published);
  4. reads GET /status of every Ingestion replica (docker exec, they have no host port) and
     checks that every book was downloaded exactly once;
  5. appends the rows to the common CSV of plan §7.1 and keeps the raw statuses and the timeline.

Only the Python standard library. Exit code 1 if the point is not valid (timeout, a FAILED book,
a duplicate download): the numbers of an incorrect run are not results (enunciado §6.2).
"""
import argparse
import csv
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

PAST_INGESTION = ("DOWNLOADED", "INDEXING", "INDEXED", "PUBLISHED")
SEED_CHUNK = 10_000
POLL_SECONDS = 0.2
COST_METRICS = ("queue_wait", "claim", "work", "complete")
CSV_HEADER = ["experiment", "component", "scope", "deployment", "client_concurrency", "replicas",
              "threads", "dataset_size", "repetition", "metric", "value", "unit"]


def get_json(url, timeout=10):
    with urllib.request.urlopen(url, timeout=timeout) as answer:
        return json.load(answer)


def post_json(url, body, timeout=120):
    request = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as answer:
        return json.load(answer)


def read_ids(path):
    with open(path, encoding="utf-8") as lines:
        return [int(line) for line in (raw.strip() for raw in lines) if line.isdigit()]


def replica_containers(compose, service):
    out = subprocess.run(compose.split() + ["ps", "-q", service], check=True,
                         capture_output=True, text=True).stdout
    return sorted(line for line in out.split() if line)


def replica_status(container, port):
    out = subprocess.run(["docker", "exec", container, "curl", "-s", "-m", "10",
                          f"http://localhost:{port}/status"], check=True, capture_output=True, text=True).stdout
    return json.loads(out)


def wait_until_ready(args, replicas, timeout=180):
    """Every replica connected with its workers, the Control connected, book-state empty."""
    deadline = time.monotonic() + timeout
    problem = "nothing checked yet"
    while time.monotonic() < deadline:
        try:
            control = get_json(f"{args.control}/control/status")
            containers = replica_containers(args.compose, args.service)
            statuses = [replica_status(c, args.port) for c in containers]
            connected = [s for s in statuses if s.get("coordination") == "connected" and s.get("workers")]
            if control.get("coordination") != "connected":
                problem = f"the Control is {control.get('coordination')}"
            elif len(containers) != replicas or len(connected) != replicas:
                problem = f"{len(connected)} of {replicas} replicas connected ({len(containers)} running)"
            elif control["books"]["total"] != 0:
                raise SystemExit(f"ERROR: book-state already has {control['books']['total']} books: "
                                 "the cluster was not emptied before this point")
            else:
                return containers
        except (urllib.error.URLError, OSError, subprocess.CalledProcessError, ValueError, KeyError) as e:
            problem = str(e)
        time.sleep(1)
    raise SystemExit(f"ERROR: the system was not ready after {timeout} s: {problem}")


def seed(args, ids):
    added = 0
    for start in range(0, len(ids), SEED_CHUNK):
        added += post_json(f"{args.control}/control/jobs", {"book_ids": ids[start:start + SEED_CHUNK]})["added"]
    if added != len(ids):
        raise SystemExit(f"ERROR: the Control added {added} of {len(ids)} ids")


def follow(args, total, began, timeline):
    """Polls the Control until the target; returns the seconds to each milestone."""
    reached = {}
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        try:
            books = get_json(f"{args.control}/control/status")["books"]
        except (urllib.error.URLError, OSError, ValueError, KeyError):
            time.sleep(POLL_SECONDS)                    # a slow answer under load: try again
            continue
        elapsed = time.monotonic() - began
        failed = books.get("FAILED", 0)
        ingested = sum(books.get(state, 0) for state in PAST_INGESTION)
        indexed = books.get("INDEXED", 0) + books.get("PUBLISHED", 0)
        published = books.get("PUBLISHED", 0)
        timeline.writerow([f"{elapsed:.3f}", ingested, indexed, published, failed])
        for name, done in (("ingested", ingested), ("indexed", indexed), ("published", published)):
            if name not in reached and done + failed >= total:
                reached[name] = elapsed
        if failed:
            raise SystemExit(f"ERROR: {failed} books FAILED (see GET /control/tasks/<id>)")
        if args.target in reached:
            return reached
        time.sleep(POLL_SECONDS)
    raise SystemExit(f"ERROR: after {args.timeout} s the target '{args.target}' was not reached")


def merge_costs(statuses):
    """The coordination_cost of the replicas together: exact count and mean; for p95 and p99 the
    worst replica (an upper bound: percentiles of different processes cannot be merged)."""
    merged = {}
    for metric in COST_METRICS:
        parts = [s["workers"]["coordination_cost"][metric] for s in statuses
                 if "coordination_cost" in s.get("workers", {})]
        count = sum(p["count"] for p in parts)
        merged[metric] = {
            "count": count,
            "mean_ms": sum(p["mean_ms"] * p["count"] for p in parts) / count if count else 0.0,
            "p95_ms": max((p["p95_ms"] for p in parts), default=0.0),
            "p99_ms": max((p["p99_ms"] for p in parts), default=0.0),
            "max_ms": max((p["max_ms"] for p in parts), default=0.0),
        }
    return merged if all(m["count"] for m in merged.values()) else None


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--ids", required=True, help="file with one book id per line (book_ids.txt of the dataset)")
    parser.add_argument("--target", choices=("ingested", "published"), default="ingested")
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--replicas", type=int, required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--repetition", type=int, required=True)
    parser.add_argument("--out", required=True, help="results folder: summary.csv, status/, timeline/")
    parser.add_argument("--control", default="http://localhost:7000")
    parser.add_argument("--compose", default="docker compose")
    parser.add_argument("--service", default="ingestion")
    parser.add_argument("--port", type=int, default=7001)
    parser.add_argument("--timeout", type=int, default=3600)
    args = parser.parse_args()

    ids = read_ids(args.ids)
    if not ids:
        raise SystemExit(f"ERROR: no book id in {args.ids}")
    label = f"{args.experiment}-n{len(ids)}-r{args.replicas}-w{args.workers}-rep{args.repetition}"
    for folder in ("status", "timeline"):
        os.makedirs(os.path.join(args.out, folder), exist_ok=True)

    containers = wait_until_ready(args, args.replicas)
    with open(os.path.join(args.out, "timeline", label + ".csv"), "w", newline="") as file:
        timeline = csv.writer(file)
        timeline.writerow(["elapsed_s", "ingested", "indexed_or_published", "published", "failed"])
        began = time.monotonic()
        seed(args, ids)
        reached = follow(args, len(ids), began, timeline)

    statuses = [replica_status(c, args.port) for c in containers]
    for i, status in enumerate(statuses, start=1):
        with open(os.path.join(args.out, "status", f"{label}-replica{i}.json"), "w") as file:
            json.dump(status, file, indent=2)
    downloaded = sum(s["workers"]["downloaded"] for s in statuses)
    already = sum(s["workers"]["already_available"] for s in statuses)
    duplicates = downloaded + already - len(ids)

    rows = []

    def row(metric, value, unit):
        rows.append([args.experiment, "ingestion", "integration", "docker", 1, args.replicas, args.workers,
                     len(ids), args.repetition, metric, value, unit])

    seconds = reached["ingested"]
    row("ingest_time", round(seconds, 3), "s")
    row("throughput", round(len(ids) / seconds, 2), "books/s")
    row("duplicate_downloads", duplicates, "books")
    if args.target == "published":
        row("time_to_indexed", round(reached["indexed"], 3), "s")
        row("time_to_published", round(reached["published"], 3), "s")
        row("pipeline_throughput", round(len(ids) / reached["indexed"], 2), "books/s")
    costs = merge_costs(statuses)
    if costs:
        for metric in COST_METRICS:
            for stat in ("mean", "p95", "p99"):
                row(f"{metric}_{stat}", round(costs[metric][stat + "_ms"], 3), "ms")
        coordination = costs["claim"]["mean_ms"] + costs["complete"]["mean_ms"]
        row("coordination_per_book", round(coordination, 3), "ms")
        row("coordination_share", round(100 * coordination / (coordination + costs["work"]["mean_ms"]), 2), "%")
    else:
        print("WARNING: the replicas show no coordination_cost (a build without CF-12)", file=sys.stderr)

    summary = os.path.join(args.out, "summary.csv")
    new = not os.path.exists(summary)
    with open(summary, "a", newline="") as file:
        writer = csv.writer(file)
        if new:
            writer.writerow(CSV_HEADER)
        writer.writerows(rows)

    per_book = f", coordination {coordination:.2f} ms/book" if costs else ""
    print(f"{label}: {len(ids) / seconds:.1f} books/s ({seconds:.1f} s){per_book}, duplicates {duplicates}")
    if duplicates != 0:
        raise SystemExit(f"ERROR: {duplicates} downloads more than books: a book was downloaded twice")


if __name__ == "__main__":
    main()
