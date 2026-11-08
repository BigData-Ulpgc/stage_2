#!/usr/bin/env python3
"""Turns one run of the indexing experiments (DR-13) into rows of the common format of plan §7.1.

run-experiments.sh calls it after each run and appends what it prints to summary.csv:

    python3 benchmarks/indexing/experiment_rows.py --experiment index_threads --threads 4 \\
        --size 2000 --repetition 1 --summary "2000,0,6.2,135.7,14.7,15.0" \\
        --timeline RUN/timeline.csv --started EPOCH [--monitor RUN/monitor] [--stats RUN/stats.csv]

    python3 benchmarks/indexing/experiment_rows.py --experiment index_publish_10 --threads 4 \\
        --size 10000 --repetition 3 --publish-seconds 0.084

Rows of a run:
  throughput           books/s between 10 % and 90 % of the books (steady: no start-up, no last batch)
  throughput_total     books/s over the whole run, start-up of the service included
  seconds_per_book     1 / throughput
  startup              s until the first book was indexed (JVM, Hazelcast, the first batch)
  duration             s until the last one
  failed               books that ended FAILED (must be 0)
  throughput_pct_A_B   books/s between A % and B % of the books, per tenth: does the rate fall as
                       the index grows?
  cpu_<service>        mean % of one CPU while indexing (between the first and the last book), from
                       scripts/monitor.sh; 200 = the 2 CPUs of a container with cpus: 2
  mem_max_<service>    the most memory it used, MiB
  index_* and posting_* (with --stats) what IndexingExperiment stats measured in MongoDB
A publication (--publish-seconds) gives one row, publish_latency. The probe of IndexingExperiment
(--probe) gives addtoset_latency and push_latency, with the length of the list as dataset_size.

The tenths use the same rule as Progress.rate in Java: a moment is the first sample with at least
that many books.
"""
import argparse
import csv
import datetime
import math
import pathlib
import sys

HEADER = ["experiment", "component", "scope", "deployment", "client_concurrency", "replicas",
          "threads", "dataset_size", "repetition", "metric", "value", "unit"]
SUMMARY = ["books", "failed", "first_book_s", "total_s", "books_per_s", "steady_books_per_s"]
# Container name fragment -> service in the metric name (docker compose names: <project>-indexing-1)
SERVICES = [("indexing", "indexing"), ("mongo", "mongo"), ("hz-", "hazelcast"), ("ingestion", "ingestion")]


def read_timeline(path):
    with open(path, newline="", encoding="utf-8") as handle:
        return [(float(r["seconds"]), int(r["books_indexed"])) for r in csv.DictReader(handle)]


def first_at_least(samples, books):
    for seconds, n in samples:
        if n >= books:
            return seconds, n
    return None


def rate(samples, total, start, end):
    """Books per second between the fractions start and end of total; None if not reached."""
    first = samples[0] if start == 0 and samples else first_at_least(samples, math.ceil(total * start))
    last = first_at_least(samples, math.ceil(total * end))
    if first is None or last is None or last[0] <= first[0]:
        return None
    return (last[1] - first[1]) / (last[0] - first[0])


def parse_time(text):
    return datetime.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp()


def resources(monitor_dir, start, end):
    """Mean CPU and maximum memory of each service between the epochs start and end."""
    files = sorted(pathlib.Path(monitor_dir).glob("*/containers.csv"))
    if not files:
        return {}
    cpu, mem = {}, {}
    with open(files[-1], newline="", encoding="utf-8") as handle:
        for r in csv.DictReader(handle):
            if not (start <= parse_time(r["timestamp"]) <= end):
                continue
            service = next((s for fragment, s in SERVICES if fragment in r["container"]), None)
            if service is None or not r["cpu_pct"]:
                continue
            # one sample per container: the two Hazelcast members add up within a timestamp
            cpu.setdefault(service, {}).setdefault(r["timestamp"], 0.0)
            cpu[service][r["timestamp"]] += float(r["cpu_pct"])
            if r["mem_used_mib"]:
                mem[service] = max(mem.get(service, 0.0), float(r["mem_used_mib"]))
    out = {}
    for service, samples in cpu.items():
        out["cpu_" + service] = (sum(samples.values()) / len(samples), "% of 1 CPU")
    for service, value in mem.items():
        out["mem_max_" + service] = (value, "MiB")
    return out


def stats_rows(path):
    """The figures of IndexingExperiment stats that go into the summary."""
    with open(path, newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    index = next((r["collection"] for r in rows if r["metric"] == "terms"), None)
    out = {}

    def value(metric, collection, term=""):
        for r in rows:
            if r["metric"] == metric and r["collection"] == collection and r["term"] == term:
                return float(r["value"])
        return 0.0

    if index is None:
        return out
    out["index_terms"] = (value("terms", index), "terms")
    out["index_postings"] = (value("postings", index), "ids")
    out["index_mean_postings"] = (value("mean_postings", index), "ids/term")
    out["index_disk"] = (value("storage_size", index) + value("index_size", index), "bytes")
    out["index_data"] = (value("data_size", index), "bytes")
    for collection in ("books", "index_log", "index_books"):
        out[collection + "_disk"] = (value("storage_size", collection) + value("index_size", collection), "bytes")
    longest = [r for r in rows if r["metric"] == "posting_length"]
    if longest:
        term = longest[0]["term"]
        out["posting_longest_length"] = (value("posting_length", index, term), "ids")
        out["posting_longest_bytes"] = (value("posting_bytes", index, term), "bytes")
        out["posting_longest_read_median"] = (value("read_median", index, term), "ms")
        out["posting_longest_read_max"] = (value("read_max", index, term), "ms")
    return out


def run_rows(args):
    values = args.summary.split(",")
    if len(values) != len(SUMMARY):
        raise SystemExit(f"--summary must have the {len(SUMMARY)} columns {','.join(SUMMARY)}")
    summary = dict(zip(SUMMARY, values))
    figures = {}

    def put(metric, text, unit):
        if text not in ("", None):
            figures[metric] = (float(text), unit)

    put("throughput", summary["steady_books_per_s"], "books/s")
    put("throughput_total", summary["books_per_s"], "books/s")
    if summary["steady_books_per_s"] and float(summary["steady_books_per_s"]) > 0:
        figures["seconds_per_book"] = (1 / float(summary["steady_books_per_s"]), "s/book")
    put("startup", summary["first_book_s"], "s")
    put("duration", summary["total_s"], "s")
    put("failed", summary["failed"], "books")
    samples = read_timeline(args.timeline)
    for tenth in range(10):
        r = rate(samples, args.size, tenth / 10, (tenth + 1) / 10)
        if r is not None:
            figures[f"throughput_pct_{tenth * 10:02d}_{(tenth + 1) * 10:03d}"] = (r, "books/s")
    if args.monitor:
        first = float(summary["first_book_s"] or 0)
        figures.update(resources(args.monitor, args.started + first, args.started + float(summary["total_s"])))
    if args.stats:
        figures.update(stats_rows(args.stats))
    return figures


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--threads", type=int, required=True, help="INDEXING_THREADS")
    parser.add_argument("--size", type=int, required=True, help="books of the run (dataset_size)")
    parser.add_argument("--repetition", type=int, required=True)
    parser.add_argument("--summary", help="the line IndexingExperiment measure printed")
    parser.add_argument("--timeline", help="the samples IndexingExperiment measure wrote")
    parser.add_argument("--started", type=float, default=0, help="epoch seconds at time 0 of the timeline")
    parser.add_argument("--monitor", help="the -o folder of scripts/monitor.sh for this run")
    parser.add_argument("--stats", help="the CSV of IndexingExperiment stats")
    parser.add_argument("--publish-seconds", type=float, help="the time of one POST /index/publish")
    parser.add_argument("--probe", help="the CSV of IndexingExperiment probe")
    parser.add_argument("--header", action="store_true", help="print the header first")
    args = parser.parse_args(argv)

    writer = csv.writer(sys.stdout, lineterminator="\n")
    if args.header:
        writer.writerow(HEADER)

    def write(size, figures):
        for metric, (value, unit) in figures.items():
            writer.writerow([args.experiment, "indexing", "integration", "docker", 1, 1, args.threads, size,
                             args.repetition, metric, f"{value:.6g}", unit])

    if args.probe:
        with open(args.probe, newline="", encoding="utf-8") as handle:
            for r in csv.DictReader(handle):
                write(int(r["list_length"]), {r["operation"].lower() + "_latency": (float(r["median_ms"]), "ms")})
    elif args.publish_seconds is not None:
        write(args.size, {"publish_latency": (args.publish_seconds * 1000, "ms")})
    elif args.summary and args.timeline:
        write(args.size, run_rows(args))
    else:
        parser.error("give --summary and --timeline, --publish-seconds or --probe")


if __name__ == "__main__":
    main()
