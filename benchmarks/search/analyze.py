#!/usr/bin/env python3
"""Turns the results of run-jmh.sh and run-load.sh into what the report needs.

    python benchmarks/search/analyze.py [--load DIR ...] [--jmh DIR] [--out DIR]

By default it reads results/jmh and results/load* and writes to results/analysis:

    common.csv    every measure in the common format of plan section 7.1
    tables.md     means and standard deviations of the repetitions, ready for the report
    *.png         the figures (needs matplotlib; without it the rest is still written)

It reports the numbers and flags problems (errors, wrong results, too few repetitions). Saying why
the system saturates is left to the person who reads them: it does not interpret anything.
"""
import argparse
import csv
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
COMMON_HEADER = ["experiment", "component", "scope", "deployment", "client_concurrency", "replicas",
                 "threads", "dataset_size", "repetition", "metric", "value", "unit"]
LOAD_METRICS = [("throughput_rps", "throughput", "req/s"), ("mean_ms", "latency_mean", "ms"),
                ("p50_ms", "latency_p50", "ms"), ("p95_ms", "latency_p95", "ms"),
                ("p99_ms", "latency_p99", "ms")]

# Fixed categorical order. Each series
# also has its own marker, so a figure printed in grey can still be read.
COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
MARKERS = ["o", "s", "^", "D"]
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def mean_sd(values):
    values = list(values)
    return statistics.fmean(values), statistics.stdev(values) if len(values) > 1 else 0.0


def dataset_size(run_dir):
    """The number of books that run-load.sh wrote in host.txt."""
    host = run_dir / "host.txt"
    match = re.search(r"^dataset:.*\((\d+) books\)", host.read_text(encoding="utf-8"), re.M) if host.exists() else None
    return int(match.group(1)) if match else 0


def load_runs(directories):
    rows = []
    for run_dir in directories:
        size = dataset_size(run_dir)
        for row in read_csv(run_dir / "summary.csv"):
            row = dict(row)
            row["dataset_size"] = size
            row["run_dir"] = run_dir
            for key in ("replicas", "http_threads", "mongo_pool", "clients", "repetition",
                        "http_errors", "wrong_results", "connection_errors"):
                row[key] = int(row[key])
            for key in ("throughput_rps", "mean_ms", "p50_ms", "p95_ms", "p99_ms"):
                row[key] = float(row[key])
            rows.append(row)
    return rows


def jmh_rows(directory):
    rows = []
    if directory is None or not directory.exists():
        return rows
    for path in sorted(directory.glob("*.csv")):
        for row in read_csv(path):
            params = {k[len("Param: "):]: v for k, v in row.items() if k.startswith("Param: ")}
            rows.append({"benchmark": path.stem, "threads": int(row["Threads"]), "samples": row["Samples"],
                         "score": float(row["Score"]), "error": row["Score Error (99.9%)"],
                         "unit": row["Unit"], "params": params})
    return rows


def write_common(load, jmh, path):
    with open(path, "w", newline="", encoding="utf-8") as handle:
        out = csv.writer(handle, lineterminator="\n")
        out.writerow(COMMON_HEADER)
        for row in load:
            for key, metric, unit in LOAD_METRICS:
                out.writerow(["search_load_" + row["experiment"], "search", "integration", "docker", row["clients"],
                              row["replicas"], row["http_threads"], row["dataset_size"], row["repetition"],
                              metric, row[key], unit])
        for row in jmh:
            params = row["params"]
            detail = "_".join(f"{k}{v}" for k, v in sorted(params.items()) if k != "books")
            name = "search_jmh_" + row["benchmark"] + ("_" + detail if detail else "")
            out.writerow([name, "search", "jmh", "host", 1, 1, row["threads"], params.get("books", ""), 1,
                          "time_per_op", row["score"], row["unit"]])


def grouped(rows, *keys):
    groups = defaultdict(list)
    for row in rows:
        groups[tuple(row[k] for k in keys)].append(row)
    return sorted(groups.items(), key=lambda item: tuple(str(type(v)) + f"{v:>12}" for v in item[0]))


def load_table(rows, *keys):
    head = [k.replace("_", " ") for k in keys] + ["reps", "throughput (req/s)", "p50 (ms)", "p95 (ms)", "p99 (ms)"]
    lines = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    for key, group in grouped(rows, *keys):
        cells = [str(v) for v in key] + [str(len(group))]
        for metric, digits in (("throughput_rps", 0), ("p50_ms", 1), ("p95_ms", 1), ("p99_ms", 1)):
            mean, sd = mean_sd(r[metric] for r in group)
            cells.append(f"{mean:,.{digits}f} ± {sd:,.{digits}f}")
        lines.append("| " + " | ".join(cells) + " |")
    return lines


def monitor_summary(run_dirs):
    """Mean CPU and peak memory of each kind of container, per configuration."""
    totals = defaultdict(lambda: defaultdict(list))
    memory = defaultdict(lambda: defaultdict(float))
    for run_dir in run_dirs:
        for path in sorted((run_dir / "monitor").glob("*/containers.csv")):
            label = path.parent.name.split("_")[0]
            match = re.match(r"(.+)-r(\d+)-t(\d+)-p(\d+)-c(\d+)-rep(\d+)", label)
            if not match:
                continue
            experiment, replicas, _, _, clients, repetition = match.groups()
            per_sample = defaultdict(lambda: defaultdict(float))
            for row in read_csv(path):
                kind = re.sub(r"^[^-]*-|-\d+$", "", row["container"]) if row["container"].count("-") >= 2 else row["container"]
                per_sample[row["timestamp"]][kind] += float(row["cpu_pct"])
                key = (experiment, int(replicas), int(clients))
                memory[key][kind] = max(memory[key][kind], float(row["mem_used_mib"]))
            key = (experiment, int(replicas), int(clients))
            for sample in per_sample.values():
                for kind, cpu in sample.items():
                    totals[key][kind].append(cpu)
    return {key: {kind: (statistics.fmean(cpu), memory[key][kind]) for kind, cpu in kinds.items()}
            for key, kinds in totals.items()}


def write_tables(load, jmh, monitor, warnings, path):
    out = ["# Search benchmarks: tables", "",
           "Generated by benchmarks/search/analyze.py. Each cell is the mean ± standard deviation of the repetitions."]
    if warnings:
        out += ["", "## Warnings", ""] + [f"- {w}" for w in warnings]
    for size in sorted({r["dataset_size"] for r in load}):
        subset = [r for r in load if r["dataset_size"] == size]
        out += ["", f"## Load tests, index of {size} books"]
        for experiment, keys, title in (
                ("clients", ("replicas", "clients"), "Clients and replicas (250 Jetty threads, pool of 50, nginx with 2 CPUs)"),
                ("threads", ("http_threads", "clients"), "Jetty threads (1 replica, pool of 50)"),
                ("pool", ("mongo_pool", "clients"), "MongoDB connections (1 replica, 250 Jetty threads)")):
            rows = [r for r in subset if r["experiment"] == experiment]
            if rows:
                out += ["", f"### {title}", ""] + load_table(rows, *keys)
        rows = [r for r in subset if r["experiment"].startswith("nginx")]
        if rows:
            out += ["", "### nginx with the CPU limit of the deployment (1 replica)", ""] + load_table(rows, "experiment", "clients")
    if monitor:
        out += ["", "## Mean CPU (% of one core, all replicas added) and peak memory (MiB)", "",
                "| experiment | replicas | clients | container | CPU % | memory MiB |", "|---|---|---|---|---|---|"]
        for key in sorted(monitor):
            for kind in sorted(monitor[key]):
                cpu, mem = monitor[key][kind]
                out.append(f"| {key[0]} | {key[1]} | {key[2]} | {kind} | {cpu:.0f} | {mem:.0f} |")
    if jmh:
        out += ["", "## JMH: mean time per search (one row per configuration)", ""]
        for name in sorted({r["benchmark"] for r in jmh}):
            rows = [r for r in jmh if r["benchmark"] == name]
            names = sorted({k for r in rows for k in r["params"]})
            out += [f"### {name}", "", "| threads | " + " | ".join(names) + " | score | ± 99.9 % | unit |",
                    "|---|" + "---|" * (len(names) + 3)]
            for r in rows:
                params = " | ".join(r["params"].get(n, "") for n in names)
                out.append(f"| {r['threads']} | {params} | {r['score']:,.2f} | {r['error']} | {r['unit']} |")
            out.append("")
    path.write_text("\n".join(out) + "\n", encoding="utf-8")


def style(ax, title, xlabel, ylabel, logx=True):
    ax.set_title(title, loc="left", fontsize=11, color=INK)
    ax.set_xlabel(xlabel, color=MUTED)
    ax.set_ylabel(ylabel, color=MUTED)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=MUTED, length=0)
    ax.set_ylim(bottom=0)
    if logx:
        ax.set_xscale("log", base=2)


def line_figure(plt, rows, series_key, series_name, metric, ylabel, title, path):
    fig, ax = plt.subplots(figsize=(6.4, 3.8), dpi=200)
    levels = sorted({r[series_key] for r in rows}, key=str)
    for index, level in enumerate(levels):
        points = [(k[0], g) for k, g in grouped([r for r in rows if r[series_key] == level], "clients")]
        xs = [x for x, _ in points]
        stats = [mean_sd(r[metric] for r in g) for _, g in points]
        ax.errorbar(xs, [m for m, _ in stats], yerr=[s for _, s in stats], color=COLORS[index % 4],
                    marker=MARKERS[index % 4], markersize=6, linewidth=2, capsize=3,
                    label=f"{series_name} {level}")
    style(ax, title, "client concurrency (closed loop)", ylabel)
    xs = sorted({r["clients"] for r in rows})
    ax.set_xticks(xs, [str(x) for x in xs])
    ax.minorticks_off()
    if len(levels) > 1:
        ax.legend(frameon=False, labelcolor=MUTED)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def cpu_figure(plt, monitor, path):
    keys = sorted(k for k in monitor if k[0] == "clients")
    if not keys:
        return
    top = max(k[2] for k in keys)
    keys = [k for k in keys if k[2] == top]
    kinds = ["search", "mongo", "nginx"]
    fig, ax = plt.subplots(figsize=(6.4, 3.8), dpi=200)
    width = 0.8 / len(kinds)
    for index, kind in enumerate(kinds):
        values = [monitor[k].get(kind, (0, 0))[0] for k in keys]
        positions = [i + index * width for i in range(len(keys))]
        ax.bar(positions, values, width=width - 0.02, color=COLORS[index], label=kind)
    style(ax, f"Mean CPU with {top} clients", "Search replicas", "CPU (% of one core, replicas added)", logx=False)
    ax.set_xticks([i + width * (len(kinds) - 1) / 2 for i in range(len(keys))], [str(k[1]) for k in keys])
    ax.legend(frameon=False, labelcolor=MUTED)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def figures(load, jmh, monitor, out):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib is not installed: tables and common.csv only", file=sys.stderr)
        return
    for size in sorted({r["dataset_size"] for r in load}):
        subset = [r for r in load if r["dataset_size"] == size]
        tag = f"{size}books"
        rows = [r for r in subset if r["experiment"] == "clients"]
        if rows:
            line_figure(plt, rows, "replicas", "replicas", "throughput_rps", "throughput (req/s)",
                        f"Throughput, index of {size} books", out / f"throughput_replicas_{tag}.png")
            line_figure(plt, rows, "replicas", "replicas", "p95_ms", "p95 latency (ms)",
                        f"p95 latency, index of {size} books", out / f"p95_replicas_{tag}.png")
        base = [r for r in rows if r["replicas"] == 1]
        rows = base + [r for r in subset if r["experiment"] == "threads"]
        if len({r["http_threads"] for r in rows}) > 1:
            line_figure(plt, rows, "http_threads", "Jetty threads", "throughput_rps", "throughput (req/s)",
                        f"Jetty threads, 1 replica, {size} books", out / f"throughput_threads_{tag}.png")
            line_figure(plt, rows, "http_threads", "Jetty threads", "p95_ms", "p95 latency (ms)",
                        f"Jetty threads, 1 replica, {size} books", out / f"p95_threads_{tag}.png")
        rows = [r for r in base if r["clients"] in {x["clients"] for x in subset if x["experiment"] == "pool"}]
        rows += [r for r in subset if r["experiment"] == "pool"]
        if len({r["mongo_pool"] for r in rows}) > 1:
            line_figure(plt, rows, "mongo_pool", "pool", "p95_ms", "p95 latency (ms)",
                        f"MongoDB connections, 1 replica, {size} books", out / f"p95_pool_{tag}.png")
    cpu_figure(plt, monitor, out / "cpu_containers.png")
    pagination = [r for r in jmh if r["benchmark"] == "PaginationBenchmark"]
    if pagination:
        fig, ax = plt.subplots(figsize=(6.4, 3.8), dpi=200)
        books = sorted({r["params"]["books"] for r in pagination}, key=int)
        for index, size in enumerate(books):
            rows = sorted([r for r in pagination if r["params"]["books"] == size and r["params"].get("limit") == "20"],
                          key=lambda r: int(r["params"]["position"]))
            ax.plot([int(r["params"]["position"]) for r in rows], [r["score"] for r in rows], color=COLORS[index % 4],
                    marker=MARKERS[index % 4], linewidth=2, label=f"{int(size):,} books")
        style(ax, "JMH: cost of the position of the page (limit 20)", "position of the page (%)", "µs per search", logx=False)
        ax.legend(frameon=False, labelcolor=MUTED)
        fig.tight_layout()
        fig.savefig(out / "jmh_pagination.png")
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--load", type=Path, nargs="*", help="directories of run-load.sh (default: results/load*)")
    parser.add_argument("--jmh", type=Path, default=HERE / "results" / "jmh")
    parser.add_argument("--out", type=Path, default=HERE / "results" / "analysis")
    args = parser.parse_args()

    directories = args.load
    if directories is None:
        directories = [d for d in sorted((HERE / "results").glob("load*")) if d.name != "load-smoke"]
    directories = [d for d in directories if (d / "summary.csv").exists()]
    load = load_runs(directories)
    jmh = jmh_rows(args.jmh)
    if not load and not jmh:
        sys.exit("No results found: run run-jmh.sh or run-load.sh first, or pass --load and --jmh.")

    warnings = []
    for row in load:
        problems = row["http_errors"] + row["wrong_results"] + row["connection_errors"]
        if problems:
            warnings.append(f"{row['experiment']} r{row['replicas']} c{row['clients']} rep{row['repetition']}: "
                            f"{row['http_errors']} HTTP errors, {row['wrong_results']} wrong results, "
                            f"{row['connection_errors']} connection errors")
    for key, group in grouped(load, "experiment", "dataset_size", "replicas", "http_threads", "mongo_pool", "clients"):
        if len(group) < 3:
            warnings.append(f"fewer than 3 repetitions: {key}")
    if any(r["samples"] == "1" for r in jmh):
        warnings.append("JMH results with a single sample: a smoke run, not valid for the report")

    args.out.mkdir(parents=True, exist_ok=True)
    monitor = monitor_summary(directories)
    write_common(load, jmh, args.out / "common.csv")
    write_tables(load, jmh, monitor, warnings, args.out / "tables.md")
    figures(load, jmh, monitor, args.out)
    print(f"{len(load)} load points and {len(jmh)} JMH rows in {args.out}")
    for warning in warnings[:10]:
        print("WARNING:", warning)
    if len(warnings) > 10:
        print(f"... and {len(warnings) - 10} more warnings, see tables.md")


if __name__ == "__main__":
    main()
