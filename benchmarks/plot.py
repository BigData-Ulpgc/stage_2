#!/usr/bin/env python3
"""The figures of the Indexing benchmarks for the report (DR-14).

Reads what DR-12 and DR-13 committed and writes PNG figures, with the same style as
benchmarks/search/analyze.py (colors, ink, grid, size), so every figure of the report looks alike:

    benchmarks/indexing/results/experiments/summary.csv     the common CSV of plan §7.1 (DR-13)
    benchmarks/indexing/results/experiments/runs/*/timeline.csv
    benchmarks/indexing/results/experiments/probe.csv
    benchmarks/indexing/results/jmh/PrepareBatchBenchmark.csv  (DR-12)

Usage:  python3 benchmarks/plot.py [--out DIR] [--format png|pdf]
        default: PNG in benchmarks/indexing/results/figures
        for the report: --format pdf --out docs/report/figures (vector, as docs/report/charts.py)

Needs matplotlib. Only the standard library otherwise.
"""
import argparse
import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INDEXING = ROOT / "benchmarks" / "indexing" / "results"

# The same as benchmarks/search/analyze.py: categorical slots 1-4 of the reference palette,
# in fixed order, and recessive ink and grid.
COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
MARKERS = ["o", "s", "^", "D"]
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"
SIZE, DPI = (6.4, 3.8), 200

# Books between two points of the rate along the index (figure index_rate): a multiple of the
# batch (INDEX_WRITE_BATCH = 100), or the blocks alternate between 2 and 3 batches and saw-tooth.
RATE_STEP = 500


# --- reading ------------------------------------------------------------------------------

def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def summary_values(rows, experiment, metric):
    """{(threads, dataset_size): [value of each repetition]} of one metric."""
    values = defaultdict(list)
    for r in rows:
        if r["experiment"] == experiment and r["metric"] == metric:
            values[(int(r["threads"]), int(r["dataset_size"]))].append(float(r["value"]))
    return values


def mean_sd(values):
    values = list(values)
    return statistics.mean(values), (statistics.stdev(values) if len(values) > 1 else 0.0)


def timeline(path):
    """[(seconds, books indexed)] of one run."""
    return [(float(r["seconds"]), int(r["books_indexed"])) for r in read_csv(path)]


def rate_along(points, step):
    """Books/s of each block of `step` books: [(books indexed at the middle of the block, rate)].

    Uses the first second each multiple of `step` was reached, so start-up (0 books) and the
    seconds between samples do not count."""
    reached = {}
    for seconds, books in points:
        mark = books // step * step
        for k in range(step, mark + 1, step):
            reached.setdefault(k, seconds)
    out = []
    marks = sorted(reached)
    for before, after in zip(marks, marks[1:]):
        elapsed = reached[after] - reached[before]
        if elapsed > 0:
            out.append(((before + after) / 2, step / elapsed))
    return out


def jmh(path, param):
    """{param value: (score, error)} of a JMH CSV."""
    return {r[f"Param: {param}"]: (float(r["Score"]), float(r["Score Error (99.9%)"])) for r in read_csv(path)}


# --- drawing -----------------------------------------------------------------------------

def style(ax, title, xlabel, ylabel):
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


def legend(ax, **kwargs):
    ax.legend(frameon=False, labelcolor=MUTED, **kwargs)


def save(plt, fig, path):
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    print(f"  {path.name}")


def threads_figure(plt, summary, prepare, path):
    """Books/s against INDEXING_THREADS: the CPU part alone (JMH) and the whole path (compose)."""
    fig, ax = plt.subplots(figsize=SIZE, dpi=DPI)
    threads = [1, 2, 4, 8]
    cpu = [1e6 / prepare[str(t)][0] for t in threads]                 # us per book -> books/s
    whole = summary_values(summary, "index_threads", "throughput")
    stats = [mean_sd(whole[(t, 2000)]) for t in threads]
    ax.plot(threads, cpu, color=COLORS[0], marker=MARKERS[0], markersize=6, linewidth=2,
            label="read, parse and tokenize only (JMH, DR-12)")
    ax.errorbar(threads, [m for m, _ in stats], yerr=[s for _, s in stats], color=COLORS[1],
                marker=MARKERS[1], markersize=6, linewidth=2, capsize=3,
                label="whole path into MongoDB (compose, 2,000 books, DR-13)")
    for t, value in zip(threads, cpu):
        ax.annotate(f"{value:.0f}", (t, value), textcoords="offset points", xytext=(0, 7),
                    ha="center", fontsize=8, color=MUTED)
    for t, (value, _) in zip(threads, stats):
        ax.annotate(f"{value:.1f}", (t, value), textcoords="offset points", xytext=(0, 7),
                    ha="center", fontsize=8, color=MUTED)
    style(ax, "More threads speed up the CPU part, not the indexing", "INDEXING_THREADS",
          "books per second (log scale)")
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xticks(threads, [str(t) for t in threads])
    ax.minorticks_off()
    ax.set_ylim(5, 1000)
    ax.set_yticks([10, 30, 100, 300, 1000], ["10", "30", "100", "300", "1,000"])
    legend(ax, loc="center right")
    save(plt, fig, path)


def cpu_figure(plt, summary, path):
    """Mean CPU of Indexing and MongoDB during the threads experiment."""
    fig, ax = plt.subplots(figsize=SIZE, dpi=DPI)
    threads = [1, 2, 4, 8]
    width = 0.38
    for index, (metric, name) in enumerate([("cpu_indexing", "Indexing"), ("cpu_mongo", "MongoDB")]):
        values = summary_values(summary, "index_threads", metric)
        stats = [mean_sd(values[(t, 2000)]) for t in threads]
        positions = [i + (index - 0.5) * width for i in range(len(threads))]
        ax.bar(positions, [m for m, _ in stats], width=width - 0.03, color=COLORS[index], label=name,
               yerr=[s for _, s in stats], error_kw={"ecolor": MUTED, "capsize": 3, "linewidth": 1})
        for x, (m, _) in zip(positions, stats):
            ax.annotate(f"{m:.0f} %", (x, m), textcoords="offset points", xytext=(0, 4),
                        ha="center", fontsize=8, color=MUTED)
    ax.axhline(200, color=MUTED, linewidth=1, linestyle=(0, (4, 3)))
    ax.annotate("limit of each container: 2 CPUs", (len(threads) - 0.5, 200), textcoords="offset points",
                xytext=(0, -12), ha="right", fontsize=8, color=MUTED)
    style(ax, "MongoDB works, Indexing waits (2,000 books)", "INDEXING_THREADS", "mean CPU (% of one core)")
    ax.set_xticks(range(len(threads)), [str(t) for t in threads])
    ax.set_ylim(0, 220)
    legend(ax, loc="upper left")
    save(plt, fig, path)


def rate_figure(plt, runs, path):
    """Books/s against the books already in the index, for the runs of 10,000 and 30,000 books."""
    fig, ax = plt.subplots(figsize=SIZE, dpi=DPI)
    series = [("index_scale-t4-n30000-rep1", "30,000 books"),
              ("index_scale-t4-n10000-rep1", "10,000 books, rep 1"),
              ("index_scale-t4-n10000-rep2", "10,000 books, rep 2"),
              ("index_scale-t4-n10000-rep3", "10,000 books, rep 3")]
    for index, (name, label) in enumerate(series):
        file = runs / name / "timeline.csv"
        if not file.exists():
            print(f"  (no {file.relative_to(ROOT)})", file=sys.stderr)
            continue
        points = rate_along(timeline(file), RATE_STEP)
        ax.plot([b for b, _ in points], [r for _, r in points], color=COLORS[index],
                linewidth=2 if index == 0 else 1.5, label=label)
    style(ax, r"The rate falls as the index grows (\$addToSet)", "books already indexed",
          f"books per second (blocks of {RATE_STEP})")
    ax.set_xlim(0, 30000)
    ax.set_ylim(bottom=0)
    ax.xaxis.set_major_formatter(lambda x, _: f"{x:,.0f}")
    legend(ax, loc="upper right")
    save(plt, fig, path)


def probe_figure(plt, probe, path):
    """Latency of adding 100 ids to one posting list: $addToSet against $push."""
    fig, ax = plt.subplots(figsize=SIZE, dpi=DPI)
    for index, (operation, name) in enumerate([("addToSet", r"\$addToSet (what the index uses)"),
                                               ("push", r"\$push (no check for duplicates)")]):
        rows = sorted((int(r["list_length"]), float(r["median_ms"])) for r in probe if r["operation"] == operation)
        ax.plot([n for n, _ in rows], [ms for _, ms in rows], color=COLORS[index], marker=MARKERS[index],
                markersize=6, linewidth=2, label=name)
        for n, ms in rows:
            ax.annotate(f"{ms:.1f}", (n, ms), textcoords="offset points", xytext=(0, 7),
                        ha="center", fontsize=8, color=MUTED)
    style(ax, r"\$addToSet grows with the list, \$push barely does", "ids already in the list (log scale)",
          "median ms per 100 ids (log scale)")
    ax.set_xscale("log")
    ax.set_yscale("log")
    lengths = sorted({int(r["list_length"]) for r in probe})
    ax.set_xticks(lengths, [f"{n:,}" for n in lengths])
    ax.minorticks_off()
    ax.set_ylim(0.5, 500)
    ax.set_yticks([1, 10, 100], ["1", "10", "100"])
    legend(ax, loc="upper left")
    save(plt, fig, path)


def publish_figure(plt, summary, path):
    """POST /index/publish on an index of 10,000 books: each of the 10 calls, and the median."""
    fig, ax = plt.subplots(figsize=SIZE, dpi=DPI)
    levels = [1, 10, 50]
    for index, level in enumerate(levels):
        values = [float(r["value"]) for r in summary
                  if r["experiment"] == f"index_publish_{level}" and r["metric"] == "publish_latency"]
        xs = [index + (i - (len(values) - 1) / 2) * 0.03 for i in range(len(values))]
        ax.scatter(xs, values, s=22, color=COLORS[0], alpha=0.55, linewidths=0)
        median = statistics.median(values)
        ax.plot([index - 0.25, index + 0.25], [median, median], color=INK, linewidth=2)
        ax.annotate(f"median {median:.0f} ms", (index + 0.27, median), va="center", fontsize=8, color=MUTED)
    style(ax, "A publication costs about 70 ms whatever its size", "new books in the publication",
          "ms per POST /index/publish")
    ax.set_xticks(range(len(levels)), [str(level) for level in levels])
    ax.set_xlim(-0.5, len(levels) - 0.1)
    ax.set_ylim(0, 110)
    save(plt, fig, path)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=INDEXING / "figures")
    parser.add_argument("--format", choices=["png", "pdf"], default="png")
    args = parser.parse_args()
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        sys.exit("matplotlib is not installed: pip install matplotlib")
    plt.rcParams["font.size"] = 9

    experiments = INDEXING / "experiments"
    summary = read_csv(experiments / "summary.csv")
    prepare = jmh(INDEXING / "jmh" / "PrepareBatchBenchmark.csv", "threads")
    args.out.mkdir(parents=True, exist_ok=True)
    print(f"figures in {args.out.relative_to(ROOT) if args.out.is_relative_to(ROOT) else args.out}:")
    threads_figure(plt, summary, prepare, args.out / f"index_threads.{args.format}")
    cpu_figure(plt, summary, args.out / f"index_threads_cpu.{args.format}")
    rate_figure(plt, experiments / "runs", args.out / f"index_rate.{args.format}")
    probe_figure(plt, read_csv(experiments / "probe.csv"), args.out / f"index_probe.{args.format}")
    publish_figure(plt, summary, args.out / f"index_publish.{args.format}")


if __name__ == "__main__":
    main()
