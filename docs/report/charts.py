#!/usr/bin/env python3
"""Figures of the Search part of the report, from the results committed in benchmarks/search/results.

    python docs/report/charts.py        (needs matplotlib; writes docs/report/figures/*.pdf)

Only what is measured: a missing result gives no figure, never a made-up one.
"""
import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
JMH = HERE.parent.parent / "benchmarks" / "search" / "results" / "jmh"
OUT = HERE / "figures"
COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
MARKERS = ["o", "s", "^", "D"]
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"
LABELS = {"common": "common (50 %)", "common_frequent": "common + frequent (15 %)",
          "common_frequent_medium": "common + frequent + medium (1.5 %)", "common_rare": "common + rare (0.5 %)"}


def rows(name):
    path = JMH / f"{name}.csv"
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def duration(micros):
    return f"{micros / 1000:.2f} ms" if micros >= 1000 else f"{micros:.1f} µs"


def style(ax, title, xlabel, ylabel):
    ax.set_title(title, loc="left", fontsize=10, color=INK)
    ax.set_xlabel(xlabel, color=MUTED)
    ax.set_ylabel(ylabel, color=MUTED)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=MUTED, length=0)


def intersection():
    data = rows("IntersectionBenchmark")
    if not data:
        return
    fig, ax = plt.subplots(figsize=(6.2, 3.6))
    for i, terms in enumerate(LABELS):
        points = sorted((int(r["Param: books"]), float(r["Score"])) for r in data if r["Param: terms"] == terms)
        ax.plot([b for b, _ in points], [s / 1000 for _, s in points], color=COLORS[i], marker=MARKERS[i],
                linewidth=2, markersize=6, label=LABELS[terms])
    ax.set_xscale("log")
    ax.set_yscale("log")
    style(ax, "JMH: time per search by index size (fraction of books that match)", "books in the index",
          "ms per search (log)")
    ax.legend(frameon=False, labelcolor=MUTED, fontsize=8)
    fig.tight_layout()
    fig.savefig(OUT / "jmh_intersection.pdf")
    plt.close(fig)


def threads():
    points = []
    for n in (1, 2, 4, 8):
        data = rows(f"threads-{n}")
        if data:
            points.append((n, n * 1e6 / float(data[-1]["Score"])))
    if len(points) < 2:
        return
    fig, ax = plt.subplots(figsize=(6.2, 3.4))
    base = points[0][1]
    ax.plot([n for n, _ in points], [base * n for n, _ in points], color=MUTED, linestyle="--", linewidth=1,
            label="perfect scaling")
    ax.plot([n for n, _ in points], [t for _, t in points], color=COLORS[0], marker="o", linewidth=2, label="measured")
    for n, t in points:
        ax.annotate(f"{t / base:.1f}x", (n, t), textcoords="offset points", xytext=(0, 8), ha="center",
                    color=INK, fontsize=8)
    ax.set_xticks([n for n, _ in points])
    ax.set_ylim(bottom=0)
    style(ax, "JMH: searches per second with threads (100,000 books)", "threads", "searches per second")
    ax.legend(frameon=False, labelcolor=MUTED, fontsize=8)
    fig.tight_layout()
    fig.savefig(OUT / "jmh_threads.pdf")
    plt.close(fig)


def pagination():
    data = [r for r in rows("PaginationBenchmark") if r["Param: limit"] == "20"]
    if not data:
        return
    fig, ax = plt.subplots(figsize=(6.2, 3.4))
    sizes = sorted({int(r["Param: books"]) for r in data})
    for i, books in enumerate(sizes):
        points = sorted((int(r["Param: position"]), float(r["Score"])) for r in data if int(r["Param: books"]) == books)
        first = points[0][1]
        ax.plot([p for p, _ in points], [s / first for _, s in points], color=COLORS[i], marker=MARKERS[i],
                linewidth=2, markersize=6, label=f"{books:,} books ({duration(first)} the first page)")
    ax.set_ylim(0, 1.3)
    ax.set_xticks([0, 50, 99])
    style(ax, "JMH: cost of a page relative to the first one (20 results)", "position of the page (% of the results)",
          "time / time of the first page")
    ax.legend(frameon=False, labelcolor=MUTED, fontsize=8, loc="lower right")
    fig.tight_layout()
    fig.savefig(OUT / "jmh_pagination.pdf")
    plt.close(fig)


if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    intersection()
    threads()
    pagination()
    print("figures in", OUT)
