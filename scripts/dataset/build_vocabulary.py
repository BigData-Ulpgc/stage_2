#!/usr/bin/env python3
"""
Builds the vocabulary of the synthetic dataset (DR-16) from the bodies of the sample dataset.

The synthetic books draw their words from this list with a Zipf distribution (see
generate_dataset.py): rank 1 is the most frequent word of the real books, rank 2 the next one,
and so on. Real words make a synthetic book searchable with real queries ("whale", "london"),
and the stopwords stay in the list on purpose: the tokenizer drops them, as it does with a
real book.

It tokenizes with the rules of Tokenizer (shared/SPEC.md, section 5), the same as Indexing and
Search: A-Z becomes a-z; a-z and 0-9 are part of a token; any other character, non-ASCII
included, is a separator; tokens shorter than 2 are dropped. Stopwords are NOT dropped here.

The output starts with comment lines (#) that record what was measured on the real books (the
spread of the lengths is the default of generate_dataset.py; the counts give the frequency of
its first ranks). Then one line per word, "word<TAB>count", most
frequent first; ties are sorted alphabetically, so the file is the same on every machine.

Usage (from the repository root):
    python3 scripts/dataset/build_vocabulary.py ../stage_1/sample_dataset
"""

import argparse
import collections
import math
import pathlib
import sys

DEFAULT_OUT = pathlib.Path(__file__).resolve().parent / "vocabulary.txt"
MIN_LENGTH = 2
# The ranks used to measure the slope: the first ones are a handful of very frequent words that
# bend the line, and the last ones are words seen once or twice.
SLOPE_RANKS = (10, 10_000)


def tokens(text):
    """Tokens of text with the rules of Tokenizer, stopwords included."""
    out, current = [], []
    for ch in text:
        if "A" <= ch <= "Z":
            current.append(chr(ord(ch) + 32))
        elif "a" <= ch <= "z" or "0" <= ch <= "9":
            current.append(ch)
        else:
            if len(current) >= MIN_LENGTH:
                out.append("".join(current))
            current = []
    if len(current) >= MIN_LENGTH:
        out.append("".join(current))
    return out


def zipf_slope(frequencies, first, last):
    """Least-squares slope s of log(frequency) = c - s*log(rank), for ranks first..last."""
    last = min(last, len(frequencies))
    if last - first < 1:
        return float("nan")                 # too few words to measure it (a tiny sample)
    xs = [math.log(r) for r in range(first, last + 1)]
    ys = [math.log(frequencies[r - 1]) for r in range(first, last + 1)]
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    return -sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sum((x - mx) ** 2 for x in xs)


def measure(sample_dir):
    """Word counts of every body, and the length in tokens of each book."""
    bodies = sorted((sample_dir / "book").glob("*/body.txt"), key=lambda p: int(p.parent.name))
    if not bodies:
        raise SystemExit(f"no book/<id>/body.txt in {sample_dir}: is it the sample dataset?")
    counts, lengths = collections.Counter(), []
    for body in bodies:
        words = tokens(body.read_text(encoding="utf-8"))
        counts.update(words)
        lengths.append(len(words))
    return counts, lengths


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("sample_dir", type=pathlib.Path, help="the sample dataset (with book/<id>/body.txt)")
    parser.add_argument("--out", type=pathlib.Path, default=DEFAULT_OUT, help=f"default {DEFAULT_OUT}")
    args = parser.parse_args(argv)

    counts, lengths = measure(args.sample_dir)
    ranked = sorted(counts.items(), key=lambda wc: (-wc[1], wc[0]))
    logs = [math.log(n) for n in lengths]
    mean_log = sum(logs) / len(logs)
    sigma = math.sqrt(sum((x - mean_log) ** 2 for x in logs) / (len(logs) - 1))
    median = sorted(lengths)[len(lengths) // 2]
    slope = zipf_slope([n for _, n in ranked], *SLOPE_RANKS)

    with open(args.out, "w", encoding="utf-8", newline="\n") as out:
        out.write("# Vocabulary of the synthetic dataset (DR-16), built by build_vocabulary.py\n")
        out.write(f"# books: {len(lengths)}\n")
        out.write(f"# tokens: {sum(lengths)}\n")
        out.write(f"# words: {len(ranked)}\n")
        out.write(f"# median_tokens_per_book: {median}\n")
        out.write(f"# sigma_log_tokens_per_book: {sigma:.2f}\n")
        out.write(f"# zipf_slope_ranks_{SLOPE_RANKS[0]}_{SLOPE_RANKS[1]}: {slope:.2f}\n")
        for word, n in ranked:
            out.write(f"{word}\t{n}\n")
    print(f"{args.out}: {len(ranked)} words from {len(lengths)} books; median {median} tokens per book, "
          f"sigma(log) {sigma:.2f}, Zipf slope {slope:.2f}", file=sys.stderr)


if __name__ == "__main__":
    main()
