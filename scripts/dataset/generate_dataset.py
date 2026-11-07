#!/usr/bin/env python3
"""
Reproducible synthetic dataset (DR-16): N books in the format of sample_dataset/raw, one
pg<id>.txt per book, for the local source of Ingestion (INGESTION_SOURCE=local) and the scale
experiments (10,000 and 100,000 books) without downloading anything from Project Gutenberg.

Each file looks like a raw Gutenberg plain-text file, byte for byte in its structure: CRLF line
ends, the "Title:", "Author:", "Release date: ... [eBook #<id>]" and "Language:" lines, and the
"*** START OF THE PROJECT GUTENBERG EBOOK ... ***" / "*** END OF ..." markers that BookSplitter
looks for. The "Credits:" line says that the book is synthetic.

The body draws its words from a vocabulary of VOCABULARY_SIZE ranks: first the real words of
vocabulary.txt (the sample books, most frequent first, stopwords included), then deterministic
pseudo-words ("baka", "tobemi"...). Two regimes, as in large real corpora (Ferrer i Cancho and
Sole, 2001):
  - the first HEAD_RANKS ranks keep their real frequency: words seen 31 times or more in the 15
    real books, so their frequency is well measured. That keeps the share of stopwords (42 %)
    and the weight of each common word;
  - beyond, Zipf with slope 2.0, joined to the real frequency of the last head rank.
Calibrated against the real books: the same number of words as the 15 of them gives 29,900
distinct words (the real ones: 30,442), and 4 times as many words gives twice as many, Heaps'
exponent 0.5 of real text. One slope for all the ranks (1.23, the one measured between ranks
10 and 10,000) put 69 % of stopwords in the text and grew the vocabulary far too fast: 200
books had 204,501 distinct terms instead of about 60,000. The spread of the lengths is the
measured one; the median is smaller (see --median-words), so that 100,000 books fit on a laptop.

Reproducible: a book depends only on (seed, id), never on N, on the order of generation or on
the number of workers. So the 10,000-book dataset is exactly the first 10,000 books of the
100,000 one, and every member of the group generates the same bytes (manifest.json has a digest
to compare). A change in how books are made must change GENERATOR (and the digest of the tests).

Output in --out: pg<id>.txt (written atomically, through a temporary file), book_ids.txt (for
CONTROL_SEED_FILE or POST /control/jobs), SHA256SUMS and manifest.json.

Usage (from the repository root):
    python3 scripts/dataset/generate_dataset.py --count 10000            # into data/books
    python3 scripts/dataset/generate_dataset.py --verify data/books
"""

import argparse
import array
import bisect
import datetime
import hashlib
import itertools
import json
import math
import multiprocessing
import os
import pathlib
import platform
import random
import sys

GENERATOR = "DR-16 generate_dataset.py v1"

HERE = pathlib.Path(__file__).resolve().parent
VOCABULARY = HERE / "vocabulary.txt"

DEFAULT_SEED = 42
DEFAULT_FIRST_ID = 1_000_001          # above every real Gutenberg id (~75,000): they never collide
DEFAULT_OUT = pathlib.Path("data/books")
DEFAULT_MEDIAN_WORDS = 10_000         # the real median is ~71,000: see the README for the disk
DEFAULT_SIGMA = 1.34                  # spread of log(length) of the real books (vocabulary.txt)
DEFAULT_HEAD_RANKS = 3_000           # ranks with their real frequency (seen 31+ times)
DEFAULT_ZIPF_TAIL = 2.0               # Zipf slope beyond them (literature; calibrated: see above)
DEFAULT_VOCABULARY_SIZE = 2_000_000   # ranks: real words, then pseudo-words
LENGTH_RANGE = 20                     # lengths kept within median/20 .. median*20

LINE_WIDTH = 72
SYLLABLES = [c + v for c in "bcdfghjklmnprstvz" for v in "aeiou"]

# The header mix of the 200 real headers (DR-04 survey): 9 with "Original publication:" (all
# released 2022-2025), 3 without "Author:" (2 of them with "Creator:"), all in English. 10 % in
# other languages, so that the language filter of Search has something to filter.
P_ORIGINAL_PUBLICATION = 0.045
P_NO_AUTHOR = 0.015
P_CREATOR_WHEN_NO_AUTHOR = 2 / 3
P_OTHER_LANGUAGE = 0.10
OTHER_LANGUAGES = ["French", "German", "Spanish", "Italian", "Portuguese", "Dutch", "Finnish", "Swedish"]
P_UPDATED = 0.4

FIRST_NAMES = ["Ada", "Albert", "Alice", "Anne", "Arthur", "Beatrice", "Charles", "Charlotte", "Clara",
               "Daniel", "Edith", "Edward", "Elinor", "Emily", "Ernest", "Frances", "George", "Grace",
               "Harriet", "Henry", "Isabel", "James", "Jane", "John", "Louisa", "Lucy", "Margaret",
               "Mary", "Nathaniel", "Oliver", "Rose", "Samuel", "Sarah", "Thomas", "Walter", "William"]
LAST_NAMES = ["Ashford", "Barlow", "Bennett", "Blake", "Carrow", "Dalton", "Easton", "Fairfax", "Fletcher",
              "Garland", "Hale", "Hartley", "Hollis", "Kendall", "Lacey", "Linton", "Marsh", "Merritt",
              "Norwood", "Oakley", "Pemberton", "Prescott", "Ramsey", "Rowe", "Sinclair", "Stanton",
              "Thorne", "Vance", "Wakefield", "Whitmore", "Winslow", "Yates"]
CREATORS = ["Anonymous", "Various", "United States", "Royal Society of London"]
PUBLISHERS = ["London: Smith, Elder", "New York: Harper", "Boston: Ticknor", "Edinburgh: Blackwood",
              "Paris: Hachette", "London: Macmillan"]
MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September",
          "October", "November", "December"]

PREAMBLE = [
    "This eBook is for the use of anyone anywhere in the United States and",
    "most other parts of the world at no cost and with almost no restrictions",
    "whatsoever. You may copy it, give it away or re-use it under the terms",
    "of the Project Gutenberg License included with this eBook or online",
    "at www.gutenberg.org. If you are not located in the United States,",
    "you will have to check the laws of the country where you are located",
    "before using this eBook.",
]
FOOTER = [
    "Updated editions will replace the previous one--the old editions will",
    "be renamed.",
    "",
    "This is a synthetic book of the DR-16 dataset: its text is random words.",
]


# ---- the vocabulary and the Zipf sampler ----

def real_entries(path=VOCABULARY):
    """(word, count) of vocabulary.txt, most frequent first."""
    with open(path, encoding="utf-8") as f:
        rows = [line.rstrip("\r\n").split("\t") for line in f if line.strip() and not line.startswith("#")]
    return [(word, int(count)) for word, count in rows]


def real_words(path=VOCABULARY):
    """The words of vocabulary.txt, most frequent first."""
    return [word for word, _ in real_entries(path)]


def pseudo_words(taken):
    """Deterministic pseudo-words of 2, 3, 4... syllables, skipping the words in taken."""
    for length in itertools.count(2):
        for parts in itertools.product(SYLLABLES, repeat=length):
            word = "".join(parts)
            if word not in taken:
                yield word


def vocabulary(size, path=VOCABULARY):
    """size ranks: the real words, then pseudo-words."""
    words = real_words(path)[:size]
    taken = set(words)
    words.extend(itertools.islice(pseudo_words(taken), size - len(words)))
    return words


def cumulative_weights(size, counts, head, tail):
    """
    Cumulative weights for random.choices (an array: 8 bytes a rank): rank r weighs its real
    count up to rank head, and counts[head-1] * (head/r)^tail beyond, so both meet at rank head.
    """
    head = min(head, len(counts), size)
    last = counts[head - 1]
    weights = (float(counts[r - 1]) if r <= head else last * (head / r) ** tail for r in range(1, size + 1))
    return array.array("d", itertools.accumulate(weights))


class Sampler:
    """What every book shares: the vocabulary and its cumulative weights."""

    def __init__(self, vocabulary_size, head_ranks, zipf_tail):
        self.words = vocabulary(vocabulary_size)
        counts = [count for _, count in real_entries()]
        self.cumulative = cumulative_weights(vocabulary_size, counts, head_ranks, zipf_tail)
        # Title words: real, frequent but not the very first ranks (the, and, of...), letters only
        self.title_words = [w for w in self.words[50:3000] if w.isalpha()]

    def draw(self, rng, n):
        return rng.choices(self.words, cum_weights=self.cumulative, k=n)


# ---- one book ----

def book_rng(seed, book_id):
    """The random generator of one book: depends only on (seed, id)."""
    return random.Random(f"DR-16:{seed}:{book_id}")


def length_of(rng, median, sigma):
    words = round(rng.lognormvariate(math.log(median), sigma))
    return max(median // LENGTH_RANGE, min(median * LENGTH_RANGE, words))


def wrap(words, width=LINE_WIDTH):
    """Greedy wrap of a paragraph into lines of at most width characters."""
    lines, line, used = [], [], 0
    for word in words:
        if line and used + 1 + len(word) > width:
            lines.append(" ".join(line))
            line, used = [], 0
        used += len(word) + (1 if line else 0)
        line.append(word)
    if line:
        lines.append(" ".join(line))
    return lines


def body_lines(rng, words):
    """Sentences of 6-24 words (capitalized, with a period) in paragraphs of 2-7 sentences."""
    lines, i, n = [], 0, len(words)
    while i < n:
        paragraph = []
        for _ in range(rng.randint(2, 7)):
            if i >= n:
                break
            sentence = words[i:i + rng.randint(6, 24)]
            i += len(sentence)
            sentence[0] = sentence[0].capitalize()
            sentence[-1] += "."
            paragraph.extend(sentence)
        lines.extend(wrap(paragraph))
        lines.append("")
    return lines


def date_text(day):
    return f"{MONTHS[day.month - 1]} {day.day}, {day.year}"


def random_day(rng, first_year, last_year):
    start = datetime.date(first_year, 1, 1).toordinal()
    end = datetime.date(last_year, 12, 31).toordinal()
    return datetime.date.fromordinal(rng.randint(start, end))


def header_lines(rng, sampler, book_id, seed):
    title = " ".join(rng.choice(sampler.title_words) for _ in range(rng.randint(1, 6))).title()
    lines = [f"The Project Gutenberg eBook of {title}", "    ", *PREAMBLE, "", f"Title: {title}", ""]
    if rng.random() < P_NO_AUTHOR:
        if rng.random() < P_CREATOR_WHEN_NO_AUTHOR:
            lines += [f"Creator: {rng.choice(CREATORS)}", ""]
    else:
        lines += [f"Author: {rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}", ""]
    original = rng.random() < P_ORIGINAL_PUBLICATION
    # Release years with the median of the real ones (2004); the ones with "Original
    # publication:" were all released 2022-2025.
    released = random_day(rng, 2022, 2025) if original else random_day(rng, 1992, 2016)
    lines += ["", "        ", f"Release date: {date_text(released)} [eBook #{book_id}]"]
    if rng.random() < P_UPDATED:
        updated = random_day(rng, max(released.year, 2018), 2026)
        lines.append(f"                Most recently updated: {date_text(updated)}")
    lines.append("")
    language = rng.choice(OTHER_LANGUAGES) if rng.random() < P_OTHER_LANGUAGE else "English"
    lines += [f"Language: {language}", ""]
    if original:
        lines += [f"Original publication: {rng.choice(PUBLISHERS)}, {rng.randint(1600, 1925)}", ""]
    lines += [f"Credits: synthetic book of the DR-16 dataset (seed {seed}), not a real eBook", "", ""]
    return title, lines


def make_book(sampler, seed, book_id, median, sigma):
    """The text of pg<id>.txt, with CRLF line ends."""
    rng = book_rng(seed, book_id)
    title, lines = header_lines(rng, sampler, book_id, seed)
    words = sampler.draw(rng, length_of(rng, median, sigma))
    lines.append(f"*** START OF THE PROJECT GUTENBERG EBOOK {title.upper()} ***")
    lines += ["", "", ""]
    lines.append(title.upper())
    lines += ["", ""]
    lines += body_lines(rng, words)
    lines.append(f"*** END OF THE PROJECT GUTENBERG EBOOK {title.upper()} ***")
    lines += [""]
    lines += FOOTER
    return "\r\n".join(lines) + "\r\n"


# ---- the dataset ----

_sampler = None


def _init_worker(vocabulary_size, head_ranks, zipf_tail):
    global _sampler
    _sampler = Sampler(vocabulary_size, head_ranks, zipf_tail)


def _write_book(job):
    out, seed, book_id, median, sigma = job
    data = make_book(_sampler, seed, book_id, median, sigma).encode("utf-8")
    target = out / f"pg{book_id}.txt"
    temporary = out / f".pg{book_id}.txt.tmp"          # not pg*.txt: LocalFileSource ignores it
    temporary.write_bytes(data)
    os.replace(temporary, target)
    return book_id, hashlib.sha256(data).hexdigest(), len(data)


def parameters(args):
    """What decides the bytes of a book (the count does not: see the class docstring)."""
    return {
        "generator": GENERATOR,
        "seed": args.seed,
        "first_id": args.first_id,
        "median_words": args.median_words,
        "sigma": args.sigma,
        "head_ranks": args.head_ranks,
        "zipf_tail": args.zipf_tail,
        "vocabulary_size": args.vocabulary_size,
        # Of the words and counts, not of the file: git may turn its line ends into CRLF on Windows
        "vocabulary_sha256": hashlib.sha256(
            "\n".join(f"{w}\t{n}" for w, n in real_entries()).encode()).hexdigest(),
    }


def digest(sums):
    """One SHA-256 for the whole dataset, from the SHA-256 of each file in id order."""
    h = hashlib.sha256()
    for book_id, sha in sums:
        h.update(f"{sha}  pg{book_id}.txt\n".encode())
    return h.hexdigest()


def check_out_dir(out, params):
    """Refuses to mix two datasets in one folder: same parameters, or an empty folder."""
    manifest = out / "manifest.json"
    if manifest.exists():
        old = json.loads(manifest.read_text(encoding="utf-8"))
        changed = sorted(k for k in params if old.get(k) != params[k])
        if changed:
            raise SystemExit(f"{out} has a dataset with other parameters ({', '.join(changed)}): "
                             f"use another --out or empty it")
    elif any(out.glob("pg*.txt")):
        raise SystemExit(f"{out} has pg*.txt files that are not from this generator: use another --out")


def generate(args):
    params = parameters(args)
    args.out.mkdir(parents=True, exist_ok=True)
    check_out_dir(args.out, params)
    ids = range(args.first_id, args.first_id + args.count)
    jobs = ((args.out, args.seed, i, args.median_words, args.sigma) for i in ids)
    sums, total = [], 0
    step = max(1, args.count // 20)
    with multiprocessing.Pool(args.workers, _init_worker, (args.vocabulary_size, args.head_ranks, args.zipf_tail)) as pool:
        for n, (book_id, sha, size) in enumerate(pool.imap(_write_book, jobs, chunksize=16), 1):
            sums.append((book_id, sha))
            total += size
            if n % step == 0 or n == args.count:
                print(f"{n}/{args.count} books, {total / 1e6:.0f} MB", file=sys.stderr)

    with open(args.out / "SHA256SUMS", "w", encoding="utf-8", newline="\n") as f:
        f.writelines(f"{sha}  pg{book_id}.txt\n" for book_id, sha in sums)
    with open(args.out / "book_ids.txt", "w", encoding="utf-8", newline="\n") as f:
        f.write(f"# Synthetic dataset DR-16: {args.count} books, seed {args.seed} (manifest.json)\n")
        f.writelines(f"{book_id}\n" for book_id in ids)
    manifest = dict(params, count=args.count, bytes=total, digest=digest(sums),
                    python=platform.python_version())
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"{args.out}: {args.count} books, {total / 1e6:.0f} MB, digest {manifest['digest']}", file=sys.stderr)
    return manifest


def verify(out):
    """Checks every file against SHA256SUMS and the digest of manifest.json; True if all match."""
    manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    sums, bad = [], 0
    for line in (out / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        sha, name = line.split("  ", 1)
        try:
            actual = hashlib.sha256((out / name).read_bytes()).hexdigest()
        except FileNotFoundError:
            actual = None
        if actual != sha:
            bad += 1
            print(f"{name}: {'missing' if actual is None else 'changed'}", file=sys.stderr)
        sums.append((int(name[2:-4]), sha))
    same_digest = digest(sums) == manifest["digest"] and len(sums) == manifest["count"]
    if not same_digest:
        print("SHA256SUMS does not match the digest of manifest.json", file=sys.stderr)
    print(f"{out}: {len(sums) - bad}/{len(sums)} files match, digest {manifest['digest']}", file=sys.stderr)
    return bad == 0 and same_digest


def positive(text):
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {value}")
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--count", type=positive, help="number of books")
    parser.add_argument("--verify", type=pathlib.Path, metavar="DIR", help="check a generated dataset instead")
    parser.add_argument("--out", type=pathlib.Path, default=DEFAULT_OUT, help=f"default {DEFAULT_OUT}")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help=f"default {DEFAULT_SEED}")
    parser.add_argument("--first-id", type=positive, default=DEFAULT_FIRST_ID, help=f"default {DEFAULT_FIRST_ID}")
    parser.add_argument("--median-words", type=positive, default=DEFAULT_MEDIAN_WORDS,
                        help=f"median length of a body in words, default {DEFAULT_MEDIAN_WORDS}")
    parser.add_argument("--sigma", type=float, default=DEFAULT_SIGMA,
                        help=f"spread of log(length), default {DEFAULT_SIGMA}")
    parser.add_argument("--head-ranks", type=positive, default=DEFAULT_HEAD_RANKS,
                        help=f"ranks with their real frequency, default {DEFAULT_HEAD_RANKS}")
    parser.add_argument("--zipf-tail", type=float, default=DEFAULT_ZIPF_TAIL,
                        help=f"Zipf slope beyond them, default {DEFAULT_ZIPF_TAIL}")
    parser.add_argument("--vocabulary-size", type=positive, default=DEFAULT_VOCABULARY_SIZE,
                        help=f"ranks of the vocabulary, default {DEFAULT_VOCABULARY_SIZE}")
    parser.add_argument("--workers", type=positive, default=os.cpu_count() or 1, help="processes, default: one per CPU")
    args = parser.parse_args(argv)
    if args.verify:
        sys.exit(0 if verify(args.verify) else 1)
    if args.count is None:
        parser.error("--count is required (or --verify DIR)")
    if args.first_id + args.count - 1 > 999_999_999:
        parser.error("the ids must stay within BookIds (1..999999999)")
    if args.sigma < 0 or args.zipf_tail <= 0:
        parser.error("--sigma must be >= 0 and --zipf-tail > 0")
    generate(args)


if __name__ == "__main__":
    main()
