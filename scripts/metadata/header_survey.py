#!/usr/bin/env python3
"""
Survey of the Project Gutenberg headers of the reference books (DR-04, step 1).

Answers, with real data, the questions behind the metadata rules of contract §6.3:
  - which values appear in "Language:" (to build the ISO 639-1 table),
  - how many books have several languages ("English, French"),
  - how many have "Original publication:" and how many only "Release date:",
  - which year the rule would give, and from which source.

It reads the fields with the same rules as MetadataParser (shared/SPEC.md):
first line that starts with "Field:", only that line, trimmed, empty -> missing,
and in "Release date" whatever is between brackets is dropped.

Downloads are polite: each book is downloaded ONCE (only its first bytes, where
the header is), with a pause between requests, and cached in data/headers/
(ignored by Git). Running the script again reads the cache and touches no network.

Usage (from the repository root):
    python3 scripts/metadata/header_survey.py
    python3 scripts/metadata/header_survey.py --ids ../../Stage_1/shared/book_ids.txt --delay 2
    python3 scripts/metadata/header_survey.py --offline        # cache only
"""

import argparse
import collections
import csv
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

URL = "https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"     # same as Stage 1's GutenbergClient
USER_AGENT = "ULPGC-BigData-Stage2-header-survey/1.0 (student project; one request per book)"
MAX_BYTES = 64 * 1024        # the header is in the first few KB; the whole book is not needed
START_MARKER = re.compile(r"^\*\*\* ?START OF (THE|THIS) PROJECT GUTENBERG EBOOK", re.MULTILINE | re.IGNORECASE)

# The same regexes as MetadataParser (MULTILINE: ^ and $ are start and end of each line).
FIELDS = {
    "title": re.compile(r"^Title:\s*(.+)$", re.MULTILINE),
    "author": re.compile(r"^Author:\s*(.+)$", re.MULTILINE),
    "language": re.compile(r"^Language:\s*(.+)$", re.MULTILINE),
    "release_date": re.compile(r"^Release date:\s*(.+?)(\s*\[.*)?$", re.MULTILINE),
    "original_publication": re.compile(r"^Original publication:\s*(.+)$", re.MULTILINE),
}
YEAR = re.compile(r"\b(\d{4})\b")


def read_ids(path):
    """Ids of the file, skipping comments (#) and blank lines, in order and without repeats."""
    ids = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if line and int(line) not in ids:
            ids.append(int(line))
    return ids


def download_header(book_id, timeout):
    """First MAX_BYTES of the book, cut before the START marker. Raises on HTTP or network errors."""
    request = urllib.request.Request(URL.format(id=book_id), headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read(MAX_BYTES)          # stops reading here: the rest of the book is not transferred
    text = raw.decode("utf-8", errors="replace").replace("\r\n", "\n")
    match = START_MARKER.search(text)
    if match is None:
        raise ValueError(f"no START marker in the first {MAX_BYTES} bytes")
    return text[:match.start()]


def first_match(pattern, text):
    """Group 1 of the first match, trimmed; None if there is none or it ends up empty."""
    m = pattern.search(text)
    if m is None:
        return None
    value = m.group(1).strip()
    return value or None


def year_of(text):
    """Last 4-digit number of the text ("June 1, 1998" -> 1998), or None."""
    if text is None:
        return None
    years = YEAR.findall(text)
    return int(years[-1]) if years else None


def parse(header):
    fields = {name: first_match(pattern, header) for name, pattern in FIELDS.items()}
    # Year rule of contract §6.3: original publication if present, else the ebook's release date.
    year, source = year_of(fields["original_publication"]), "original_publication"
    if year is None:
        year, source = year_of(fields["release_date"]), "release_date"
    fields["year"] = year
    fields["year_source"] = source if year is not None else None
    return fields


def load_headers(ids, cache, offline, delay, timeout):
    """id -> header text. Downloads only what is not in the cache."""
    headers, failures = {}, {}
    cache.mkdir(parents=True, exist_ok=True)
    pending = [i for i in ids if not (cache / f"{i}.header.txt").exists()]
    if pending and not offline:
        print(f"Downloading {len(pending)} headers ({delay}s between requests)...", file=sys.stderr)
    for n, book_id in enumerate(ids, 1):
        cached = cache / f"{book_id}.header.txt"
        if cached.exists():
            headers[book_id] = cached.read_text(encoding="utf-8")
            continue
        if offline:
            failures[book_id] = "not in cache (--offline)"
            continue
        try:
            header = download_header(book_id, timeout)
            cached.write_text(header, encoding="utf-8")
            headers[book_id] = header
            print(f"  [{n}/{len(ids)}] {book_id} ok", file=sys.stderr)
        except (urllib.error.URLError, TimeoutError, ValueError) as e:
            failures[book_id] = str(e)
            print(f"  [{n}/{len(ids)}] {book_id} FAILED: {e}", file=sys.stderr)
        time.sleep(delay)                       # polite: never two requests in a row
    return headers, failures


def report(books, failures):
    total = len(books)

    def pct(n):
        return f"{n:4d}  ({100 * n / total:5.1f} %)" if total else f"{n:4d}"

    print(f"\n=== Header survey: {total} books read, {len(failures)} failed ===")
    for book_id, reason in failures.items():
        print(f"  failed {book_id}: {reason}")

    print("\n-- Fields present --")
    for name in ("title", "author", "language", "release_date", "original_publication"):
        print(f"  {name:22s} {pct(sum(1 for b in books.values() if b[name]))}")

    print("\n-- Language values (raw text after 'Language:') --")
    for value, n in collections.Counter(b["language"] for b in books.values()).most_common():
        print(f"  {str(value):30s} {pct(n)}")
    multi = {i: b["language"] for i, b in books.items() if b["language"] and re.search(r",|;| and ", b["language"])}
    print(f"  several languages: {len(multi)}  {dict(list(multi.items())[:10])}")

    print("\n-- Year rule --")
    for source, n in collections.Counter(b["year_source"] for b in books.values()).most_common():
        print(f"  year_source={str(source):22s} {pct(n)}")
    years = sorted(b["year"] for b in books.values() if b["year"] is not None)
    if years:
        print(f"  years: min {years[0]}, median {years[len(years) // 2]}, max {years[-1]}")
        for decade, n in sorted(collections.Counter(y // 10 * 10 for y in years).items()):
            print(f"    {decade}s {'#' * n} {n}")
    examples = [(i, b["original_publication"]) for i, b in books.items() if b["original_publication"]][:10]
    if examples:
        print(f"  'Original publication' examples: {examples}")
    odd = [(i, b["release_date"]) for i, b in books.items() if b["release_date"] and year_of(b["release_date"]) is None]
    if odd:
        print(f"  'Release date' without a 4-digit year: {odd}")


def write_csv(books, path):
    columns = ["book_id", "title", "author", "language", "release_date", "original_publication", "year", "year_source"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for book_id, b in books.items():
            writer.writerow([book_id] + [b[c] for c in columns[1:]])
    print(f"\nPer-book detail: {path}")


def main():
    repo = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--ids", type=Path, default=repo.parent.parent / "Stage_1" / "shared" / "book_ids.txt",
                        help="file with one Gutenberg id per line (default: Stage 1's shared/book_ids.txt)")
    parser.add_argument("--cache", type=Path, default=repo / "data" / "headers",
                        help="where the downloaded headers are kept (default: data/headers, ignored by Git)")
    parser.add_argument("--csv", type=Path, default=None, help="per-book CSV (default: <cache>/survey.csv)")
    parser.add_argument("--delay", type=float, default=1.5, help="seconds between downloads (default: 1.5)")
    parser.add_argument("--timeout", type=float, default=20, help="seconds per request (default: 20)")
    parser.add_argument("--offline", action="store_true", help="do not download: use only the cache")
    args = parser.parse_args()

    ids = read_ids(args.ids)
    headers, failures = load_headers(ids, args.cache, args.offline, args.delay, args.timeout)
    books = {i: parse(headers[i]) for i in ids if i in headers}
    report(books, failures)
    write_csv(books, args.csv or args.cache / "survey.csv")


if __name__ == "__main__":
    main()
