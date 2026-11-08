#!/usr/bin/env python3
"""Documented failure tests of the coordination (CF-11, plan §10), on Docker Compose.

    python3 scripts/chaos/chaos.py all                  the four scenarios, one after the other
    python3 scripts/chaos/chaos.py kill-ingestion       one of them (kill-ingestion, kill-hazelcast-member,
                                                        kill-control, restart-hazelcast)
    python3 scripts/chaos/chaos.py all --books 400      fewer books: quicker, the same checks

Each scenario starts the system from scratch (empty cluster, datalake and database of its own),
seeds the books through POST /control/jobs, causes its failure halfway, waits for the system to
finish and checks the end state against the coordination contract: every book in its final
state exactly once, nothing FAILED, every book complete in the datalake (and in the index when
Indexing runs). It writes into scripts/chaos/results/<scenario>/:

    report.md            what was expected, what happened (with times) and the evidence: PASS or FAIL
    control-*.json       GET /control/status at the moments of the report
    status-*.json        GET /status of the services at the end
    timeline.csv         the books by state every 0.5 s
    logs/<service>.txt   docker compose logs of every service

Exit code 0 only if every check of every scenario passed. The settings are shorter than in
production so that a lost lease is requeued in seconds: LEASE_SECONDS=15, LEASE_CHECK_SECONDS=3,
PUBLISH_INTERVAL_S=5. Only the Python standard library, Docker with Compose and curl.
"""
import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
COMPOSE = os.environ.get("COMPOSE", "docker compose").split()
CONTROL = "http://localhost:7000"
DATA = os.path.join("data", "chaos")
DATALAKE = os.path.join(DATA, "datalake")
DATABASE = "chaos_test"
LEASE_SECONDS = 15
WORKERS = 4
PAST_INGESTION = ("DOWNLOADED", "INDEXING", "INDEXED", "PUBLISHED")
SETTINGS = {
    "INGESTION_SOURCE": "local",
    "DATALAKE_HOST_DIR": "./" + DATALAKE,
    "MONGO_DATABASE": DATABASE,
    "INGESTION_WORKERS": str(WORKERS),
    "LEASE_SECONDS": str(LEASE_SECONDS),
    "LEASE_CHECK_SECONDS": "3",
    "PUBLISH_BATCH": "100",
    "PUBLISH_INTERVAL_S": "5",
}


class ChaosFailure(Exception):
    """The system did not get where the scenario expected: the scenario is a FAIL."""


# ---- the system -------------------------------------------------------------------------

def run(command, check=True, capture=True):
    result = subprocess.run(command, cwd=ROOT, env={**os.environ, **SETTINGS}, check=False,
                            capture_output=capture, text=True)
    if check and result.returncode != 0:
        raise ChaosFailure(f"{' '.join(command)} failed: {(result.stderr or '').strip()[-500:]}")
    return result.stdout if capture else ""


def compose(*args, check=True):
    return run(COMPOSE + list(args), check=check)


def containers(service):
    return sorted(compose("ps", "-q", service).split())


def get_json(url, timeout=10):
    with urllib.request.urlopen(url, timeout=timeout) as answer:
        return json.load(answer)


def control_status():
    return get_json(f"{CONTROL}/control/status")


def service_status(container, port):
    out = run(["docker", "exec", container, "curl", "-s", "-m", "10", f"http://localhost:{port}/status"])
    return json.loads(out)


def mongo_count(collection):
    out = compose("exec", "-T", "mongo", "mongosh", "--quiet", DATABASE, "--eval",
                  f"db.{collection}.countDocuments()")
    return int(out.strip().splitlines()[-1])


def seed(ids):
    body = json.dumps({"book_ids": ids}).encode()
    request = urllib.request.Request(f"{CONTROL}/control/jobs", data=body, method="POST",
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=120) as answer:
        return json.load(answer)


def wipe_datalake():
    """The services write it as root on Linux: delete it from a container, as the benchmarks do."""
    os.makedirs(os.path.join(ROOT, DATALAKE), exist_ok=True)
    run(["docker", "run", "--rm", "-v", f"{os.path.join(ROOT, DATALAKE)}:/wipe", "--entrypoint", "sh",
         "nginx:" + os.environ.get("NGINX_VERSION", "1.28-alpine"), "-c", "rm -rf /wipe/* /wipe/.[!.]* 2>/dev/null; true"])


def dataset(count):
    """count synthetic books of DR-16 (seed 42), generated once."""
    folder = os.path.join(DATA, f"books-{count}")
    if not os.path.exists(os.path.join(ROOT, folder, "manifest.json")):
        print(f"Generating {count} synthetic books into {folder} (once)")
        run([sys.executable, "scripts/dataset/generate_dataset.py", "--count", str(count), "--out", folder])
    with open(os.path.join(ROOT, folder, "book_ids.txt"), encoding="utf-8") as lines:
        ids = [int(line) for line in (raw.strip() for raw in lines) if line.isdigit()]
    return folder, ids


def wait_for(what, predicate, timeout, every=0.5):
    """Calls predicate until it returns something truthy; ChaosFailure after timeout seconds."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except (urllib.error.URLError, OSError, ValueError, KeyError, ChaosFailure) as e:
            last = e                                    # a service restarting: try again
        time.sleep(every)
    raise ChaosFailure(f"timeout after {timeout} s waiting for {what}" + (f" (last error: {last})" if last else ""))


# ---- one scenario -----------------------------------------------------------------------

class Scenario:
    """The run of one scenario: its system, its events and its checks, and the report."""

    def __init__(self, name, title, expected, books, replicas=1, indexing=False, workers=WORKERS):
        self.name, self.title, self.expected = name, title, expected
        self.folder, self.ids = books
        self.replicas, self.indexing, self.workers = replicas, indexing, workers
        self.out = os.path.join(ROOT, "scripts", "chaos", "results", name)
        self.events, self.checks = [], []
        self.began = None
        self.timeline = None

    # -- system
    def start(self):
        shutil.rmtree(self.out, ignore_errors=True)
        os.makedirs(os.path.join(self.out, "logs"))
        SETTINGS["INGESTION_LOCAL_HOST_DIR"] = "./" + self.folder
        SETTINGS["INGESTION_WORKERS"] = str(self.workers)
        compose("down")
        wipe_datalake()
        compose("up", "-d", "--wait", "mongo")
        compose("exec", "-T", "mongo", "mongosh", "--quiet", DATABASE, "--eval", "db.dropDatabase()")
        services = ["hz-1", "hz-2", "control", "ingestion"] + (["indexing"] if self.indexing else [])
        compose("up", "-d", "--wait", "--scale", f"ingestion={self.replicas}", *services)
        wait_for("the Control and every replica connected, book-state empty", self.ready, 180)
        self.event(f"system up: {self.replicas} Ingestion replica(s) with {self.workers} worker(s)"
                   + (", Indexing" if self.indexing else "") + f"; {len(self.ids)} books")

    def ready(self):
        control = control_status()
        if control["coordination"] != "connected" or control["books"]["total"] != 0:
            return False
        replicas = [service_status(c, 7001) for c in containers("ingestion")]
        if len(replicas) != self.replicas or any(r.get("coordination") != "connected" for r in replicas):
            return False
        if self.indexing:
            indexing = [service_status(c, 7002) for c in containers("indexing")]
            if not indexing or indexing[0].get("coordination") != "connected":
                return False
        return True

    def seed_all(self):
        self.began = time.monotonic()
        answer = seed(self.ids)
        self.event(f"POST /control/jobs: {answer['added']} added, {answer['already_known']} already known")
        return answer

    def seed_part(self, ids, what):
        if self.began is None:
            self.began = time.monotonic()
        answer = seed(ids)
        self.event(f"POST /control/jobs, {what}: {answer['added']} added, {answer['already_known']} already known")
        return answer

    def check_caught_halfway(self, books, what):
        """The failure must hit books still in flight: with the local source a run can end in
        seconds, and a failure after the end would pass every check without testing anything."""
        pending = len(self.ids) - self.past_ingestion(books)
        self.check(f"the failure happened with books still to {what}", pending > 0,
                   f"{pending} books not yet past ingestion right after it ({describe(books)})")

    def seed_all_again(self):
        answer = seed(self.ids)
        self.event(f"POST /control/jobs again (contract §7): {answer['added']} added, "
                   f"{answer['already_known']} already known")
        return answer

    # -- observation
    def books(self):
        books = control_status()["books"]
        if self.timeline is not None:
            try:
                self.timeline.writerow([f"{self.elapsed():.1f}"] + [books.get(s, 0) for s in TIMELINE_STATES])
            except ValueError:                          # the file of the timeline is already closed
                self.timeline = None
        return books

    def past_ingestion(self, books=None):
        books = books or self.books()
        return sum(books.get(state, 0) for state in PAST_INGESTION)

    def wait_until(self, what, count_of, at_least, timeout):
        def reached():
            books = self.books()
            if books.get("FAILED", 0):
                raise ChaosFailure(f"{books['FAILED']} books FAILED while waiting for {what}")
            return books if count_of(books) >= at_least else None
        books = wait_for(what, reached, timeout, every=0.2)
        self.event(f"{what}: {describe(books)}")
        return books

    def snapshot(self, name):
        status = control_status()
        save(os.path.join(self.out, f"control-{name}.json"), status)
        return status

    def elapsed(self):
        return 0.0 if self.began is None else time.monotonic() - self.began

    def event(self, text):
        stamp = f"t = {self.elapsed():6.1f} s" if self.began is not None else "before"
        self.events.append((stamp, text))
        print(f"  [{stamp}] {text}")

    def check(self, name, ok, detail):
        self.checks.append((name, bool(ok), detail))
        print(f"  {'PASS' if ok else 'FAIL'}  {name}: {detail}")

    # -- the end state every scenario must reach
    def check_end_state(self, final_state):
        status = self.snapshot("end")
        books = status["books"]
        n = len(self.ids)
        done = books.get("PUBLISHED", 0) if final_state == "PUBLISHED" else self.past_ingestion(books)
        self.check(f"every book {final_state}", done == n, f"{done} of {n} ({describe(books)})")
        self.check("no book FAILED", books.get("FAILED", 0) == 0, f"FAILED = {books.get('FAILED', 0)}")
        self.check("book-state has each book once", books["total"] == n, f"total = {books['total']}, books = {n}")
        complete, partial = datalake_books(set(self.ids))
        self.check("every book complete in the datalake", complete == n,
                   f"{complete} of {n} with header, body and _READY; {partial} without _READY")
        if final_state == "PUBLISHED":
            for collection in ("index_log", "index_books"):
                count = mongo_count(collection)
                self.check(f"{collection} has each book once", count == n, f"{count} documents, books = {n}")

    def ingestion_counters(self, moment="end"):
        """The counters of the workers of every running replica, added up; their statuses saved."""
        totals = {}
        for i, container in enumerate(containers("ingestion"), start=1):
            status = service_status(container, 7001)
            save(os.path.join(self.out, f"status-ingestion-{i}-{moment}.json"), status)
            for key, value in status.get("workers", {}).items():
                if isinstance(value, int):
                    totals[key] = totals.get(key, 0) + value
        return totals

    def finish(self, error=None):
        os.makedirs(os.path.join(self.out, "logs"), exist_ok=True)
        if error:
            self.check("the scenario ran to the end", False, str(error))
        for service in ("hz-1", "hz-2", "control", "ingestion", "indexing", "mongo"):
            with open(os.path.join(self.out, "logs", f"{service}.txt"), "w") as file:
                file.write(compose("logs", "--no-color", "--timestamps", service, check=False))
        passed = all(ok for _, ok, _ in self.checks)
        with open(os.path.join(self.out, "report.md"), "w") as report:
            report.write(f"# {self.title}: {'PASS' if passed else 'FAIL'}\n\n")
            report.write(f"Run {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}, commit "
                         f"{run(['git', 'rev-parse', '--short', 'HEAD']).strip()}, {len(self.ids)} synthetic books "
                         f"({self.folder}), {self.replicas} Ingestion replica(s) with {self.workers} worker(s)"
                         f"{', Indexing' if self.indexing else ''}. LEASE_SECONDS={LEASE_SECONDS}, "
                         f"LEASE_CHECK_SECONDS=3, PUBLISH_INTERVAL_S=5.\n\n")
            report.write(f"## Expected\n\n{self.expected}\n\n## What happened\n\n")
            report.write("t = 0 is the POST /control/jobs.\n\n")
            for stamp, text in self.events:
                report.write(f"- `{stamp}` {text}\n")
            report.write("\n## Evidence\n\n| Check | Result | Detail |\n|---|---|---|\n")
            for name, ok, detail in self.checks:
                report.write(f"| {name} | {'PASS' if ok else '**FAIL**'} | {detail} |\n")
            report.write("\nFiles: `control-*.json` (GET /control/status), `status-*.json` (GET /status of the "
                         "services at the end), `timeline.csv` (books by state every 0.5 s), `logs/` (every "
                         "service, with timestamps).\n")
        print(f"  {'PASS' if passed else 'FAIL'}: {os.path.relpath(os.path.join(self.out, 'report.md'), ROOT)}")
        return passed


TIMELINE_STATES = ("PENDING", "INGESTING", "DOWNLOADED", "INDEXING", "INDEXED", "PUBLISHED", "FAILED", "total")


def describe(books):
    return ", ".join(f"{state} {books[state]}" for state in TIMELINE_STATES[:-1] if books.get(state))


def save(path, value):
    with open(path, "w") as file:
        json.dump(value, file, indent=2)


def datalake_books(ids):
    """Books of the datalake with header, body and _READY (contract datalake-readiness §3), and
    the ones with some file but no marker, which do not exist."""
    complete = partial = 0
    root = os.path.join(ROOT, DATALAKE, "range")
    for book in ids:
        start = book // 1000 * 1000
        folder = os.path.join(root, f"{start:05d}-{start + 999:05d}")
        files = [os.path.exists(os.path.join(folder, f"{book}{suffix}")) for suffix in (".header.txt", ".body.txt", "._READY")]
        if all(files):
            complete += 1
        elif any(files):
            partial += 1
    return complete, partial


def killed(container):
    """SIGKILL: no shutdown hook runs. Without its restart policy first, so that Docker does not
    start it again by itself (restart: unless-stopped): the scenario starts it when it says."""
    run(["docker", "update", "--restart=no", container])
    run(["docker", "kill", container])


KILL_INGESTION_EXPECTED = """\
Two Ingestion replicas share `ingest-queue`. One is killed with SIGKILL (`docker kill`) while it is
working: its process dies without releasing anything. The books it had claimed stay `INGESTING`
with its lease; their files may be half written, without `_READY`, so they do not exist for anyone
(datalake-readiness §4). When the lease expires (LEASE_SECONDS, 15 s here) the Control requeues
them (T4) and the other replica downloads them again, whole. Nothing is lost, nothing ends
`FAILED`, and every book is in the datalake once (contract §7, "an Ingestion replica dies")."""

KILL_HAZELCAST_MEMBER_EXPECTED = """\
`book-state` and the queues have one backup on the other member (hazelcast.yaml, backup-count 1).
Member hz-1 is killed with SIGKILL during the ingestion. The clients (Control and Ingestion) carry
on with hz-2, which promotes its backups; a transaction caught by the death is rolled back or
committed whole (TWO_PHASE), so no book is left half moved. The Control does NOT rebuild anything:
the `initialized` marker survives with the rest. When hz-1 starts again it rejoins and the
partitions migrate back. Nothing is lost and nothing ends `FAILED` (contract §7, "one member dies")."""

KILL_CONTROL_EXPECTED = """\
The Control only seeds, checks leases and triggers publications; the workers do not need it to
consume the queues. It is killed with SIGKILL while Indexing is indexing. Meanwhile Ingestion
and Indexing keep working (their counters grow), but nothing new is published (`index_books`
does not grow) and no lease is checked. When it starts again it finds the `initialized` marker
(the cluster kept its state), so it rebuilds nothing, and it publishes the rest. Seeding is
idempotent. In the end every book is `PUBLISHED`, once (contract §7, "the Control dies")."""

RESTART_HAZELCAST_EXPECTED = """\
A first wave of books is seeded and gets published; then the rest is seeded and, while it is
being ingested, both members are killed at once and started again: `book-state`, the queues, the lock and the
`initialized` marker are lost (Hazelcast keeps nothing on disk). Every client reconnects by itself.
The Control finds no marker and rebuilds `book-state` from the persistent evidence (CF-10, contract
§7): `index_books` up to CURRENT → `PUBLISHED`, `index_log` → `INDEXED`, `<id>._READY` →
`DOWNLOADED` (offered again to `index-queue`). The books with no evidence yet (`PENDING`, or
`INGESTING` without `_READY`) are lost from the cluster, as the contract says, and are sent again
with `POST /control/jobs`, which leaves the recovered ones as they are. In the end every book is
`PUBLISHED`, once; a book with `_READY` is never downloaded again, and the index writer takes
`index-writer` again."""

# ---- the scenarios ----------------------------------------------------------------------

def kill_ingestion(s):
    s.start()
    with open(os.path.join(s.out, "timeline.csv"), "w", newline="") as file:
        s.timeline = csv.writer(file)
        s.timeline.writerow(["elapsed_s", *TIMELINE_STATES])
        s.seed_all()
        n = len(s.ids)
        before = s.wait_until("a twentieth of the books ingested", s.past_ingestion, n // 20, 300)
        victim = containers("ingestion")[0]
        killed(victim)
        after_kill = s.snapshot("after-kill")["books"]
        s.event(f"docker kill of replica {victim[:12]}: {before.get('INGESTING', 0)} books INGESTING in "
                f"both replicas just before; right after: {describe(after_kill)}")
        s.check_caught_halfway(after_kill, "ingest")
        books = s.wait_until("every book past ingestion", s.past_ingestion, n, LEASE_SECONDS + 300)
    requeued = control_status()["lease_check"]["requeued_or_failed"]
    s.event(f"the Control requeued {requeued} books with an expired lease")
    s.check("the lost books were requeued by their lease", requeued >= 1,
            f"lease_check.requeued_or_failed = {requeued}")
    s.check_end_state("DOWNLOADED")
    s.ingestion_counters()
    return s


def kill_hazelcast_member(s):
    s.start()
    runs_before = control_status()["recovery"]["runs"]
    with open(os.path.join(s.out, "timeline.csv"), "w", newline="") as file:
        s.timeline = csv.writer(file)
        s.timeline.writerow(["elapsed_s", *TIMELINE_STATES])
        s.seed_all()
        n = len(s.ids)
        s.wait_until("a twentieth of the books ingested", s.past_ingestion, n // 20, 300)
        killed("hz-1")
        after_kill = wait_for("an answer of the Control with hz-2 alone", lambda: s.snapshot("after-kill")["books"], 60)
        s.event(f"docker kill hz-1; right after, with hz-2 alone: {describe(after_kill)}")
        s.check_caught_halfway(after_kill, "ingest")
        s.wait_until("every book past ingestion, with hz-2 alone", s.past_ingestion, n, 600)
        compose("start", "hz-1")
        compose("up", "-d", "--wait", "hz-1")
        s.event("hz-1 started again and healthy")
        time.sleep(10)                                  # the partitions migrate back in 1-4 s
        s.books()
    runs_after = control_status()["recovery"]["runs"]
    s.check("the Control did not rebuild book-state", runs_after == runs_before,
            f"recovery.runs {runs_before} before, {runs_after} after")
    s.check_end_state("DOWNLOADED")
    s.ingestion_counters()
    return s


def kill_control(s):
    s.start()
    with open(os.path.join(s.out, "timeline.csv"), "w", newline="") as file:
        s.timeline = csv.writer(file)
        s.timeline.writerow(["elapsed_s", *TIMELINE_STATES])
        s.seed_all()
        n = len(s.ids)
        s.wait_until("the first publication", lambda b: b.get("PUBLISHED", 0), 1, 600)
        indexing = containers("indexing")[0]
        s.snapshot("before-kill")
        killed(containers("control")[0])
        s.timeline = None                               # the Control is down: nothing to poll
        time.sleep(2)                                   # a publication in flight (about 70 ms) ends first
        published_at_kill = mongo_count("index_books")
        indexed_at_kill = service_status(indexing, 7002)["writer"]["indexed"]
        s.event(f"docker kill of the Control: {published_at_kill} books in index_books, "
                f"{indexed_at_kill} indexed by the writer")
        time.sleep(20)
        published_later = mongo_count("index_books")
        indexed_later = service_status(indexing, 7002)["writer"]["indexed"]
        s.event(f"20 s later, without the Control: {published_later} books in index_books, "
                f"{indexed_later} indexed by the writer")
        s.check("Indexing kept working without the Control", indexed_later > indexed_at_kill or indexed_later >= n,
                f"writer.indexed {indexed_at_kill} -> {indexed_later}")
        s.check("nothing was published without the Control", published_later == published_at_kill,
                f"index_books {published_at_kill} -> {published_later}")
        compose("start", "control")
        compose("up", "-d", "--wait", "control")
        s.timeline = csv.writer(file)
        s.event("the Control started again and healthy")
        recovery = control_status()["recovery"]
        s.check("the new Control rebuilt nothing (marker found)", recovery["runs"] == 0,
                f"recovery.runs = {recovery['runs']}")
        s.wait_until("every book PUBLISHED", lambda b: b.get("PUBLISHED", 0), n, 900)
    s.check_end_state("PUBLISHED")
    s.ingestion_counters()
    return s


def restart_hazelcast(s):
    s.start()
    with open(os.path.join(s.out, "timeline.csv"), "w", newline="") as file:
        s.timeline = csv.writer(file)
        s.timeline.writerow(["elapsed_s", *TIMELINE_STATES])
        n = len(s.ids)
        first = s.ids[:n // 5]
        # The first wave gets published, so every kind of evidence exists; the second wave is still
        # being ingested when the cluster dies, so some books have no evidence at all.
        s.seed_part(first, f"first wave of {len(first)} books")
        s.wait_until("the first publication", lambda b: b.get("PUBLISHED", 0), 1, 900)
        s.seed_part(s.ids[len(first):], f"second wave of {n - len(first)} books")
        s.wait_until("the second wave being ingested", s.past_ingestion, len(first) + 10, 300)
        before = s.ingestion_counters("before-restart")
        lost_before = s.snapshot("before-restart")["books"]
        killed("hz-1")
        killed("hz-2")
        s.event(f"docker kill hz-1 hz-2, the whole cluster ({describe(lost_before)} just before)")
        s.check_caught_halfway(lost_before, "ingest")
        compose("start", "hz-1", "hz-2")
        compose("up", "-d", "--wait", "hz-1", "hz-2")
        s.event("hz-1 and hz-2 started again and healthy, empty")
        recovery = wait_for("the recovery of the Control",
                            lambda: (r := control_status()["recovery"])["runs"] >= 2 and r, 180)
        save(os.path.join(s.out, "control-after-recovery.json"), control_status())
        s.event(f"the Control rebuilt book-state in {recovery['millis']} ms: {recovery['published']} PUBLISHED, "
                f"{recovery['indexed']} INDEXED, {recovery['downloaded']} DOWNLOADED")
        evidence = recovery["published"] + recovery["indexed"] + recovery["downloaded"]
        answer = s.seed_all_again()
        s.check("POST /control/jobs added exactly the books without evidence, and there were some",
                0 < answer["added"] == n - evidence and answer["already_known"] == evidence,
                f"{answer['added']} added, {answer['already_known']} already known; evidence for {evidence}")
        s.wait_until("every book PUBLISHED", lambda b: b.get("PUBLISHED", 0), n, 1200)
    after = s.ingestion_counters()
    downloaded = after.get("downloaded", 0) - before.get("downloaded", 0)
    available = after.get("already_available", 0) - before.get("already_available", 0)
    s.check("after the restart only the books without evidence were ingested",
            downloaded + available == answer["added"],
            f"{downloaded} downloaded and {available} already in the datalake after the restart, "
            f"for {answer['added']} books sent again; the recovered DOWNLOADED went to index-queue")
    s.check_end_state("PUBLISHED")
    return s


SCENARIOS = {  # name: (body, title, expected, system)
    "kill-ingestion": (kill_ingestion, "Kill an Ingestion replica in the middle of its books", KILL_INGESTION_EXPECTED, dict(replicas=2, workers=1)),
    "kill-hazelcast-member": (kill_hazelcast_member, "Kill one Hazelcast member", KILL_HAZELCAST_MEMBER_EXPECTED, dict(workers=1)),
    "kill-control": (kill_control, "Kill the Control Module", KILL_CONTROL_EXPECTED, dict(indexing=True)),
    "restart-hazelcast": (restart_hazelcast, "Restart the whole Hazelcast cluster", RESTART_HAZELCAST_EXPECTED, dict(indexing=True, workers=1)),
}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("scenario", choices=["all", *SCENARIOS])
    parser.add_argument("--books", type=int, default=2000, help="synthetic books per scenario (default 2000)")
    parser.add_argument("--no-build", action="store_true", help="use the images already built")
    args = parser.parse_args()

    for tool in ("docker", "curl", "git"):
        if shutil.which(tool) is None:
            raise SystemExit(f"ERROR: {tool} not found. Requirements: Docker with Compose, curl, git and Python 3.")
    books = dataset(args.books)
    if not args.no_build:
        print("Building the images")
        compose("build", "control", "ingestion", "indexing")

    results = {}
    try:
        for name in (SCENARIOS if args.scenario == "all" else [args.scenario]):
            print(f"\n== {name}")
            body, title, expected, system = SCENARIOS[name]
            scenario = Scenario(name, title, expected, books, **system)
            try:
                body(scenario)
                results[name] = scenario.finish()
            except (ChaosFailure, subprocess.SubprocessError, OSError, KeyError, ValueError, TypeError) as e:
                print(f"  FAIL: {e}")
                results[name] = scenario.finish(e)
    finally:
        print("\nCleaning up: deleting the database and the datalake, stopping the system")
        compose("exec", "-T", "mongo", "mongosh", "--quiet", DATABASE, "--eval", "db.dropDatabase()", check=False)
        compose("down", check=False)
        try:
            wipe_datalake()
        except ChaosFailure:
            pass
    print()
    for name, passed in results.items():
        print(f"{'PASS' if passed else 'FAIL'}  {name}")
    sys.exit(0 if results and all(results.values()) else 1)

if __name__ == "__main__":
    main()
