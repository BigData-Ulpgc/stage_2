package es.ulpgc.bigdata.indexing.benchmark;

import com.hazelcast.core.HazelcastInstance;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.config.MongoSettings;
import es.ulpgc.bigdata.coordination.BookState;
import es.ulpgc.bigdata.coordination.Coordination;
import es.ulpgc.bigdata.coordination.CoordinationSettings;
import es.ulpgc.bigdata.datamart.indexlog.MongoIndexLog;
import es.ulpgc.bigdata.datamart.metadata.MongoMetadataRepository;
import es.ulpgc.bigdata.indexing.MongoPublicationRepository;
import org.bson.Document;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.OptionalDouble;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

/**
 * The driver of the indexing experiments (DR-13). benchmarks/indexing/run-experiments.sh runs it
 * from the host against the stack of docker compose; it does what only a Hazelcast or MongoDB
 * client can do:
 *
 * <pre>
 *   seed FIRST COUNT                       T1 for the ids FIRST .. FIRST+COUNT-1, so that Ingestion
 *                                          takes them to index-queue (the books are already in the
 *                                          datalake, so it only checks them)
 *   wait STATES COUNT SECONDS              until COUNT books are in STATES (one, or several
 *                                          separated by commas, added up)
 *   measure COUNT SECONDS OUT -- COMMAND   runs COMMAND (which starts Indexing) and polls every
 *                                          second until COUNT books are INDEXED (or FAILED); the
 *                                          samples go to OUT and one summary line to stdout
 *   stats OUT                              sizes of the collections and the longest posting lists,
 *                                          with the time to read each one
 *   probe OUT                              the time of adding 100 ids to one posting list of 1,000,
 *                                          10,000 and 100,000 ids, with $addToSet (what the index
 *                                          does) and with $push, in a collection of its own
 * </pre>
 *
 * Hazelcast comes from HZ_MEMBERS and HZ_CLUSTER_NAME, MongoDB from MONGO_URI, MONGO_DATABASE and
 * MONGO_COLLECTION, as in the services. Messages go to stderr; exit status 1 on a timeout or a
 * wrong argument.
 */
public final class IndexingExperiment {

    /** The columns of the line {@code measure} prints. */
    public static final String SUMMARY_HEADER =
            "books,failed,first_book_s,total_s,books_per_s,steady_books_per_s";

    /** Reads of each long posting list in {@code stats}, after {@link #WARMUP_READS} not counted. */
    static final int READS = 20;
    static final int WARMUP_READS = 3;
    /** How many of the longest posting lists {@code stats} reports. */
    static final int LONGEST = 5;

    private static final Duration POLL = Duration.ofSeconds(1);

    private IndexingExperiment() {
    }

    public static void main(String[] args) throws Exception {
        // The client logs every connection step at INFO; this driver reports what matters itself.
        if (System.getProperty("hazelcast.logging.type") == null) {
            System.setProperty("hazelcast.logging.type", "none");
        }
        try {
            System.exit(run(args, Env.system(), System.out));
        } catch (IllegalArgumentException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.err.println("usage: seed FIRST COUNT | wait STATES COUNT SECONDS"
                    + " | measure COUNT SECONDS OUT -- COMMAND... | stats OUT | probe OUT");
            System.exit(1);
        }
    }

    static int run(String[] args, Env env, PrintStream out) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("no command");
        }
        String[] rest = Arrays.copyOfRange(args, 1, args.length);
        switch (args[0]) {
            case "seed" -> {
                need(rest, 2);
                try (Connection c = Connection.hazelcast(env)) {
                    out.println(seed(c.coordination(), positive(rest[0]), positive(rest[1])));
                }
                return 0;
            }
            case "wait" -> {
                need(rest, 3);
                List<BookState> states = states(rest[0]);
                try (Connection c = Connection.hazelcast(env)) {
                    return waitFor(c.coordination(), states, positive(rest[1]),
                            Duration.ofSeconds(positive(rest[2])), POLL) ? 0 : 1;
                }
            }
            case "measure" -> {
                int separator = Arrays.asList(rest).indexOf("--");
                if (separator != 3 || rest.length == 4) {
                    throw new IllegalArgumentException("measure COUNT SECONDS OUT -- COMMAND...");
                }
                List<String> command = List.of(rest).subList(4, rest.length);
                try (Connection c = Connection.hazelcast(env);
                     Timeline timeline = new Timeline(Path.of(rest[2]))) {
                    Coordination coordination = c.coordination();
                    Measurement m = measure(() -> coordination.count(BookState.INDEXED),
                            () -> coordination.count(BookState.FAILED), positive(rest[0]),
                            Duration.ofSeconds(positive(rest[1])), POLL, () -> start(command), timeline);
                    out.println(m.summary());
                    return m.complete() ? 0 : 1;
                }
            }
            case "stats" -> {
                need(rest, 1);
                MongoSettings settings = MongoSettings.fromEnv(env);
                try (MongoClient client = settings.openClient()) {
                    fsync(client);
                    List<String> rows = stats(client.getDatabase(settings.database()), settings.collection());
                    Files.write(Path.of(rest[0]), rows, StandardCharsets.UTF_8);
                }
                return 0;
            }
            case "probe" -> {
                need(rest, 1);
                MongoSettings settings = MongoSettings.fromEnv(env);
                try (MongoClient client = settings.openClient()) {
                    List<String> rows = probe(client.getDatabase(settings.database()), PROBE_LENGTHS);
                    Files.write(Path.of(rest[0]), rows, StandardCharsets.UTF_8);
                    rows.stream().skip(1).forEach(row -> System.err.println("  " + row));
                }
                return 0;
            }
            default -> throw new IllegalArgumentException("unknown command: " + args[0]);
        }
    }

    // --- seed and wait -------------------------------------------------------------------

    /** @return how many of the ids were new to book-state */
    static int seed(Coordination coordination, int first, int count) {
        return coordination.seed(IntStream.range(first, first + count).boxed().toList());
    }

    /**
     * @param states added up: after a publication its books go from INDEXED to PUBLISHED in the
     *               background, so "INDEXED,PUBLISHED" counts every indexed book at any moment
     * @return false if the time ran out first
     */
    static boolean waitFor(Coordination coordination, List<BookState> states, long count, Duration timeout,
                           Duration poll) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        long seen;
        while ((seen = states.stream().mapToLong(coordination::count).sum()) < count) {
            if (System.nanoTime() > deadline) {
                System.err.printf("ERROR: %d of %d books in %s after %d s%n", seen, count, states, timeout.toSeconds());
                return false;
            }
            Thread.sleep(poll.toMillis());
        }
        return true;
    }

    static List<BookState> states(String text) {
        List<BookState> states = new ArrayList<>();
        for (String name : text.split(",")) {
            try {
                states.add(BookState.valueOf(name.trim()));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("not a book state: " + name + " (one of "
                        + Arrays.toString(BookState.values()) + ")");
            }
        }
        return states;
    }

    // --- measure -------------------------------------------------------------------------

    /** What {@code measure} found: the samples, and the books failed at the end. */
    record Measurement(long target, Progress progress, long failed, boolean complete) {

        double totalSeconds() {
            List<Progress.Sample> s = progress.samples();
            return s.isEmpty() ? 0 : s.get(s.size() - 1).seconds();
        }

        long indexed() {
            List<Progress.Sample> s = progress.samples();
            return s.isEmpty() ? 0 : s.get(s.size() - 1).books();
        }

        /** One line with the columns of {@link #SUMMARY_HEADER}; an empty value if it was not reached. */
        String summary() {
            double total = totalSeconds();
            return String.join(",",
                    Long.toString(indexed()),
                    Long.toString(failed),
                    format(progress.secondsToReach(1)),
                    format(OptionalDouble.of(total)),
                    format(total > 0 ? OptionalDouble.of(indexed() / total) : OptionalDouble.empty()),
                    format(progress.rate(target, 0.1, 0.9)));
        }
    }

    /**
     * Starts the run and polls until {@code target} books are indexed or failed, or the time runs
     * out. Time 0 is the moment just before {@code start}, so the start-up of the service counts.
     */
    static Measurement measure(LongSupplier indexed, LongSupplier failed, long target, Duration timeout,
                               Duration poll, Runnable start, Consumer<Progress.Sample> each)
            throws InterruptedException {
        Progress progress = new Progress();
        long t0 = System.nanoTime();
        start.run();
        long deadline = t0 + timeout.toNanos();
        while (true) {
            long now = System.nanoTime();
            long books = indexed.getAsLong();
            long lost = failed.getAsLong();
            progress.add((now - t0) / 1e9, books);
            each.accept(progress.samples().get(progress.samples().size() - 1));
            if (books + lost >= target) {
                if (lost > 0) {
                    System.err.printf("WARNING: %d books FAILED%n", lost);
                }
                return new Measurement(target, progress, lost, true);
            }
            if (now > deadline) {
                System.err.printf("ERROR: %d of %d books indexed after %d s%n", books, target, timeout.toSeconds());
                return new Measurement(target, progress, lost, false);
            }
            Thread.sleep(poll.toMillis());
        }
    }

    /** Runs the command that starts Indexing and waits for it (docker compose up -d returns at once). */
    private static void start(List<String> command) {
        try {
            Process p = new ProcessBuilder(command)
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .redirectError(ProcessBuilder.Redirect.INHERIT)
                    .start();
            int status = p.waitFor();
            if (status != 0) {
                throw new IllegalStateException("the command that starts Indexing failed (exit " + status + "): " + command);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
    }

    /**
     * The samples as CSV, each one written and flushed when it is taken: a long run that is
     * stopped keeps what it measured, and the file can be watched while it runs.
     */
    static final class Timeline implements Consumer<Progress.Sample>, AutoCloseable {

        static final String HEADER = "seconds,books_indexed";

        private final BufferedWriter writer;

        Timeline(Path file) throws IOException {
            writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8);
            writer.write(HEADER);
            writer.newLine();
            writer.flush();
        }

        @Override
        public void accept(Progress.Sample s) {
            try {
                writer.write(String.format(Locale.ROOT, "%.3f,%d", s.seconds(), s.books()));
                writer.newLine();
                writer.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    private static String format(OptionalDouble value) {
        return value.isPresent() ? String.format(Locale.ROOT, "%.3f", value.getAsDouble()) : "";
    }

    // --- stats ---------------------------------------------------------------------------

    /**
     * WiredTiger moves the data to the collection files at each checkpoint (about 60 s), so the
     * sizes are only exact after an fsync. A MongoDB without permission for it gives sizes that
     * may lag one checkpoint.
     */
    private static void fsync(MongoClient client) {
        try {
            client.getDatabase("admin").runCommand(new Document("fsync", 1));
        } catch (RuntimeException e) {
            System.err.println("WARNING: fsync refused, the sizes may lag one checkpoint: " + e.getMessage());
        }
    }

    /**
     * CSV rows (with a header): for the collection of the index that CURRENT names (or
     * {@code defaultCollection}) and for books, index_log and index_books, the documents and the
     * bytes of data, storage and indexes; for the whole index the terms and the mean postings per
     * term; for the {@link #LONGEST} longest posting lists, their length, their size in bytes and
     * the median and maximum time to read each one ({@link #READS} finds by term).
     */
    static List<String> stats(MongoDatabase db, String defaultCollection) {
        String index = currentCollection(db, defaultCollection);
        List<String> rows = new ArrayList<>();
        rows.add("metric,collection,term,value,unit");
        for (String name : List.of(index, MongoMetadataRepository.DEFAULT_COLLECTION,
                MongoIndexLog.DEFAULT_COLLECTION, MongoPublicationRepository.INDEX_BOOKS)) {
            Document s = storage(db, name);
            rows.add(row("documents", name, "", number(s, "count"), "documents"));
            rows.add(row("data_size", name, "", number(s, "size"), "bytes"));
            rows.add(row("storage_size", name, "", number(s, "storageSize"), "bytes"));
            rows.add(row("index_size", name, "", number(s, "totalIndexSize"), "bytes"));
        }
        MongoCollection<Document> terms = db.getCollection(index);
        Document totals = terms.aggregate(List.of(
                new Document("$group", new Document("_id", null)
                        .append("terms", new Document("$sum", 1))
                        .append("postings", new Document("$sum", new Document("$size", "$postings")))))).first();
        long termCount = totals == null ? 0 : number(totals, "terms");
        long postingCount = totals == null ? 0 : number(totals, "postings");
        rows.add(row("terms", index, "", termCount, "terms"));
        rows.add(row("postings", index, "", postingCount, "ids"));
        rows.add(String.format(Locale.ROOT, "mean_postings,%s,,%.3f,ids/term", index,
                termCount == 0 ? 0.0 : (double) postingCount / termCount));
        List<Document> longest = terms.aggregate(List.of(
                new Document("$project", new Document("term", 1)
                        .append("length", new Document("$size", "$postings"))
                        .append("bytes", new Document("$bsonSize", "$$ROOT"))),
                new Document("$sort", new Document("length", -1).append("term", 1)),
                new Document("$limit", LONGEST))).into(new ArrayList<>());
        for (Document d : longest) {
            String term = d.getString("term");
            rows.add(row("posting_length", index, term, number(d, "length"), "ids"));
            rows.add(row("posting_bytes", index, term, number(d, "bytes"), "bytes"));
            double[] millis = readTimes(terms, term);
            rows.add(String.format(Locale.ROOT, "read_median,%s,%s,%.3f,ms", index, term, median(millis)));
            rows.add(String.format(Locale.ROOT, "read_max,%s,%s,%.3f,ms", index, term,
                    Arrays.stream(millis).max().orElse(0)));
        }
        return rows;
    }

    static String currentCollection(MongoDatabase db, String defaultCollection) {
        Document current = db.getCollection(MongoPublicationRepository.INDEX_META)
                .find(Filters.eq("_id", MongoPublicationRepository.CURRENT)).first();
        String named = current == null ? null : current.getString("collection");
        return named == null || named.isBlank() ? defaultCollection : named;
    }

    private static Document storage(MongoDatabase db, String name) {
        if (!db.listCollectionNames().into(new ArrayList<>()).contains(name)) {
            return new Document();
        }
        Document stats = db.getCollection(name).aggregate(List.of(
                new Document("$collStats", new Document("storageStats", new Document())))).first();
        return stats == null ? new Document() : stats.get("storageStats", Document.class);
    }

    /** The time of each of {@link #READS} reads of the whole document of a term, in ms. */
    private static double[] readTimes(MongoCollection<Document> terms, String term) {
        for (int i = 0; i < WARMUP_READS; i++) {
            terms.find(Filters.eq("term", term)).first();
        }
        double[] millis = new double[READS];
        for (int i = 0; i < READS; i++) {
            long start = System.nanoTime();
            Document d = terms.find(Filters.eq("term", term)).first();
            millis[i] = (System.nanoTime() - start) / 1e6;
            if (d == null) {
                throw new IllegalStateException("the term " + term + " disappeared while it was measured");
            }
        }
        return millis;
    }

    static double median(double[] values) {
        double[] sorted = values.clone();
        Arrays.sort(sorted);
        int n = sorted.length;
        if (n == 0) {
            return 0;
        }
        return n % 2 == 1 ? sorted[n / 2] : (sorted[n / 2 - 1] + sorted[n / 2]) / 2;
    }

    private static String row(String metric, String collection, String term, long value, String unit) {
        return String.join(",", metric, collection, term, Long.toString(value), unit);
    }

    private static long number(Document d, String key) {
        Object v = d.get(key);
        return v instanceof Number n ? n.longValue() : 0;
    }

    // --- probe ---------------------------------------------------------------------------

    static final List<Integer> PROBE_LENGTHS = List.of(1_000, 10_000, 100_000);
    /** The ids one update adds: those of one term in a batch of INDEX_WRITE_BATCH books. */
    static final int PROBE_NEW_IDS = 100;
    static final String PROBE_COLLECTION = "dr13_probe";

    /**
     * Why indexing slows down as the index grows, measured on its own: one document with a list
     * of {@code length} ids, and {@link #READS} updates that add {@link #PROBE_NEW_IDS} new ids
     * each, after {@link #WARMUP_READS} not counted. $addToSet compares every new id with the
     * whole list so as not to repeat one, so its time grows with the list; $push only appends.
     * Each operation starts from a fresh document of the same length.
     *
     * @return CSV rows with a header: operation, list length, median and maximum ms
     */
    static List<String> probe(MongoDatabase db, List<Integer> lengths) {
        MongoCollection<Document> probe = db.getCollection(PROBE_COLLECTION);
        List<String> rows = new ArrayList<>();
        rows.add("operation,list_length,median_ms,max_ms");
        try {
            for (int length : lengths) {
                for (String operation : List.of("$addToSet", "$push")) {
                    double[] millis = updateTimes(probe, operation, length);
                    rows.add(String.format(Locale.ROOT, "%s,%d,%.3f,%.3f", operation.substring(1), length,
                            median(millis), Arrays.stream(millis).max().orElse(0)));
                }
            }
        } finally {
            probe.drop();
        }
        return rows;
    }

    private static double[] updateTimes(MongoCollection<Document> probe, String operation, int length) {
        probe.drop();
        probe.insertOne(new Document("_id", 1).append("postings", IntStream.range(0, length).boxed().toList()));
        int next = length;
        double[] millis = new double[READS];
        for (int i = -WARMUP_READS; i < READS; i++) {
            List<Integer> ids = IntStream.range(next, next + PROBE_NEW_IDS).boxed().toList();
            next += PROBE_NEW_IDS;
            Document update = new Document(operation, new Document("postings", new Document("$each", ids)));
            long start = System.nanoTime();
            probe.updateOne(Filters.eq("_id", 1), update);
            if (i >= 0) {
                millis[i] = (System.nanoTime() - start) / 1e6;
            }
        }
        return millis;
    }

    // --- arguments and connections -------------------------------------------------------

    private static void need(String[] args, int n) {
        if (args.length != n) {
            throw new IllegalArgumentException("expected " + n + " arguments, got " + args.length);
        }
    }

    private static int positive(String text) {
        try {
            int n = Integer.parseInt(text);
            if (n > 0) {
                return n;
            }
        } catch (NumberFormatException e) {
            // reported below
        }
        throw new IllegalArgumentException("not a positive number: " + text);
    }

    /** A Hazelcast client and its Coordination, closed together. */
    private record Connection(HazelcastInstance client, Coordination coordination) implements AutoCloseable {

        static Connection hazelcast(Env env) {
            CoordinationSettings settings = CoordinationSettings.fromEnv(env);
            HazelcastInstance client = settings.openClient();
            return new Connection(client, Coordination.on(client, settings, Clock.systemUTC()));
        }

        @Override
        public void close() {
            client.shutdown();
        }
    }
}
