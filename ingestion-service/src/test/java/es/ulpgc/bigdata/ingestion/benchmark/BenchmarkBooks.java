package es.ulpgc.bigdata.ingestion.benchmark;

import es.ulpgc.bigdata.ingestion.BookSplitter;
import es.ulpgc.bigdata.model.RawBook;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The books the Ingestion benchmarks measure (CF-13): the synthetic dataset of DR-16, which
 * benchmarks/ingestion/run-jmh.sh generates (200 books, seed 42, the same bytes as the Indexing
 * benchmarks) and checks by its digest.
 *
 * The books are INGESTION_BENCH_BOOKS, or benchmarks/ingestion/data/books from the repository
 * root (where run-jmh.sh runs): the raw pg&lt;id&gt;.txt files, as LocalFileSource reads them
 * (\r\n included). The datalakes the benchmarks write go under INGESTION_BENCH_WORK, or
 * benchmarks/ingestion/data/work: on the disk of the repository, not in /tmp, which may be
 * in memory.
 *
 * Run as a program, it prints the books each benchmark uses, with their size: run-jmh.sh keeps
 * that next to the results, to turn a time per book into MB/s.
 */
public final class BenchmarkBooks {

    public static final String DIR_VARIABLE = "INGESTION_BENCH_BOOKS";
    public static final String WORK_VARIABLE = "INGESTION_BENCH_WORK";
    public static final Path DEFAULT_DIR = Path.of("benchmarks", "ingestion", "data", "books");
    public static final Path DEFAULT_WORK = Path.of("benchmarks", "ingestion", "data", "work");

    /**
     * The books of a round: what the K workers of a replica share in each invocation of the
     * benchmarks with threads, so each thread has several books to take.
     */
    public static final int BATCH = 100;

    /** Percentile of the raw length of each named book. */
    private static final Map<String, Double> LENGTHS = Map.of("short", 0.10, "median", 0.50, "long", 0.90);

    private static final Pattern FILE = Pattern.compile("pg(\\d+)\\.txt");

    /** A raw file of the dataset, as the source returns it. */
    public record RawFile(int id, String text) {
    }

    private BenchmarkBooks() {
    }

    public static Path booksDir() {
        return fromEnv(DIR_VARIABLE, DEFAULT_DIR);
    }

    public static Path workDir() {
        return fromEnv(WORK_VARIABLE, DEFAULT_WORK);
    }

    private static Path fromEnv(String variable, Path fallback) {
        String value = System.getenv(variable);
        return value == null || value.isBlank() ? fallback : Path.of(value);
    }

    /** Every raw file of the folder, by id. */
    public static List<RawFile> load(Path dir) {
        if (!Files.isDirectory(dir)) {
            throw new IllegalStateException("no books folder " + dir.toAbsolutePath()
                    + ": run benchmarks/ingestion/run-jmh.sh, which generates it, or set " + DIR_VARIABLE);
        }
        List<RawFile> books = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path file : files.toList()) {
                Matcher name = FILE.matcher(file.getFileName().toString());
                if (name.matches()) {
                    books.add(new RawFile(Integer.parseInt(name.group(1)), Files.readString(file, StandardCharsets.UTF_8)));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (books.isEmpty()) {
            throw new IllegalStateException("no pg<id>.txt in " + dir.toAbsolutePath());
        }
        books.sort(Comparator.comparingInt(RawFile::id));
        return books;
    }

    /**
     * The book at a percentile of the raw length: "short" (10), "median" (50) or "long" (90).
     * Ties are broken by id, so it is always the same book.
     */
    public static RawFile byLength(List<RawFile> books, String which) {
        Double percentile = LENGTHS.get(which);
        if (percentile == null) {
            throw new IllegalArgumentException("unknown book: " + which + " (short, median or long)");
        }
        List<RawFile> sorted = new ArrayList<>(books);
        sorted.sort(Comparator.comparingInt((RawFile b) -> b.text().length()).thenComparingInt(RawFile::id));
        return sorted.get((int) Math.floor(percentile * (sorted.size() - 1)));
    }

    /** The first BATCH books, by id: the round of the benchmarks with threads. */
    public static List<RawFile> batch(List<RawFile> books) {
        if (books.size() < BATCH) {
            throw new IllegalStateException("at least " + BATCH + " books are needed, there are " + books.size());
        }
        return books.subList(0, BATCH);
    }

    /** Header and body, as Ingestion stores them. */
    public static RawBook split(RawFile file) {
        return new BookSplitter().split(file.id(), file.text())
                .orElseThrow(() -> new IllegalStateException("pg" + file.id() + ".txt has no START/END markers"));
    }

    /**
     * Runs task on every item, on the pool, and waits for all of them: the threads take the
     * items as the workers of a replica take books from ingest-queue.
     *
     * @return how many tasks returned true
     */
    public static <T> int onPool(ExecutorService pool, List<T> items, Check<T> task) throws Exception {
        List<Callable<Boolean>> calls = new ArrayList<>(items.size());
        for (T item : items) {
            calls.add(() -> task.run(item));
        }
        int ok = 0;
        for (Future<Boolean> done : pool.invokeAll(calls)) {
            try {
                if (done.get()) {
                    ok++;
                }
            } catch (ExecutionException e) {
                throw e.getCause() instanceof Exception cause ? cause : e;
            }
        }
        return ok;
    }

    @FunctionalInterface
    public interface Check<T> {
        boolean run(T item) throws Exception;
    }

    /** A new, empty folder for one datalake, under the work folder. */
    public static Path newDatalakeDir(Path work) throws IOException {
        Files.createDirectories(work);
        return Files.createTempDirectory(work, "datalake-");
    }

    /** Deletes a folder the benchmark created, with everything in it. */
    public static void delete(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(dir)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    /** Prints the books each benchmark uses, as CSV. */
    public static void main(String[] args) {
        List<RawFile> books = load(booksDir());
        System.out.println("role,book_id,raw_bytes,header_bytes,body_bytes");
        for (String which : List.of("short", "median", "long")) {
            RawFile book = byLength(books, which);
            System.out.printf("%s,%d,%s%n", which, book.id(), sizes(List.of(book)));
        }
        List<RawFile> batch = batch(books);
        System.out.printf("batch_of_%d,%d-%d,%s%n", BATCH, batch.get(0).id(), batch.get(BATCH - 1).id(), sizes(batch));
        System.out.printf("all_%d_books,,%s%n", books.size(), sizes(books));
    }

    private static String sizes(List<RawFile> books) {
        long raw = 0;
        long header = 0;
        long body = 0;
        for (RawFile file : books) {
            RawBook book = split(file);
            raw += utf8(file.text());
            header += utf8(book.header());
            body += utf8(book.body());
        }
        return raw + "," + header + "," + body;
    }

    private static long utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8).length;
    }
}
