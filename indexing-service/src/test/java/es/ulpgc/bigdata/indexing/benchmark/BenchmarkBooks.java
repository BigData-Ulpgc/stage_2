package es.ulpgc.bigdata.indexing.benchmark;

import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.datamart.index.Tokenizer;
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
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The books the Indexing benchmarks measure (DR-12): the synthetic dataset of DR-16, which
 * benchmarks/indexing/run-jmh.sh generates (200 books, seed 42) and checks by its digest, so
 * every member of the group measures the same bytes.
 *
 * The folder is INDEXING_BENCH_BOOKS, or benchmarks/indexing/data/books from the repository root
 * (where run-jmh.sh runs). Each pg&lt;id&gt;.txt is split into header and body with the markers of
 * BookSplitter (Ingestion), as Ingestion would store it in the datalake.
 *
 * Run as a program, it prints the books each benchmark uses, with their size: run-jmh.sh keeps
 * that next to the results, to turn a time per book into MB/s or words/s.
 */
public final class BenchmarkBooks {

    public static final String DIR_VARIABLE = "INDEXING_BENCH_BOOKS";
    public static final Path DEFAULT_DIR = Path.of("benchmarks", "indexing", "data", "books");

    /** The books of a batch, as the index-queue writer takes them by default (INDEX_WRITE_BATCH). */
    public static final int BATCH = 100;

    /** Percentile of the body length of each named book. */
    private static final Map<String, Double> LENGTHS = Map.of("short", 0.10, "median", 0.50, "long", 0.90);

    // The markers of BookSplitter (ingestion-service), which Indexing does not depend on
    private static final Pattern START = Pattern.compile("\\*\\*\\* START OF (THE|THIS) PROJECT GUTENBERG EBOOK.*");
    private static final Pattern END = Pattern.compile("\\*\\*\\* END OF (THE|THIS) PROJECT GUTENBERG EBOOK.*");
    private static final Pattern FILE = Pattern.compile("pg(\\d+)\\.txt");

    private BenchmarkBooks() {
    }

    /** Every book of the folder, by id. */
    public static List<RawBook> load() {
        String dir = System.getenv(DIR_VARIABLE);
        return load(dir == null || dir.isBlank() ? DEFAULT_DIR : Path.of(dir));
    }

    static List<RawBook> load(Path dir) {
        if (!Files.isDirectory(dir)) {
            throw new IllegalStateException("no existe la carpeta de libros " + dir.toAbsolutePath()
                    + ": ejecuta benchmarks/indexing/run-jmh.sh, que la genera, o define " + DIR_VARIABLE);
        }
        List<RawBook> books = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path file : files.toList()) {
                Matcher name = FILE.matcher(file.getFileName().toString());
                if (name.matches()) {
                    int id = Integer.parseInt(name.group(1));
                    books.add(split(id, Files.readString(file, StandardCharsets.UTF_8))
                            .orElseThrow(() -> new IllegalStateException(file + " no tiene las marcas START/END")));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (books.isEmpty()) {
            throw new IllegalStateException("no hay ningún pg<id>.txt en " + dir.toAbsolutePath());
        }
        books.sort(Comparator.comparingInt(RawBook::id));
        return books;
    }

    /** Header and body, as BookSplitter splits them: line ends to \n, trimmed, footer dropped. */
    static Optional<RawBook> split(int id, String raw) {
        String text = raw.replace("\r\n", "\n");
        Matcher start = START.matcher(text);
        if (!start.find()) {
            return Optional.empty();
        }
        Matcher end = END.matcher(text);
        int startLineEnd = text.indexOf('\n', start.end());
        if (!end.find(start.end()) || startLineEnd == -1 || startLineEnd > end.start()) {
            return Optional.empty();
        }
        return Optional.of(new RawBook(id, text.substring(0, start.start()).trim(),
                text.substring(startLineEnd, end.start()).trim()));
    }

    /**
     * The book at a percentile of the body length: "short" (10), "median" (50) or "long" (90).
     * Ties are broken by id, so it is always the same book.
     */
    public static RawBook byLength(List<RawBook> books, String which) {
        Double percentile = LENGTHS.get(which);
        if (percentile == null) {
            throw new IllegalArgumentException("libro desconocido: " + which + " (short, median o long)");
        }
        List<RawBook> sorted = new ArrayList<>(books);
        sorted.sort(Comparator.comparingInt((RawBook b) -> b.body().length()).thenComparingInt(RawBook::id));
        return sorted.get((int) Math.floor(percentile * (sorted.size() - 1)));
    }

    /** The first BATCH books, by id: the batch of the batch benchmarks. */
    public static List<RawBook> batch(List<RawBook> books) {
        if (books.size() < BATCH) {
            throw new IllegalStateException("hacen falta al menos " + BATCH + " libros, hay " + books.size());
        }
        return books.subList(0, BATCH);
    }

    /** A datalake in dir with these books, complete (ready marker included). */
    public static RangeBasedDatalake datalake(Path dir, List<RawBook> books) {
        RangeBasedDatalake datalake = new RangeBasedDatalake(dir);
        books.forEach(datalake::save);
        return datalake;
    }

    /** Prints the books each benchmark uses, as CSV. */
    public static void main(String[] args) {
        List<RawBook> books = load();
        Tokenizer tokenizer = Tokenizer.standard();
        System.out.println("role,book_id,body_bytes,tokens,unique_terms");
        for (String which : List.of("short", "median", "long")) {
            print(which, byLength(books, which), tokenizer);
        }
        long bytes = 0;
        long tokens = 0;
        long unique = 0;
        for (RawBook book : batch(books)) {
            bytes += utf8(book.body());
            tokens += tokenizer.tokenize(book.body()).size();
            unique += tokenizer.uniqueTerms(book.body()).size();
        }
        System.out.printf("batch_of_%d,%d-%d,%d,%d,%d%n", BATCH, batch(books).get(0).id(),
                batch(books).get(BATCH - 1).id(), bytes, tokens, unique);
        System.out.printf("all_%d_books,,%d,,%n", books.size(), books.stream().mapToLong(b -> utf8(b.body())).sum());
    }

    private static void print(String role, RawBook book, Tokenizer tokenizer) {
        System.out.printf("%s,%d,%d,%d,%d%n", role, book.id(), utf8(book.body()),
                tokenizer.tokenize(book.body()).size(), tokenizer.uniqueTerms(book.body()).size());
    }

    private static long utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8).length;
    }
}
