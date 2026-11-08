package es.ulpgc.bigdata.search.fixture;

import es.ulpgc.bigdata.search.fixture.MongoFixtureLoader.Book;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Loads the 15 real books of stage_1/sample_dataset into MongoDB as published version 1, to
 * run the Search Service by hand before Indexing exists. It writes to the REAL collections of
 * the given database (by default search_engine), replacing what is there.
 *
 * From search-service/, with MongoDB running (docker compose up -d mongo):
 * <pre>
 * mvn -q test-compile exec:java -Dexec.args="../../stage_1/sample_dataset/book"
 * mvn -q test-compile exec:java -Dexec.args="&lt;book dir&gt; mongodb://localhost:27017 search_engine"
 * </pre>
 */
public final class SampleDatasetLoader {

    private SampleDatasetLoader() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1 || args.length > 3) {
            System.err.println("usage: SampleDatasetLoader <sample_dataset/book> [mongo-uri] [database]");
            System.exit(1);
        }
        Path bookDir = Path.of(args[0]);
        String uri = args.length > 1 ? args[1] : "mongodb://localhost:27017";
        String database = args.length > 2 ? args[2] : "search_engine";

        List<Book> books = readBooks(bookDir);
        Map<Integer, Integer> versionOf = new HashMap<>();
        books.forEach(book -> versionOf.put(book.id(), 1));
        MongoFixtureLoader.load(uri, database, "inverted_index", books, versionOf, 1);
        System.out.printf("Published version 1 with %d books in %s/%s%n", books.size(), uri, database);
    }

    /** book/&lt;id&gt;/header.txt and body.txt, ascending by id. */
    static List<Book> readBooks(Path bookDir) throws IOException {
        try (Stream<Path> dirs = Files.list(bookDir)) {
            return dirs.filter(Files::isDirectory)
                    .map(dir -> Integer.parseInt(dir.getFileName().toString()))
                    .sorted()
                    .map(id -> new Book(id, read(bookDir.resolve(id + "/header.txt")), read(bookDir.resolve(id + "/body.txt"))))
                    .toList();
        }
    }

    private static String read(Path file) {
        try {
            return Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
