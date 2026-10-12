package es.ulpgc.bigdata.search.index;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Loads the published index version from the datamarts directory (contracts §6.3 and §6.5):
 *
 * <pre>
 * datamarts/
 *   metadata.db                               books table (may be ahead of the index)
 *   index/CURRENT                             contains e.g. "v000042"
 *   index/versions/v000042/inverted_index.json {"term": [id, ...], ...}
 *   index/versions/v000042/manifest.json       {"version": 42, "books": 187, "published_at": "..."}
 * </pre>
 *
 * Search starts from the ids of the index: books that are in metadata.db but not yet in the
 * published index are not visible.
 */
public final class IndexSnapshotLoader {

    private static final Pattern VERSION_NAME = Pattern.compile("v\\d{6}");
    private static final Type INDEX_TYPE = new TypeToken<Map<String, int[]>>() {}.getType();
    private static final Gson GSON = new Gson();

    private IndexSnapshotLoader() {
    }

    /**
     * Loads the version CURRENT points to.
     *
     * @return empty if nothing has been published yet (no CURRENT file): the API answers 503
     * @throws IllegalStateException if CURRENT points to a missing or inconsistent version
     */
    public static Optional<IndexSnapshot> loadCurrent(Path datamartsDir) {
        Path current = datamartsDir.resolve("index").resolve("CURRENT");
        if (!Files.exists(current)) {
            return Optional.empty();
        }
        String version = readString(current).strip();
        if (!VERSION_NAME.matcher(version).matches()) {
            throw new IllegalStateException("CURRENT contains an invalid version name: '" + version + "'");
        }
        return Optional.of(load(datamartsDir, version));
    }

    /** Loads one specific version (used by loadCurrent and by tests). */
    public static IndexSnapshot load(Path datamartsDir, String version) {
        Path versionDir = datamartsDir.resolve("index").resolve("versions").resolve(version);
        Path indexFile = versionDir.resolve("inverted_index.json");
        Path manifestFile = versionDir.resolve("manifest.json");
        if (!Files.exists(indexFile) || !Files.exists(manifestFile)) {
            throw new IllegalStateException("Index version " + version + " is incomplete or missing in " + versionDir);
        }

        Map<String, int[]> postings = readJson(indexFile, INDEX_TYPE);
        TreeSet<Integer> bookIds = new TreeSet<>();
        for (Map.Entry<String, int[]> entry : postings.entrySet()) {
            checkSortedWithoutDuplicates(entry.getKey(), entry.getValue());
            for (int id : entry.getValue()) {
                bookIds.add(id);
            }
        }

        JsonObject manifest = readJson(manifestFile, JsonObject.class);
        int declaredBooks = manifest.get("books").getAsInt();
        if (declaredBooks != bookIds.size()) {
            throw new IllegalStateException("manifest.json of " + version + " declares " + declaredBooks
                    + " books but the index contains " + bookIds.size());
        }

        Map<Integer, BookRecord> books = readMetadata(datamartsDir.resolve("metadata.db"), bookIds);
        return new IndexSnapshot(version, postings, books, bookIds);
    }

    /** Reads the metadata of the given books; a book without a row gets an all-null record. */
    private static Map<Integer, BookRecord> readMetadata(Path dbFile, TreeSet<Integer> bookIds) {
        Map<Integer, BookRecord> books = new HashMap<>();
        for (int id : bookIds) {
            books.put(id, BookRecord.withoutMetadata(id));
        }
        if (!Files.exists(dbFile)) {
            return books;
        }
        Properties readOnly = new Properties();
        readOnly.setProperty("open_mode", "1");          // SQLITE_OPEN_READONLY: Search never writes
        String sql = "SELECT book_id, title, author, language, year, year_source, release_date FROM books";
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbFile.toAbsolutePath(), readOnly);
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet rows = statement.executeQuery()) {
            while (rows.next()) {
                int id = rows.getInt("book_id");
                if (bookIds.contains(id)) {               // metadata.db may be ahead of the index
                    int yearValue = rows.getInt("year");
                    Integer year = rows.wasNull() ? null : yearValue;   // must follow getInt directly
                    books.put(id, new BookRecord(id,
                            rows.getString("title"),
                            rows.getString("author"),
                            rows.getString("language"),
                            year,
                            rows.getString("year_source"),
                            rows.getString("release_date")));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot read " + dbFile, e);
        }
        return books;
    }

    private static void checkSortedWithoutDuplicates(String term, int[] ids) {
        for (int i = 1; i < ids.length; i++) {
            if (ids[i] <= ids[i - 1]) {
                throw new IllegalStateException("Posting list of '" + term + "' is not strictly ascending");
            }
        }
    }

    private static <T> T readJson(Path file, Type type) {
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            return GSON.fromJson(reader, type);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + file, e);
        }
    }

    private static String readString(Path file) {
        try {
            return Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + file, e);
        }
    }
}
