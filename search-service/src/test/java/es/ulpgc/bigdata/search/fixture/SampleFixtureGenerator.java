package es.ulpgc.bigdata.search.fixture;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import es.ulpgc.bigdata.datamart.index.Tokenizer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Builds the Search Service test fixture (DP-03) from stage_1/sample_dataset (15 books), with
 * the datamarts layout that the Indexing Service will publish (contracts §6.3 and §6.5):
 *
 * <pre>
 * &lt;out&gt;/metadata.db
 * &lt;out&gt;/index/CURRENT                                "v000001"
 * &lt;out&gt;/index/versions/v000001/inverted_index.json   {"term": [ids ascending], ...}
 * &lt;out&gt;/index/versions/v000001/manifest.json
 * </pre>
 *
 * It follows the Stage 1 rules: only the body is indexed, with the shared Tokenizer;
 * metadata comes from the first "Title:", "Author:", "Language:", "Release date:" lines.
 * Stage 2 additions (§6.3): language as an ISO 639-1 code and a year with its source.
 *
 * It is NOT the Indexing Service: it only lets Search be developed and tested before
 * Indexing exists. Run it (from search-service/):
 * <pre>
 * mvn -q test-compile exec:java -Dexec.args="&lt;sample_dataset/book&gt; &lt;stopwords.txt&gt; &lt;out&gt;"
 * </pre>
 */
public final class SampleFixtureGenerator {

    static final String VERSION = "v000001";
    /** Fixed so that regenerating the fixture gives identical files. */
    static final String PUBLISHED_AT = "2026-10-12T00:00:00Z";

    private static final Pattern TITLE = Pattern.compile("^Title:\\s*(.+)$", Pattern.MULTILINE);
    private static final Pattern AUTHOR = Pattern.compile("^Author:\\s*(.+)$", Pattern.MULTILINE);
    private static final Pattern LANGUAGE = Pattern.compile("^Language:\\s*(.+)$", Pattern.MULTILINE);
    private static final Pattern RELEASE_DATE = Pattern.compile("^Release date:\\s*(.+?)(\\s*\\[.*)?$", Pattern.MULTILINE);
    private static final Pattern ORIGINAL_PUBLICATION = Pattern.compile("^Original publication:\\s*(.+)$", Pattern.MULTILINE);
    private static final Pattern YEAR = Pattern.compile("\\b(\\d{4})\\b");

    /** Gutenberg language names -> ISO 639-1. Unknown names give null (contract §6.3). */
    private static final Map<String, String> ISO_639_1 = Map.ofEntries(
            Map.entry("english", "en"), Map.entry("spanish", "es"), Map.entry("french", "fr"),
            Map.entry("german", "de"), Map.entry("italian", "it"), Map.entry("portuguese", "pt"),
            Map.entry("dutch", "nl"), Map.entry("latin", "la"), Map.entry("finnish", "fi"),
            Map.entry("swedish", "sv"), Map.entry("danish", "da"), Map.entry("greek", "el"),
            Map.entry("russian", "ru"), Map.entry("chinese", "zh"), Map.entry("esperanto", "eo"));

    private static final String SCHEMA = """
            CREATE TABLE books (
                book_id      INTEGER PRIMARY KEY,
                title        TEXT,
                author       TEXT,
                language     TEXT,
                year         INTEGER,
                year_source  TEXT,
                release_date TEXT,
                body_path    TEXT,
                header_path  TEXT
            )""";

    record Metadata(int id, String title, String author, String language,
                    Integer year, String yearSource, String releaseDate) {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("usage: SampleFixtureGenerator <sample_dataset/book> <stopwords.txt> <out-dir>");
            System.exit(1);
        }
        generate(Path.of(args[0]), Path.of(args[1]), Path.of(args[2]));
    }

    static void generate(Path bookDir, Path stopwords, Path out) throws IOException, SQLException {
        Tokenizer tokenizer = Tokenizer.fromStopwordsFile(stopwords);
        TreeMap<String, TreeSet<Integer>> index = new TreeMap<>();
        List<Metadata> books;
        try (Stream<Path> dirs = Files.list(bookDir)) {
            books = dirs.filter(Files::isDirectory)
                    .map(dir -> Integer.parseInt(dir.getFileName().toString()))
                    .sorted()
                    .map(id -> readBook(bookDir, id, tokenizer, index))
                    .toList();
        }

        Path versionDir = out.resolve("index").resolve("versions").resolve(VERSION);
        Files.createDirectories(versionDir);
        Gson gson = new Gson();
        Files.writeString(versionDir.resolve("inverted_index.json"), gson.toJson(index), StandardCharsets.UTF_8);

        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("version", 1);
        manifest.put("books", books.size());
        manifest.put("terms", index.size());
        manifest.put("published_at", PUBLISHED_AT);
        manifest.put("source", "stage_1/sample_dataset (DP-03 test fixture)");
        Files.writeString(versionDir.resolve("manifest.json"),
                new GsonBuilder().setPrettyPrinting().create().toJson(manifest), StandardCharsets.UTF_8);

        writeMetadata(out.resolve("metadata.db"), books);
        // CURRENT is written last: the version is complete before it becomes visible.
        Files.writeString(out.resolve("index").resolve("CURRENT"), VERSION, StandardCharsets.UTF_8);

        System.out.printf("Fixture written to %s: %d books, %d terms, version %s%n",
                out, books.size(), index.size(), VERSION);
    }

    private static Metadata readBook(Path bookDir, int id, Tokenizer tokenizer,
                                     TreeMap<String, TreeSet<Integer>> index) {
        try {
            Path dir = bookDir.resolve(String.valueOf(id));
            String header = Files.readString(dir.resolve("header.txt"), StandardCharsets.UTF_8).replace("\r\n", "\n");
            String body = Files.readString(dir.resolve("body.txt"), StandardCharsets.UTF_8);
            for (String term : tokenizer.uniqueTerms(body)) {
                index.computeIfAbsent(term, t -> new TreeSet<>()).add(id);
            }
            return parseMetadata(id, header);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read book " + id, e);
        }
    }

    static Metadata parseMetadata(int id, String header) {
        String languageName = firstMatch(LANGUAGE, header);
        String language = languageName == null ? null
                : ISO_639_1.get(languageName.strip().toLowerCase(java.util.Locale.ROOT));
        String releaseDate = firstMatch(RELEASE_DATE, header);

        // Year rule (§6.3): original publication year if the header has it, else release year.
        Integer year = yearOf(firstMatch(ORIGINAL_PUBLICATION, header));
        String yearSource = year != null ? "original_publication" : null;
        if (year == null) {
            year = yearOf(releaseDate);
            yearSource = year != null ? "release_date" : null;
        }
        return new Metadata(id, firstMatch(TITLE, header), firstMatch(AUTHOR, header),
                language, year, yearSource, releaseDate);
    }

    private static Integer yearOf(String text) {
        if (text == null) {
            return null;
        }
        Matcher m = YEAR.matcher(text);
        Integer last = null;
        while (m.find()) {
            last = Integer.parseInt(m.group(1));     // "June 1, 1998" -> 1998
        }
        return last;
    }

    private static String firstMatch(Pattern pattern, String text) {
        Matcher m = pattern.matcher(text);
        if (!m.find()) {
            return null;
        }
        String value = m.group(1).strip();
        return value.isEmpty() ? null : value;
    }

    private static void writeMetadata(Path dbFile, List<Metadata> books) throws IOException, SQLException {
        Files.deleteIfExists(dbFile);
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + dbFile.toAbsolutePath())) {
            try (Statement st = c.createStatement()) {
                st.execute(SCHEMA);
                st.execute("CREATE INDEX idx_books_author ON books(author COLLATE NOCASE)");
                st.execute("CREATE INDEX idx_books_language ON books(language)");
                st.execute("CREATE INDEX idx_books_year ON books(year)");
            }
            String insert = "INSERT INTO books VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement ps = c.prepareStatement(insert)) {
                for (Metadata b : books) {
                    ps.setInt(1, b.id());
                    ps.setString(2, b.title());
                    ps.setString(3, b.author());
                    ps.setString(4, b.language());
                    if (b.year() == null) {
                        ps.setNull(5, java.sql.Types.INTEGER);
                    } else {
                        ps.setInt(5, b.year());
                    }
                    ps.setString(6, b.yearSource());
                    ps.setString(7, b.releaseDate());
                    ps.setString(8, "datalake/book/" + b.id() + "/body.txt");
                    ps.setString(9, "datalake/book/" + b.id() + "/header.txt");
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
    }
}
