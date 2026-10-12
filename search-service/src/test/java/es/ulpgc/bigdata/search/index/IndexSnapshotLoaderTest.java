package es.ulpgc.bigdata.search.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Loads the DP-03 fixture (src/test/resources/fixtures/sample-datamarts, 15 books of
 * stage_1/sample_dataset). Expected postings were cross-checked with an independent
 * implementation of the SPEC tokenizer (identical index, 30396 terms).
 */
class IndexSnapshotLoaderTest {

    private static Path fixture() throws URISyntaxException {
        return Path.of(IndexSnapshotLoaderTest.class.getResource("/fixtures/sample-datamarts").toURI());
    }

    @Test
    void loadsThePublishedVersion() throws Exception {
        IndexSnapshot snapshot = IndexSnapshotLoader.loadCurrent(fixture()).orElseThrow();
        assertEquals("v000001", snapshot.version());
        assertEquals(15, snapshot.bookCount());
        assertEquals(30396, snapshot.termCount());
        assertEquals(List.of(5, 11, 46, 74, 76, 84, 98, 174, 345, 1080, 1342, 1661, 1952, 2542, 2701),
                List.copyOf(snapshot.bookIds()));
    }

    @Test
    void postingsMatchTheReferenceResults() throws Exception {
        IndexSnapshot snapshot = IndexSnapshotLoader.loadCurrent(fixture()).orElseThrow();
        assertEquals(List.of(76, 84, 345, 2701), snapshot.postings("whale"));
        assertEquals(List.of(46, 74, 76, 84, 174, 345, 1342, 1661, 2701), snapshot.postings("adventure"));
        assertEquals(List.of(), snapshot.postings("zzzzqqq"));      // unknown term
        assertEquals(List.of(), snapshot.postings("the"));          // stopword: never indexed
    }

    @Test
    void metadataFollowsTheStage2Contract() throws Exception {
        IndexSnapshot snapshot = IndexSnapshotLoader.loadCurrent(fixture()).orElseThrow();
        BookRecord book = snapshot.book(1342).orElseThrow();
        assertEquals("Pride and Prejudice", book.title());
        assertEquals("Jane Austen", book.author());
        assertEquals("en", book.language());                 // ISO 639-1, not "English"
        assertEquals(1998, book.year());                     // Gutenberg release year...
        assertEquals("release_date", book.yearSource());     // ...because there is no original publication line
        assertTrue(snapshot.book(999999).isEmpty());         // not in the published index
    }

    @Test
    void nothingPublishedYieldsEmpty(@TempDir Path empty) {
        assertTrue(IndexSnapshotLoader.loadCurrent(empty).isEmpty());   // the API will answer 503
    }

    @Test
    void currentPointingToAMissingVersionIsRejected(@TempDir Path dir) throws IOException {
        Files.createDirectories(dir.resolve("index"));
        Files.writeString(dir.resolve("index/CURRENT"), "v000099");
        assertThrows(IllegalStateException.class, () -> IndexSnapshotLoader.loadCurrent(dir));
    }

    @Test
    void manifestThatDisagreesWithTheIndexIsRejected(@TempDir Path dir) throws Exception {
        copyTree(fixture(), dir);
        Files.writeString(dir.resolve("index/versions/v000001/manifest.json"), "{\"version\": 1, \"books\": 14}");
        assertThrows(IllegalStateException.class, () -> IndexSnapshotLoader.loadCurrent(dir));
    }

    private static void copyTree(Path from, Path to) throws IOException {
        try (Stream<Path> paths = Files.walk(from)) {
            for (Path source : paths.toList()) {
                Path target = to.resolve(from.relativize(source).toString());
                if (Files.isDirectory(source)) {
                    Files.createDirectories(target);
                } else {
                    Files.copy(source, target);
                }
            }
        }
    }
}
