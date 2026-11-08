package es.ulpgc.bigdata.ingestion.source;

import es.ulpgc.bigdata.config.Env;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BookSourcesTest {

    @TempDir
    Path tmp;

    /** A local folder must hold at least one book (LocalFileSource). */
    @BeforeEach
    void oneBook() throws Exception {
        Files.writeString(tmp.resolve("pg1.txt"), "a book");
    }

    @Test
    void theDefaultIsGutenberg() {
        BookSource source = BookSources.fromEnv(Env.of(Map.of()));
        assertInstanceOf(GutenbergClient.class, source);
        assertEquals("gutenberg https://www.gutenberg.org/cache/epub", source.describe());
    }

    @Test
    void gutenbergTakesItsUrlFromTheEnvironment() {
        BookSource source = BookSources.fromEnv(Env.of(Map.of("INGESTION_GUTENBERG_URL", "http://mirror:8080/epub")));
        assertEquals("gutenberg http://mirror:8080/epub", source.describe());
    }

    @Test
    void localReadsTheFolderOfTheEnvironment() {
        BookSource source = BookSources.fromEnv(Env.of(Map.of("INGESTION_SOURCE", "local", "INGESTION_LOCAL_DIR", tmp.toString())));
        assertInstanceOf(LocalFileSource.class, source);
        assertEquals("local " + tmp.toAbsolutePath(), source.describe());
    }

    @Test
    void anUnknownSourceFailsNamingTheVariable() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> BookSources.fromEnv(Env.of(Map.of("INGESTION_SOURCE", "s3"))));
        assertTrue(e.getMessage().contains("INGESTION_SOURCE"));
        assertTrue(e.getMessage().contains("s3"));
    }

    @Test
    void aMissingLocalFolderFailsNamingTheVariable() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> BookSources.fromEnv(Env.of(Map.of("INGESTION_SOURCE", "local", "INGESTION_LOCAL_DIR", tmp.resolve("x").toString()))));
        assertTrue(e.getMessage().startsWith("INGESTION_LOCAL_DIR"));
    }

    @Test
    void timeoutsMustBePositive() {
        assertThrows(IllegalArgumentException.class,
                () -> BookSources.fromEnv(Env.of(Map.of("INGESTION_REQUEST_TIMEOUT_SECONDS", "0"))));
    }

    @Test
    void onlyTheVariablesOfTheChosenSourceAreRead() {
        // A bad Gutenberg timeout does not matter when the source is local
        BookSources.fromEnv(Env.of(Map.of("INGESTION_SOURCE", "local", "INGESTION_LOCAL_DIR", tmp.toString(),
                "INGESTION_REQUEST_TIMEOUT_SECONDS", "x")));
    }
}
