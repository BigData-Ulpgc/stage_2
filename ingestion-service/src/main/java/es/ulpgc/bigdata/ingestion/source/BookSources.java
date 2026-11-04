package es.ulpgc.bigdata.ingestion.source;

import es.ulpgc.bigdata.config.Env;

import java.nio.file.Path;
import java.time.Duration;

/**
 * Builds the BookSource of the replica from environment variables:
 *
 * <pre>
 *   INGESTION_SOURCE                    gutenberg | local                 (default gutenberg)
 *   INGESTION_GUTENBERG_URL             https://www.gutenberg.org/cache/epub
 *   INGESTION_CONNECT_TIMEOUT_SECONDS   10
 *   INGESTION_REQUEST_TIMEOUT_SECONDS   30
 *   INGESTION_LOCAL_DIR                 sample_dataset/raw   (folder of pg&lt;id&gt;.txt files)
 * </pre>
 *
 * Only the variables of the chosen source are read. A wrong value stops the service at
 * startup with a message naming the variable.
 */
public final class BookSources {

    public static final String SOURCE = "INGESTION_SOURCE";
    public static final String GUTENBERG_URL = "INGESTION_GUTENBERG_URL";
    public static final String CONNECT_TIMEOUT = "INGESTION_CONNECT_TIMEOUT_SECONDS";
    public static final String REQUEST_TIMEOUT = "INGESTION_REQUEST_TIMEOUT_SECONDS";
    public static final String LOCAL_DIR = "INGESTION_LOCAL_DIR";

    static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 10;
    static final int DEFAULT_REQUEST_TIMEOUT_SECONDS = 30;
    static final String DEFAULT_LOCAL_DIR = "sample_dataset/raw";

    private BookSources() {
    }

    public static BookSource fromEnv(Env env) {
        String source = env.string(SOURCE, "gutenberg");
        return switch (source) {
            case "gutenberg" -> new GutenbergClient(
                    env.string(GUTENBERG_URL, GutenbergClient.DEFAULT_BASE_URL),
                    Duration.ofSeconds(env.integerAtLeast(CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT_SECONDS, 1)),
                    Duration.ofSeconds(env.integerAtLeast(REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT_SECONDS, 1)));
            case "local" -> {
                Path dir = Path.of(env.string(LOCAL_DIR, DEFAULT_LOCAL_DIR));
                try {
                    yield new LocalFileSource(dir);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(LOCAL_DIR + ": " + e.getMessage(), e);
                }
            }
            default -> throw new IllegalArgumentException(
                    SOURCE + " must be 'gutenberg' or 'local', got '" + source + "'");
        };
    }
}
