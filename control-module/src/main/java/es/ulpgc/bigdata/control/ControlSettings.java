package es.ulpgc.bigdata.control;

import es.ulpgc.bigdata.config.Env;
import es.ulpgc.bigdata.http.ServiceServer;

import java.net.URI;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

/**
 * Configuration of the Control Module, read from environment variables (plan §3.5):
 *
 * <pre>
 *   CONTROL_PORT            7000
 *   CONTROL_HTTP_THREADS    ServiceServer.DEFAULT_MAX_THREADS
 *   PUBLISH_BATCH           100    publish when this many books are INDEXED...
 *   PUBLISH_INTERVAL_S      30     ...or this long after the last publication, if any is INDEXED
 *   INDEXING_URL            http://localhost:7002   (inside Docker: http://indexing:7002)
 *   CONTROL_SEED_FILE       (none) a book_ids.txt file to seed at startup; ids one per line,
 *                           # comments and blank lines skipped
 * </pre>
 *
 * The lease check period, the attempts and the cluster come from CoordinationSettings.
 */
public record ControlSettings(int port, int httpThreads, int publishBatch, int publishIntervalSeconds,
                              URI indexingUrl, Optional<Path> seedFile) {

    public static final String PORT = "CONTROL_PORT";
    public static final String HTTP_THREADS = "CONTROL_HTTP_THREADS";
    public static final String PUBLISH_BATCH = "PUBLISH_BATCH";
    public static final String PUBLISH_INTERVAL_S = "PUBLISH_INTERVAL_S";
    public static final String INDEXING_URL = "INDEXING_URL";
    public static final String SEED_FILE = "CONTROL_SEED_FILE";

    public static final int DEFAULT_PORT = 7000;
    public static final int DEFAULT_PUBLISH_BATCH = 100;
    public static final int DEFAULT_PUBLISH_INTERVAL_S = 30;
    public static final String DEFAULT_INDEXING_URL = "http://localhost:7002";

    public ControlSettings {
        Objects.requireNonNull(indexingUrl, INDEXING_URL);
        Objects.requireNonNull(seedFile, SEED_FILE);
    }

    public static ControlSettings fromEnv(Env env) {
        String url = env.string(INDEXING_URL, DEFAULT_INDEXING_URL);
        URI indexing;
        try {
            indexing = URI.create(url.endsWith("/") ? url.substring(0, url.length() - 1) : url);
            if (indexing.getScheme() == null || indexing.getHost() == null) {
                throw new IllegalArgumentException("no scheme or host");
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(INDEXING_URL + " must be an http URL, got '" + url + "'", e);
        }
        String seed = env.string(SEED_FILE, null);
        return new ControlSettings(
                env.port(PORT, DEFAULT_PORT),
                env.integerAtLeast(HTTP_THREADS, ServiceServer.DEFAULT_MAX_THREADS, ServiceServer.MIN_THREADS),
                env.integerAtLeast(PUBLISH_BATCH, DEFAULT_PUBLISH_BATCH, 1),
                env.integerAtLeast(PUBLISH_INTERVAL_S, DEFAULT_PUBLISH_INTERVAL_S, 1),
                indexing,
                Optional.ofNullable(seed).map(Path::of));
    }
}
