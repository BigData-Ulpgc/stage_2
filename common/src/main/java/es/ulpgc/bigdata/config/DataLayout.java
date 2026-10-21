package es.ulpgc.bigdata.config;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Where each thing lives on the shared ./data volume. It is the Stage 2 version of the path
 * part of Stage 1's AppConfig: the ONLY place that builds "datalake/...", so Ingestion and
 * Indexing cannot disagree on a path.
 *
 * <pre>
 *   &lt;DATA_DIR&gt;/datalake/range/&lt;START&gt;-&lt;END&gt;/&lt;id&gt;.header.txt, &lt;id&gt;.body.txt   RangeBasedDatalake
 * </pre>
 *
 * The datalake is the only thing on this volume: the inverted index and the metadata live in
 * MongoDB (see MongoSettings), so there is no datamarts folder any more (DR-04).
 *
 * In Docker DATA_DIR is the mount point of the volume (e.g. /data, with ./data/datalake
 * mounted below it); with java -jar it defaults to ./data.
 *
 * What Stage 1's AppConfig had and is NOT here: the .properties file (Docker configures with
 * environment variables, read with Env), the choice of datalake/index structure (Stage 2
 * fixes range + mongo) and the benchmark settings.
 */
public record DataLayout(Path dataDir) {

    public static final String DATA_DIR = "DATA_DIR";
    public static final String DEFAULT_DATA_DIR = "data";

    public DataLayout {
        Objects.requireNonNull(dataDir, "dataDir");
    }

    /** DATA_DIR from the environment, or ./data. */
    public static DataLayout fromEnv(Env env) {
        return new DataLayout(Path.of(env.string(DATA_DIR, DEFAULT_DATA_DIR)));
    }

    /** Root of the range-based datalake: one folder per range of 1000 ids below it. */
    public Path datalakeDir() {
        return dataDir.resolve("datalake").resolve("range");
    }
}
