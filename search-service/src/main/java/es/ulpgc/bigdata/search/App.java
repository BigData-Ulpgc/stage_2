package es.ulpgc.bigdata.search;

import es.ulpgc.bigdata.search.index.IndexSnapshot;
import es.ulpgc.bigdata.search.index.IndexSnapshotLoader;

import java.nio.file.Path;
import java.util.Optional;

/**
 * Entry point of the Search Service.
 *
 * DP-03: for now it only loads the published index version and reports it. The Javalin
 * server with GET /status is the next step (DP-03, part 2); the API follows api/openapi.yaml.
 *
 * Configuration (environment variables):
 *   DATA_DIR  root of the shared data volume; the datamarts are in DATA_DIR/datamarts (default "data")
 */
public final class App {

    private App() {
    }

    public static void main(String[] args) {
        Path datamarts = Path.of(System.getenv().getOrDefault("DATA_DIR", "data")).resolve("datamarts");
        Optional<IndexSnapshot> snapshot = IndexSnapshotLoader.loadCurrent(datamarts);
        if (snapshot.isEmpty()) {
            System.out.println("No index version published yet in " + datamarts.toAbsolutePath());
            return;
        }
        IndexSnapshot s = snapshot.get();
        System.out.printf("Loaded index %s from %s: %d books, %d terms%n",
                s.version(), datamarts.toAbsolutePath(), s.bookCount(), s.termCount());
    }
}
