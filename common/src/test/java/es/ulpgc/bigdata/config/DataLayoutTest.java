package es.ulpgc.bigdata.config;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DataLayoutTest {

    @Test
    void withoutDataDirEverythingIsBelowDotData() {
        DataLayout layout = DataLayout.fromEnv(Env.of(Map.of()));

        assertEquals(Path.of("data"), layout.dataDir());
        assertEquals(Path.of("data", "datalake", "range"), layout.datalakeDir());
    }

    @Test
    void pathsAreBelowDataDir() {
        DataLayout layout = DataLayout.fromEnv(Env.of(Map.of("DATA_DIR", "/data")));

        assertEquals(Path.of("/data/datalake/range"), layout.datalakeDir());
        assertEquals(Path.of("/data/datamarts"), layout.datamartsDir());
        assertEquals(Path.of("/data/datamarts/metadata.db"), layout.metadataDb());
    }
}
