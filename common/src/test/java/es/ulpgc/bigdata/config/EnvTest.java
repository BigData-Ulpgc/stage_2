package es.ulpgc.bigdata.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnvTest {

    @Test
    void unsetOrBlankVariablesUseTheDefault() {
        Env env = Env.of(Map.of("BLANK", "   "));
        assertEquals("data", env.string("DATA_DIR", "data"));
        assertEquals("data", env.string("BLANK", "data"));
        assertEquals(7002, env.port("INDEXING_PORT", 7002));
    }

    @Test
    void setVariablesWinAndAreTrimmed() {
        Env env = Env.of(Map.of("DATA_DIR", " /data ", "INDEXING_PORT", "7012"));
        assertEquals("/data", env.string("DATA_DIR", "data"));
        assertEquals(7012, env.port("INDEXING_PORT", 7002));
    }

    @Test
    void malformedIntegerFailsNamingTheVariable() {
        Env env = Env.of(Map.of("INDEXING_PORT", "abc"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> env.port("INDEXING_PORT", 7002));
        assertTrue(e.getMessage().contains("INDEXING_PORT"));
    }

    @Test
    void portOutOfRangeIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> Env.of(Map.of("P", "70000")).port("P", 1));
        assertThrows(IllegalArgumentException.class, () -> Env.of(Map.of("P", "-1")).port("P", 1));
        assertEquals(0, Env.of(Map.of("P", "0")).port("P", 1));
    }

    @Test
    void integerAtLeastEnforcesTheMinimum() {
        Env env = Env.of(Map.of("THREADS", "2"));
        assertThrows(IllegalArgumentException.class, () -> env.integerAtLeast("THREADS", 250, 8));
        assertEquals(250, Env.of(Map.of()).integerAtLeast("THREADS", 250, 8));
    }
}
