package es.ulpgc.bigdata.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class YearSourceTest {

    @Test
    void fromKeyEsLoContrarioDeKey() {
        for (YearSource source : YearSource.values()) {
            assertEquals(source, YearSource.fromKey(source.key()));
        }
    }

    @Test
    void fromKeyDeNullEsNull() {
        assertNull(YearSource.fromKey(null));
    }

    @Test
    void fromKeyNoAceptaElNombreJavaDelEnum() {
        // The database stores "release_date", never "RELEASE_DATE".
        assertThrows(IllegalArgumentException.class, () -> YearSource.fromKey("RELEASE_DATE"));
    }

    @Test
    void fromKeyDesconocidoEsUnError() {
        assertThrows(IllegalArgumentException.class, () -> YearSource.fromKey("release-date"));
    }
}
