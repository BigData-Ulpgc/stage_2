package es.ulpgc.bigdata.datamart.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LanguageCodesTest {

    @Test
    void nombreDeGutenbergACodigoIso() {
        assertEquals("en", LanguageCodes.toIso("English"));
        assertEquals("el", LanguageCodes.toIso("Greek"));
        assertEquals("eo", LanguageCodes.toIso("  Esperanto  "));
    }

    @Test
    void nullDaNull() {
        assertNull(LanguageCodes.toIso(null));
    }

    @Test
    void separadorSinIdiomaDelanteDaNull() {
        assertNull(LanguageCodes.toIso(", English"));
    }

    @Test
    void losQuinceCodigosSoportadosOrdenados() {
        assertEquals(15, LanguageCodes.supportedCodes().size());
        assertEquals("da", LanguageCodes.supportedCodes().first());
        assertTrue(LanguageCodes.supportedCodes().contains("en"));
    }

    @Test
    void laListaSoportadaNoSePuedeModificar() {
        assertThrows(UnsupportedOperationException.class, () -> LanguageCodes.supportedCodes().add("xx"));
    }
}
