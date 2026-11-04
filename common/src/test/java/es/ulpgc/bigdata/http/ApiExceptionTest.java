package es.ulpgc.bigdata.http;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ApiExceptionTest {

    @Test
    void invalidEsUn400ConElCampo() {
        ApiException e = ApiException.invalid("limit", "limit must be between 1 and 100");

        assertEquals(400, e.status());
        assertEquals("INVALID_PARAMETER", e.code());
        assertEquals("limit", e.field());
        assertEquals("limit must be between 1 and 100", e.getMessage());
    }

    @Test
    void missingEsUn400QueNombraElCampo() {
        ApiException e = ApiException.missing("q");

        assertEquals(400, e.status());
        assertEquals("MISSING_PARAMETER", e.code());
        assertEquals("q", e.field());
        assertEquals("q is required", e.getMessage());
    }

    @Test
    void elConstructorPermiteLosCodigosPropiosDeCadaServicio() {
        ApiException e = new ApiException(503, "INDEX_NOT_READY", "no index version has been published yet", null);

        assertEquals(503, e.status());
        assertEquals("INDEX_NOT_READY", e.code());
        assertNull(e.field());
    }
}
