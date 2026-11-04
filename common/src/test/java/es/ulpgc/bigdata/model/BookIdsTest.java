package es.ulpgc.bigdata.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.*;

class BookIdsTest {

    @ParameterizedTest
    @ValueSource(strings = {"1", "84", "1342", "75000", "999999999"})
    void aceptaLosIdsEnFormaCanonica(String text) {
        assertEquals(OptionalInt.of(Integer.parseInt(text)), BookIds.parse(text));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",                 // vacío
            "0",                // el mínimo es 1
            "-5",               // negativo
            "+84",              // con signo: parseInt lo acepta
            "0084", "007",      // ceros a la izquierda: parseInt los acepta
            " 84", "84 ",       // espacios
            "1e3", "84.0", "8_4", "abc", "0x54",
            "١٢٣",              // dígitos arábigos: parseInt los acepta
            "８４",              // dígitos de ancho completo: parseInt los acepta
            "1000000000",       // 10 dígitos: por encima de MAX
            "99999999999"       // ni siquiera cabe en un int
    })
    void rechazaTodoLoQueNoEsCanonico(String text) {
        assertTrue(BookIds.parse(text).isEmpty(), () -> "debería rechazar \"" + text + "\"");
    }

    @Test
    void nullNoEsUnId() {
        assertTrue(BookIds.parse(null).isEmpty());
    }

    @Test
    void parseIntEsDemasiadoPermisivo() {
        // El motivo de esta clase: con parseInt, tres textos distintos nombran el libro 84.
        assertEquals(84, Integer.parseInt("+84"));
        assertEquals(84, Integer.parseInt("0084"));
        assertEquals(123, Integer.parseInt("١٢٣"));
    }

    @Test
    void cadaIdValidoTieneUnSoloTextoYEsElDeToString() {
        for (int id : new int[]{1, 9, 10, 84, 1342, 999_999_999}) {
            assertEquals(OptionalInt.of(id), BookIds.parse(Integer.toString(id)));
        }
    }

    @Test
    void isValidAplicaLaMismaRegla() {
        assertFalse(BookIds.isValid(Integer.MIN_VALUE));
        assertFalse(BookIds.isValid(-1));
        assertFalse(BookIds.isValid(0));
        assertTrue(BookIds.isValid(1));
        assertTrue(BookIds.isValid(BookIds.MAX));
        assertFalse(BookIds.isValid(BookIds.MAX + 1));
        assertFalse(BookIds.isValid(Integer.MAX_VALUE));
    }
}
