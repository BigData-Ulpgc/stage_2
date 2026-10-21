package es.ulpgc.bigdata.model;

import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class BookMetadataTest {

    @Test
    void creaMetadataCompleta() {
        BookMetadata meta = new BookMetadata(
                10, "Red Boat", "Jane Doe", "en", 2001, YearSource.RELEASE_DATE, "2001",
                Path.of("data/datalake/book/10/body.txt"),
                Path.of("data/datalake/book/10/header.txt")
        );

        assertEquals(10, meta.bookId());
        assertEquals("Red Boat", meta.title());
        assertEquals("Jane Doe", meta.author());
        assertEquals("en", meta.language());
        assertEquals(2001, meta.year());
        assertEquals(YearSource.RELEASE_DATE, meta.yearSource());
        assertEquals("2001", meta.releaseDate());
    }

    @Test
    void permiteCamposAusentesComoNull() {
        BookMetadata meta = new BookMetadata(
                20, "Blue Boat", null, null, null, null, null,
                Path.of("data/datalake/book/20/body.txt"),
                Path.of("data/datalake/book/20/header.txt")
        );

        assertNull(meta.author());
        assertNull(meta.language());
        assertNull(meta.year());
        assertNull(meta.yearSource());
        assertNull(meta.releaseDate());
        // the title and the id MUST always be present
        assertNotNull(meta.title());
        assertEquals(20, meta.bookId());
    }

    @Test
    void rechazaAnioSinOrigen() {
        assertThrows(IllegalArgumentException.class, () -> new BookMetadata(
                30, "Green Boat", null, null, 1998, null, null, null, null));
    }

    @Test
    void rechazaOrigenSinAnio() {
        assertThrows(IllegalArgumentException.class, () -> new BookMetadata(
                30, "Green Boat", null, null, null, YearSource.ORIGINAL_PUBLICATION, null, null, null));
    }

    @Test
    void elOrigenSeGuardaConElTextoDelContrato() {
        assertEquals("original_publication", YearSource.ORIGINAL_PUBLICATION.key());
        assertEquals("release_date", YearSource.RELEASE_DATE.key());
    }
}
