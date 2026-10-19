package es.ulpgc.bigdata.model;

import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class BookMetadataTest {

    @Test
    void creaMetadataCompleta() {
        BookMetadata meta = new BookMetadata(
                10, "Red Boat", "Jane Doe", "English", "2001",
                Path.of("data/datalake/book/10/body.txt"),
                Path.of("data/datalake/book/10/header.txt")
        );

        assertEquals(10, meta.bookId());
        assertEquals("Red Boat", meta.title());
        assertEquals("Jane Doe", meta.author());
        assertEquals("English", meta.language());
        assertEquals("2001", meta.releaseDate());
    }

    @Test
    void permiteCamposAusentesComoNull() {
        BookMetadata meta = new BookMetadata(
                20, "Blue Boat", null, null, null,
                Path.of("data/datalake/book/20/body.txt"),
                Path.of("data/datalake/book/20/header.txt")
        );

        assertNull(meta.author());
        assertNull(meta.language());
        assertNull(meta.releaseDate());
        // the title and the id MUST always be present
        assertNotNull(meta.title());
        assertEquals(20, meta.bookId());
    }
}
