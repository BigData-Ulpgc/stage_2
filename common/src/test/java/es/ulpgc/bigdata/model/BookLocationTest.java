package es.ulpgc.bigdata.model;

import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class BookLocationTest {

    @Test
    void guardaLasRutasComoPathNoComoString() {
        Path header = Path.of("data/datalake/book/10/header.txt");
        Path body = Path.of("data/datalake/book/10/body.txt");

        BookLocation location = new BookLocation(10, header, body);

        assertEquals(header, location.headerPath());
        assertEquals(body, location.bodyPath());
        assertInstanceOf(Path.class, location.headerPath());
    }

    @Test
    void dosRutasConstruidasDeFormaDistintaSonIgualesSiApuntanAlMismoSitio() {
        Path a = Path.of("data/datalake/book/10/header.txt");
        Path b = Path.of("data", "datalake", "book", "10", "header.txt");

        assertEquals(a, b);
    }
}