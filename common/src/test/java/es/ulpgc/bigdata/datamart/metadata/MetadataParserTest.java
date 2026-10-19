package es.ulpgc.bigdata.datamart.metadata;

import es.ulpgc.bigdata.model.BookLocation;
import es.ulpgc.bigdata.model.BookMetadata;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class MetadataParserTest {

    private final MetadataParser parser = new MetadataParser();

    private static final BookLocation LOCATION = new BookLocation(1342,
            Path.of("datalake/book/1342/header.txt"),
            Path.of("datalake/book/1342/body.txt"));

    /** This is how the Pride and Prejudice header really starts. */
    private static final String FULL_HEADER = String.join("\n",
            "The Project Gutenberg eBook of Pride and Prejudice",
            "",
            "This ebook is for the use of anyone anywhere in the United States and",
            "most other parts of the world at no cost and with almost no restrictions",
            "whatsoever.",
            "",
            "Title: Pride and Prejudice",
            "",
            "Author: Jane Austen",
            "",
            "Release date: June 1, 1998 [eBook #1342]",
            "                Most recently updated: June 17, 2024",
            "",
            "Language: English",
            "",
            "Credits: Chuck Greif and the Online Distributed Proofreading Team");

    private BookMetadata parse(String header) {
        return parser.parse(LOCATION, header);
    }

    // --- The three headers the challenge asks for ---------------------------

    @Test
    void headerCompleto() {
        BookMetadata m = parse(FULL_HEADER);

        assertEquals(1342, m.bookId());
        assertEquals("Pride and Prejudice", m.title());
        assertEquals("Jane Austen", m.author());
        assertEquals("English", m.language());
        assertEquals("June 1, 1998", m.releaseDate());
    }

    @Test
    void sinAutorDevuelveNullYElRestoSigueBien() {
        String header = "Title: Beowulf\n\nRelease date: July 19, 2005 [eBook #16328]\n\nLanguage: English\n";

        BookMetadata m = parse(header);

        assertNull(m.author());
        assertEquals("Beowulf", m.title());
        assertEquals("July 19, 2005", m.releaseDate());
        assertEquals("English", m.language());
    }

    @Test
    void fechaConCorchetesSeQuedaSinEllos() {
        assertEquals("June 1, 1998",
                parse("Release date: June 1, 1998 [eBook #1342]\n").releaseDate());
    }

    // --- Contract details ---------------------------------------------------

    @Test
    void fechaSinCorchetesSeQuedaEntera() {
        assertEquals("June 1, 1998", parse("Release date: June 1, 1998\n").releaseDate());
    }

    @Test
    void soloCuentaLaPrimeraLineaDelValor() {
        // The "Most recently updated" line belongs to the date but is not included.
        assertEquals("June 1, 1998", parse(FULL_HEADER).releaseDate());
    }

    @Test
    void soloCuentaLaPrimeraCoincidencia() {
        assertEquals("Primero", parse("Title: Primero\nTitle: Segundo\n").title());
    }

    @Test
    void recortaEspaciosAlrededorDelValor() {
        assertEquals("Emma", parse("Title:     Emma   \t\n").title());
    }

    @Test
    void crlfNoDejaRetornosDeCarro() {
        BookMetadata m = parse("Title: Emma\r\nAuthor: Jane Austen\r\nLanguage: English\r\n");

        assertEquals("Emma", m.title());
        assertEquals("Jane Austen", m.author());
        assertEquals("English", m.language());
    }

    @Test
    void elCampoTieneQueEmpezarLaLinea() {
        // Thanks to MULTILINE, ^ is "start of line": "Original Title:" does not count.
        BookMetadata m = parse("Original Title: Otra cosa\nTitle: Emma\n");
        assertEquals("Emma", m.title());
    }

    @Test
    void headerVacioNoLanzaExcepcion() {
        BookMetadata m = parse("");

        assertNull(m.title());
        assertNull(m.author());
        assertNull(m.language());
        assertNull(m.releaseDate());
    }

    @Test
    void campoSoloConEspaciosAlFinalEsNull() {
        assertNull(parse("Language: English\nTitle:    ").title());
    }

    @Test
    void idYRutasVienenDeLaLocation() {
        BookMetadata m = parse(FULL_HEADER);

        assertEquals(LOCATION.id(), m.bookId());
        assertEquals(LOCATION.headerPath(), m.headerPath());
        assertEquals(LOCATION.bodyPath(), m.bodyPath());
    }

    // --- SPEC behaviour worth having documented -----------------------------

    @Test
    void releaseDateConDMayusculaNoCoincide() {
        // The SPEC regex is case-sensitive. Some old books use
        // "Release Date:". If the team decides to accept it, change the regex in the
        // three languages and this test.
        assertNull(parse("Release Date: March 1, 1994 [EBook #84]\n").releaseDate());
    }

    @Test
    void autorVacioCapturaLaLineaSiguiente() {
        // \s* also consumes line breaks, so with an empty "Author:" the
        // SPEC regex swallows the next line. It is the behaviour agreed
        // today; if the team changes it to [ \t]*, this test must expect null.
        assertEquals("Language: English", parse("Author:\nLanguage: English\n").author());
    }
}