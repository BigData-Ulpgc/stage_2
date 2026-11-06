package es.ulpgc.bigdata.ingestion.source;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalFileSourceTest {

    @TempDir
    Path tmp;

    /** Every folder of these tests has at least one book, or the source would not start. */
    @BeforeEach
    void oneBook() throws Exception {
        Files.writeString(tmp.resolve("pg1.txt"), "a book");
    }

    @Test
    void returnsTheFilePgIdUntouched() throws Exception {
        String text = "Title: Ñandú\r\n*** START OF X ***\r\ncuerpo\r\n*** END OF X ***\r\n";   // \r\n and UTF-8
        Files.writeString(tmp.resolve("pg42.txt"), text, StandardCharsets.UTF_8);
        assertEquals(text, new LocalFileSource(tmp).fetch(42));
    }

    @Test
    void aMissingFileIsABookNotFound() {
        BookNotFoundException e = assertThrows(BookNotFoundException.class, () -> new LocalFileSource(tmp).fetch(7));
        assertEquals(7, e.bookId());
    }

    @Test
    void anotherReadErrorIsTransient() throws Exception {
        Files.createDirectory(tmp.resolve("pg9.txt"));             // exists, but cannot be read as a file
        assertThrows(SourceUnavailableException.class, () -> new LocalFileSource(tmp).fetch(9));
    }

    @Test
    void aFolderThatDoesNotExistIsRejectedAtStartup() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new LocalFileSource(tmp.resolve("nowhere")));
        assertTrue(e.getMessage().contains("nowhere"));
    }

    /** What Docker leaves when the host folder of a bind mount does not exist. */
    @Test
    void anEmptyFolderIsRejectedAtStartup() throws Exception {
        Path empty = Files.createDirectory(tmp.resolve("empty"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new LocalFileSource(empty));
        assertTrue(e.getMessage().contains("has no pg<id>.txt file"), e.getMessage());
    }

    @Test
    void aFolderWithoutBookFilesIsRejectedToo() throws Exception {
        Path other = Files.createDirectory(tmp.resolve("other"));
        Files.writeString(other.resolve("README.md"), "not a book");
        Files.writeString(other.resolve("pgX.txt"), "not an id");
        assertThrows(IllegalArgumentException.class, () -> new LocalFileSource(other));
    }
}
