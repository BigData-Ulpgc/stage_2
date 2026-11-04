package es.ulpgc.bigdata.ingestion.source;

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
}
