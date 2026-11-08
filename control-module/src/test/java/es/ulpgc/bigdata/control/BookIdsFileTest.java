package es.ulpgc.bigdata.control;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BookIdsFileTest {

    @TempDir
    Path dir;

    @Test
    void commentsAndBlankLinesAreSkipped() throws Exception {
        Path file = Files.writeString(dir.resolve("book_ids.txt"),
                "# Sample dataset\n# one id per line\n\n1342\n  84  \n\n11\n1342\n");
        assertEquals(List.of(1342, 84, 11, 1342), BookIdsFile.read(file));
    }

    @Test
    void aBadLineIsReportedWithItsNumber() throws Exception {
        Path file = Files.writeString(dir.resolve("book_ids.txt"), "# ids\n1342\n0084\n");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> BookIdsFile.read(file));
        assertTrue(e.getMessage().contains("line 3"), e.getMessage());
    }
}
