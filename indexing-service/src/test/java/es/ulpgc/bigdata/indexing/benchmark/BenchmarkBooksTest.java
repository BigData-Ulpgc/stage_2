package es.ulpgc.bigdata.indexing.benchmark;

import es.ulpgc.bigdata.model.RawBook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The benchmarks must measure the right books: this checks how they are read and chosen. */
class BenchmarkBooksTest {

    @TempDir Path dir;

    private static String raw(int id, String body) {
        return ("The Project Gutenberg eBook of Book " + id + "\r\n\r\nTitle: Book " + id + "\r\n\r\n"
                + "*** START OF THE PROJECT GUTENBERG EBOOK BOOK " + id + " ***\r\n\r\n" + body + "\r\n\r\n"
                + "*** END OF THE PROJECT GUTENBERG EBOOK BOOK " + id + " ***\r\n\r\nlicense\r\n");
    }

    @Test
    void separaCabeceraYCuerpoComoBookSplitter() {
        RawBook book = BenchmarkBooks.split(7, raw(7, "First line.\r\nSecond line.")).orElseThrow();
        assertEquals("The Project Gutenberg eBook of Book 7\n\nTitle: Book 7", book.header());
        assertEquals("First line.\nSecond line.", book.body(), "sin el pie de licencia y con \\n");
    }

    @Test
    void sinMarcasNoHayLibro() {
        assertTrue(BenchmarkBooks.split(7, "Title: nothing\r\n").isEmpty());
        assertTrue(BenchmarkBooks.split(7, "*** START OF THE PROJECT GUTENBERG EBOOK X ***\r\nno end").isEmpty());
    }

    @Test
    void leeLosLibrosDeLaCarpetaOrdenadosPorIdEIgnoraLosDemasFicheros() throws Exception {
        Files.writeString(dir.resolve("pg30.txt"), raw(30, "thirty"));
        Files.writeString(dir.resolve("pg4.txt"), raw(4, "four"));
        Files.writeString(dir.resolve("manifest.json"), "{}");
        Files.writeString(dir.resolve("book_ids.txt"), "4\n30\n");

        List<RawBook> books = BenchmarkBooks.load(dir);

        assertEquals(List.of(4, 30), books.stream().map(RawBook::id).toList());
        assertEquals("thirty", books.get(1).body());
    }

    @Test
    void unaCarpetaQueNoExisteOVaciaDiceComoGenerarla() {
        IllegalStateException missing = assertThrows(IllegalStateException.class,
                () -> BenchmarkBooks.load(dir.resolve("nope")));
        assertTrue(missing.getMessage().contains("run-jmh.sh"), missing.getMessage());
        assertThrows(IllegalStateException.class, () -> BenchmarkBooks.load(dir));
    }

    @Test
    void eligeElLibroDelPercentilDeLongitudYDesempataPorId() {
        List<RawBook> books = new ArrayList<>();
        for (int id = 1; id <= 11; id++) {
            books.add(new RawBook(id, "h", "x".repeat(12 - id)));   // id 1 is the longest
        }
        books.add(new RawBook(12, "h", "x".repeat(6)));             // ties with id 6 (6 chars)

        assertEquals(10, BenchmarkBooks.byLength(books, "short").id());
        assertEquals(6, BenchmarkBooks.byLength(books, "median").id(), "con un empate, el id menor");
        assertEquals(3, BenchmarkBooks.byLength(books, "long").id());
        assertThrows(IllegalArgumentException.class, () -> BenchmarkBooks.byLength(books, "huge"));
    }

    @Test
    void elLoteSonLosPrimerosCienPorId() {
        List<RawBook> books = new ArrayList<>();
        for (int id = 1; id <= 150; id++) {
            books.add(new RawBook(id, "h", "b"));
        }
        List<RawBook> batch = BenchmarkBooks.batch(books);
        assertEquals(BenchmarkBooks.BATCH, batch.size());
        assertEquals(1, batch.get(0).id());
        assertEquals(100, batch.get(99).id());
        assertThrows(IllegalStateException.class, () -> BenchmarkBooks.batch(books.subList(0, 99)));
    }
}
