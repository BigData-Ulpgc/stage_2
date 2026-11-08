package es.ulpgc.bigdata.ingestion;

import es.ulpgc.bigdata.model.RawBook;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class BookSplitterTest {

    /** The Stage 1 repository next to this one, as the Search fixtures use it. */
    private static final Path SAMPLE = Path.of("../../stage_1/sample_dataset");

    private final BookSplitter splitter = new BookSplitter();

    @Test
    void splitsHeaderAndBodyAndDropsTheFooter() {
        String raw = """
                Title: Red Boat
                Author: Jane Doe

                *** START OF THE PROJECT GUTENBERG EBOOK RED BOAT ***

                red boat sails

                *** END OF THE PROJECT GUTENBERG EBOOK RED BOAT ***

                This is the footer, licensing info, etc.
                """;
        RawBook book = splitter.split(10, raw).orElseThrow();
        assertEquals(10, book.id());
        assertEquals("Title: Red Boat\nAuthor: Jane Doe", book.header());
        assertEquals("red boat sails", book.body());
    }

    @Test
    void acceptsThisAsWellAsThe() {
        String raw = "Header\n*** START OF THIS PROJECT GUTENBERG EBOOK X ***\nbody\n*** END OF THIS PROJECT GUTENBERG EBOOK X ***\n";
        assertEquals("body", splitter.split(10, raw).orElseThrow().body());
    }

    @Test
    void withoutAStartOrAnEndMarkerItIsEmpty() {
        assertTrue(splitter.split(10, "Header\nbody\n*** END OF THE PROJECT GUTENBERG EBOOK X ***\n").isEmpty());
        assertTrue(splitter.split(10, "Header\n*** START OF THE PROJECT GUTENBERG EBOOK X ***\nbody\n").isEmpty());
        assertTrue(splitter.split(10, "").isEmpty());
    }

    @Test
    void anEndMarkerBeforeTheStartDoesNotCount() {
        String raw = "*** END OF THE PROJECT GUTENBERG EBOOK X ***\n*** START OF THE PROJECT GUTENBERG EBOOK X ***\nbody\n";
        assertTrue(splitter.split(10, raw).isEmpty());
    }

    @Test
    void anEndMarkerOnTheStartLineIsEmptyNotAnException() {
        String raw = "H\n*** START OF THE PROJECT GUTENBERG EBOOK X *** *** END OF THE PROJECT GUTENBERG EBOOK X ***\n";
        assertTrue(splitter.split(10, raw).isEmpty());
    }

    @Test
    void normalizesCrlf() {
        String raw = "Header\r\n\r\n*** START OF THE PROJECT GUTENBERG EBOOK X ***\r\n\r\n"
                + "red\r\nboat\r\n\r\n*** END OF THE PROJECT GUTENBERG EBOOK X ***\r\n";
        RawBook book = splitter.split(10, raw).orElseThrow();
        assertEquals("red\nboat", book.body());
        assertFalse(book.header().contains("\r"));
    }

    /**
     * The 15 real books of stage_1/sample_dataset: raw/pg<id>.txt split here must give exactly
     * book/<id>/header.txt and body.txt, the expected output of Stage 1. Skipped if the Stage 1
     * repository is not next to this one.
     */
    @Test
    void theRealSampleBooksSplitExactlyAsInStage1() throws IOException {
        assumeTrue(Files.isDirectory(SAMPLE.resolve("raw")), "stage_1/sample_dataset not found next to stage_2");
        List<Path> raws;
        try (Stream<Path> files = Files.list(SAMPLE.resolve("raw"))) {
            raws = files.filter(p -> p.getFileName().toString().matches("pg\\d+\\.txt")).sorted().toList();
        }
        assertEquals(15, raws.size());
        for (Path raw : raws) {
            int id = Integer.parseInt(raw.getFileName().toString().replaceAll("\\D", ""));
            Optional<RawBook> book = splitter.split(id, Files.readString(raw, StandardCharsets.UTF_8));
            assertTrue(book.isPresent(), "book " + id + " has its markers");
            Path expected = SAMPLE.resolve("book").resolve(String.valueOf(id));
            assertEquals(Files.readString(expected.resolve("header.txt"), StandardCharsets.UTF_8), book.get().header(), "header of " + id);
            assertEquals(Files.readString(expected.resolve("body.txt"), StandardCharsets.UTF_8), book.get().body(), "body of " + id);
        }
    }
}
