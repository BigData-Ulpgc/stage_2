package es.ulpgc.bigdata.ingestion;

import es.ulpgc.bigdata.datalake.RangeBasedDatalake;
import es.ulpgc.bigdata.ingestion.source.BookNotFoundException;
import es.ulpgc.bigdata.ingestion.source.SourceUnavailableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IngestionServiceTest {

    @TempDir
    Path dataDir;

    private final FakeSource source = new FakeSource();
    private RangeBasedDatalake datalake;
    private IngestionService service;

    @BeforeEach
    void setUp() {
        datalake = new RangeBasedDatalake(dataDir.resolve("datalake/range"));
        service = new IngestionService(source, new BookSplitter(), datalake);
    }

    @Test
    void aNewBookIsDownloadedSplitAndSaved() throws Exception {
        source.book(1342);
        IngestionService.IngestResult result = service.ingest(1342);
        assertEquals(IngestionService.Outcome.DOWNLOADED, result.outcome());
        assertEquals("Title: Red Boat\nAuthor: Jane Doe", Files.readString(result.location().headerPath()));
        assertEquals("red boat sails", Files.readString(result.location().bodyPath()));
        assertTrue(service.locate(1342).isPresent());
    }

    @Test
    void aBookAlreadyInTheDatalakeIsNotDownloadedAgain() throws Exception {
        source.book(1342);
        service.ingest(1342);
        IngestionService.IngestResult again = service.ingest(1342);
        assertEquals(IngestionService.Outcome.ALREADY_AVAILABLE, again.outcome());
        assertEquals(1, source.fetches.get());
    }

    @Test
    void aBookTheSourceDoesNotHaveWritesNothing() {
        assertThrows(BookNotFoundException.class, () -> service.ingest(7));
        assertTrue(service.locate(7).isEmpty());
    }

    @Test
    void aBookWithoutMarkersIsInvalidAndWritesNothing() {
        source.text(8, "just some text without markers");
        InvalidBookException e = assertThrows(InvalidBookException.class, () -> service.ingest(8));
        assertEquals(8, e.bookId());
        assertTrue(service.locate(8).isEmpty());
        assertTrue(service.list().isEmpty());
    }

    @Test
    void anUnavailableSourceWritesNothingAndCanBeRetried() throws Exception {
        source.unavailable(9);
        assertThrows(SourceUnavailableException.class, () -> service.ingest(9));
        assertTrue(service.locate(9).isEmpty());
        source.book(9);                                     // the source is back
        assertEquals(IngestionService.Outcome.DOWNLOADED, service.ingest(9).outcome());
    }

    @Test
    void theListIsSortedAndHasOnlyCompleteBooks() throws Exception {
        source.book(2701).book(84).book(1342);
        service.ingest(2701);
        service.ingest(84);
        service.ingest(1342);
        assertEquals(List.of(84, 1342, 2701), service.list());
    }

    @Test
    void anInvalidIdIsABugOfTheCaller() {
        assertThrows(IllegalArgumentException.class, () -> service.ingest(0));
        assertThrows(IllegalArgumentException.class, () -> service.locate(1_000_000_000));
    }

    // --- Readiness (api/contracts/datalake-readiness.md, CF-07) ---------------------------

    /** Contract §4: the process died after the body and before the marker. */
    @Test
    void aBookWithoutItsMarkerIsNotAvailableAndIsWrittenAgainWhole() throws Exception {
        source.book(1342);
        Path marker = marker(service.ingest(1342));
        Files.delete(marker);

        assertTrue(service.locate(1342).isEmpty());
        assertTrue(service.list().isEmpty());

        IngestionService.IngestResult again = service.ingest(1342);
        assertEquals(IngestionService.Outcome.DOWNLOADED, again.outcome());      // not already_available
        assertEquals(2, source.fetches.get());
        assertTrue(Files.isRegularFile(marker));                                 // the marker is back, last
        assertEquals("red boat sails", Files.readString(again.location().bodyPath()));
    }

    /** A marker whose body was lost (deleted by hand) is a damaged book: absent, and repaired. */
    @Test
    void aBookWhoseBodyWasLostIsRepaired() throws Exception {
        source.book(84);
        Path body = service.ingest(84).location().bodyPath();
        Files.delete(body);

        assertTrue(service.locate(84).isEmpty());
        assertEquals(IngestionService.Outcome.DOWNLOADED, service.ingest(84).outcome());
        assertEquals("red boat sails", Files.readString(body));
    }

    /** A ready book is never modified: ingesting it again writes nothing. */
    @Test
    void ingestingAReadyBookAgainDoesNotTouchItsFiles() throws Exception {
        source.book(11);
        IngestionService.IngestResult first = service.ingest(11);
        BasicFileAttributes before = Files.readAttributes(first.location().bodyPath(), BasicFileAttributes.class);
        String markerBefore = Files.readString(marker(first));

        assertEquals(IngestionService.Outcome.ALREADY_AVAILABLE, service.ingest(11).outcome());

        BasicFileAttributes after = Files.readAttributes(first.location().bodyPath(), BasicFileAttributes.class);
        assertEquals(before.lastModifiedTime(), after.lastModifiedTime());
        assertEquals(before.fileKey(), after.fileKey());                         // the same file (inode), not a new one
        assertEquals(markerBefore, Files.readString(marker(first)));             // not even the date of the marker
    }

    /** Where the datalake puts the marker of a book: next to its header (contract §1). */
    private static Path marker(IngestionService.IngestResult result) {
        return result.location().headerPath().resolveSibling(result.bookId() + "._READY");
    }
}
