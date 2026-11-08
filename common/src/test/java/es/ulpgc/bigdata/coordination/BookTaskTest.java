package es.ulpgc.bigdata.coordination;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BookTaskTest {

    @Test
    void aStateThatIsNotABookStateIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new BookTask(1, "DONE", null, 0, 0, null, null, 0, 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new BookTask(1, "FAILED", null, 0, 0, "PARSING", null, 0, 0, 0));
    }

    @Test
    void aCompletedStageClearsTheLeaseAndTheAttempts() {
        BookTask claimed = BookTask.pending(7, 1_000).claimed(Stage.INGESTION, "w", 61_000, 2_000)
                .requeued(Stage.INGESTION, 1, "DOWNLOAD_FAILED", 3_000)
                .claimed(Stage.INGESTION, "w", 64_000, 4_000);
        BookTask done = claimed.completed(Stage.INGESTION, 5_000, true);
        assertEquals(BookState.DOWNLOADED, done.bookState());
        assertNull(done.owner());
        assertEquals(0, done.leaseUntil());
        assertEquals(0, done.attempts());
        assertEquals(5_000, done.enqueuedAt());       // offered to index-queue now
        assertEquals(4_000, done.startedAt());
        assertEquals(5_000, done.finishedAt());
    }

    @Test
    void aRetriedBookStartsTheStageThatFailedAgain() {
        BookTask failed = BookTask.pending(7, 0).failed(Stage.INDEXING, 3, "EMPTY_BOOK");
        BookTask retried = failed.retried(10_000);
        assertEquals(BookState.DOWNLOADED, retried.bookState());
        assertEquals(0, retried.attempts());
        assertNull(retried.failedStage());
        assertEquals("EMPTY_BOOK", retried.lastError());
    }

    @Test
    void theErrorTextIsCodeAndMessage() {
        assertEquals("NOT_FOUND: HTTP 404", BookTask.error(ErrorCode.NOT_FOUND, " HTTP 404 "));
        assertEquals("EMPTY_BOOK", BookTask.error(ErrorCode.EMPTY_BOOK, null));
        assertEquals(BookTask.MAX_ERROR_LENGTH, BookTask.error(ErrorCode.INTERNAL_ERROR, "x".repeat(1_000)).length());
    }
}
