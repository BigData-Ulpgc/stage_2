package es.ulpgc.bigdata.ingestion;

import es.ulpgc.bigdata.ingestion.source.BookNotFoundException;
import es.ulpgc.bigdata.ingestion.source.BookSource;
import es.ulpgc.bigdata.ingestion.source.SourceUnavailableException;

import java.net.http.HttpTimeoutException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** A BookSource that answers what each test sets, and counts the fetches. Unknown ids are 404. */
public final class FakeSource implements BookSource {

    public static final String VALID_TEXT = """
            Title: Red Boat
            Author: Jane Doe

            *** START OF THE PROJECT GUTENBERG EBOOK RED BOAT ***
            red boat sails
            *** END OF THE PROJECT GUTENBERG EBOOK RED BOAT ***
            footer
            """;

    private final Map<Integer, Object> answers = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> failuresLeft = new ConcurrentHashMap<>();
    private final Map<Integer, Runnable> duringFetch = new ConcurrentHashMap<>();
    private final Map<Integer, AtomicInteger> perBook = new ConcurrentHashMap<>();
    public final AtomicInteger fetches = new AtomicInteger();

    public FakeSource book(int id) {
        answers.put(id, VALID_TEXT);
        return this;
    }

    public FakeSource text(int id, String text) {
        answers.put(id, text);
        return this;
    }

    public FakeSource unavailable(int id) {
        answers.put(id, new SourceUnavailableException("HTTP 503 from the fake source"));
        return this;
    }

    public FakeSource timeout(int id) {
        answers.put(id, new SourceUnavailableException("timeout (30 s) from the fake source",
                new HttpTimeoutException("request timed out")));
        return this;
    }

    /** The source refuses us (429 or 403). */
    public FakeSource throttled(int id, int status) {
        answers.put(id, new SourceUnavailableException("HTTP " + status + " from the fake source", status));
        return this;
    }

    /** The first {@code times} fetches of the book fail with a 503, then it is served. */
    public FakeSource failsFirst(int id, int times) {
        failuresLeft.put(id, new AtomicInteger(times));
        return book(id);
    }

    /** Runs before every fetch of the book (to make something happen during a download). */
    public FakeSource whileFetching(int id, Runnable action) {
        duringFetch.put(id, action);
        return this;
    }

    public int fetchesOf(int id) {
        return perBook.getOrDefault(id, new AtomicInteger()).get();
    }

    @Override
    public String fetch(int bookId) throws BookNotFoundException, SourceUnavailableException {
        fetches.incrementAndGet();
        perBook.computeIfAbsent(bookId, k -> new AtomicInteger()).incrementAndGet();
        Runnable action = duringFetch.get(bookId);
        if (action != null) {
            action.run();
        }
        AtomicInteger left = failuresLeft.get(bookId);
        if (left != null && left.getAndDecrement() > 0) {
            throw new SourceUnavailableException("HTTP 503 from the fake source", 503);
        }
        Object answer = answers.get(bookId);
        if (answer == null) {
            throw new BookNotFoundException(bookId, "HTTP 404 from the fake source");
        }
        if (answer instanceof SourceUnavailableException e) {
            throw e;
        }
        return (String) answer;
    }

    @Override
    public String describe() {
        return "fake";
    }
}
