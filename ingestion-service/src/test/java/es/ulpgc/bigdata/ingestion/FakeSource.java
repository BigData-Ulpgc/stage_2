package es.ulpgc.bigdata.ingestion;

import es.ulpgc.bigdata.ingestion.source.BookNotFoundException;
import es.ulpgc.bigdata.ingestion.source.BookSource;
import es.ulpgc.bigdata.ingestion.source.SourceUnavailableException;

import java.net.http.HttpTimeoutException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** A BookSource that answers what each test sets, and counts the fetches. Unknown ids are 404. */
final class FakeSource implements BookSource {

    static final String VALID_TEXT = """
            Title: Red Boat
            Author: Jane Doe

            *** START OF THE PROJECT GUTENBERG EBOOK RED BOAT ***
            red boat sails
            *** END OF THE PROJECT GUTENBERG EBOOK RED BOAT ***
            footer
            """;

    private final Map<Integer, Object> answers = new ConcurrentHashMap<>();
    final AtomicInteger fetches = new AtomicInteger();

    FakeSource book(int id) {
        answers.put(id, VALID_TEXT);
        return this;
    }

    FakeSource text(int id, String text) {
        answers.put(id, text);
        return this;
    }

    FakeSource unavailable(int id) {
        answers.put(id, new SourceUnavailableException("HTTP 503 from the fake source"));
        return this;
    }

    FakeSource timeout(int id) {
        answers.put(id, new SourceUnavailableException("timeout (30 s) from the fake source",
                new HttpTimeoutException("request timed out")));
        return this;
    }

    @Override
    public String fetch(int bookId) throws BookNotFoundException, SourceUnavailableException {
        fetches.incrementAndGet();
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
