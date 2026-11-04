package es.ulpgc.bigdata.coordination;

import java.util.Objects;

/**
 * Message of index-events (contract §6): the same fields as the 200 of POST /index/publish.
 * Indexing publishes it after CURRENT has switched, only when a new version is created.
 *
 * It is only a wake-up: a Search replica that receives it reads CURRENT from MongoDB and
 * reloads only if the version is newer than the one it serves. A lost, repeated or
 * out-of-order message therefore does no harm.
 *
 * @param version        version that CURRENT points to now
 * @param booksPublished books that became visible in this version (> 0)
 * @param totalBooks     books visible in total
 * @param publishedAt    ISO-8601 UTC, e.g. 2026-11-03T10:15:00Z
 */
public record IndexPublished(int version, int booksPublished, int totalBooks, String publishedAt) {

    public IndexPublished {
        Objects.requireNonNull(publishedAt, "publishedAt");
    }
}
