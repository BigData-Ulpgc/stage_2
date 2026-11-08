package es.ulpgc.bigdata.indexing;

import java.time.Instant;

/**
 * What IndexingService.status returns, in raw units (bytes, Instant); IndexingApi turns it
 * into the JSON of GET /index/status (MB, ISO text).
 *
 * @param booksIndexed     books complete in the index, published or not: the books of the
 *                         index log (DR-07). A book whose update failed halfway does not count
 * @param lastUpdate       when a book was last indexed (the latest indexed_at of the index
 *                         log); null if none. Persistent: it survives a restart
 * @param indexSizeBytes   data + indexes of the inverted index collection; approximate on a
 *                         shared client (up to one MongoDB checkpoint behind)
 * @param publishedVersion version CURRENT points to; null if nothing is published yet
 * @param booksPublished   books visible in publishedVersion; 0 if nothing is published yet
 */
public record IndexStatus(long booksIndexed, Instant lastUpdate, long indexSizeBytes,
                          Integer publishedVersion, long booksPublished) {
}
