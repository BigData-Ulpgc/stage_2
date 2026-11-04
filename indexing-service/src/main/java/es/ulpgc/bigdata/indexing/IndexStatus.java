package es.ulpgc.bigdata.indexing;

import java.time.Instant;

/**
 * What IndexingService.status returns, in raw units (bytes, Instant); IndexingApi turns it
 * into the JSON of GET /index/status (MB, ISO text).
 *
 * @param booksIndexed     books indexed, published or not. Provisional until DR-07: documents
 *                         of the metadata collection (index_log will make it exact)
 * @param lastUpdate       end of the last successful update; null if none. Provisional until
 *                         DR-07: kept in memory, so it is null again after a restart
 * @param indexSizeBytes   data + indexes of the inverted index collection; approximate on a
 *                         shared client (up to one MongoDB checkpoint behind)
 * @param publishedVersion version CURRENT points to; null if nothing is published yet
 * @param booksPublished   books visible in publishedVersion; 0 if nothing is published yet
 */
public record IndexStatus(long booksIndexed, Instant lastUpdate, long indexSizeBytes,
                          Integer publishedVersion, long booksPublished) {
}
