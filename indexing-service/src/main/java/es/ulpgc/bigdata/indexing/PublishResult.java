package es.ulpgc.bigdata.indexing;

/**
 * What Publisher.publish returns; IndexingApi turns it into the JSON of POST /index/publish.
 * When there was nothing new it describes CURRENT as it was (D5).
 *
 * @param version        the version CURRENT points to after the call; null if nothing has ever
 *                       been published
 * @param booksPublished books that became visible with this call; 0 if there was nothing new
 * @param totalBooks     books visible in {@code version}; 0 if nothing has ever been published
 * @param publishedAt    when {@code version} was published, ISO-8601 UTC to the second; null if
 *                       nothing has ever been published (or whoever wrote CURRENT did not say)
 */
public record PublishResult(Integer version, int booksPublished, long totalBooks, String publishedAt) {
}
