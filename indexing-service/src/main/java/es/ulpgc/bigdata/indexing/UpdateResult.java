package es.ulpgc.bigdata.indexing;

/**
 * What IndexingService.update returns: the book is indexed (metadata and posting lists in
 * the store), not yet published. terms = distinct terms of the body after tokenizing.
 */
public record UpdateResult(int bookId, int terms) {
}
