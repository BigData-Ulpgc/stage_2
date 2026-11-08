package es.ulpgc.bigdata.indexing;

/**
 * The published version of the index, as the CURRENT document of index_meta says (§6.5):
 * Search shows the books published with a version <= version.
 *
 * @param version     the visible version
 * @param books       how many books are visible in it
 * @param collection  the collection of the inverted index that version uses (DR-10 changes it
 *                    with a rebuild); null if whoever wrote CURRENT did not say
 * @param publishedAt when it was published, ISO-8601 UTC to the second ("2026-11-03T10:15:00Z");
 *                    null if whoever wrote CURRENT did not say
 */
public record Publication(int version, long books, String collection, String publishedAt) {

    /** Only what GET /index/status needs. */
    public Publication(int version, long books) {
        this(version, books, null, null);
    }
}
