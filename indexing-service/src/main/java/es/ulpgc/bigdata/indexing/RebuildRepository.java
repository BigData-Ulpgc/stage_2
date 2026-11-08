package es.ulpgc.bigdata.indexing;

import java.util.List;
import java.util.Optional;

/**
 * What a rebuild (DR-10) keeps in MongoDB besides the index: its progress (index_meta REBUILD)
 * and the collections of the database, to find the next free inverted_index_rNN and drop the
 * ones nobody reads.
 */
public interface RebuildRepository {

    /** The last rebuild started in the cluster; empty if there was none. */
    Optional<Rebuild> last();

    /** Replaces the last rebuild. */
    void save(Rebuild rebuild);

    /** The names of the collections of the database. */
    List<String> collections();

    /** Drops a collection; nothing if it does not exist. */
    void drop(String collection);
}
