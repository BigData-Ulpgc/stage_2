package es.ulpgc.bigdata.search.index;

import java.util.Map;
import java.util.Set;

/**
 * Where a request reads the posting lists of its terms. MongoPostingSource in production;
 * the unit tests give a map, so the search logic is tested without MongoDB.
 */
public interface PostingSource {

    /**
     * The posting list of each term found in the collection, ascending and without
     * repetitions. A term that is not in the index is NOT in the map. The lists may contain
     * books that are not published yet: the caller filters them with its snapshot.
     */
    Map<String, int[]> postings(String collection, Set<String> terms);
}
