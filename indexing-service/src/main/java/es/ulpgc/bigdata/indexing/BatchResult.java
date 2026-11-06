package es.ulpgc.bigdata.indexing;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * What IndexingService.indexBatch did with each book of a batch (DR-09).
 *
 * @param indexed  the books indexed, complete in the index and in the log, in the order asked
 * @param rejected the books that could not be prepared, with the exception update would have
 *                 thrown for each one; nothing of them was written
 */
public record BatchResult(List<UpdateResult> indexed, Map<Integer, RuntimeException> rejected) {

    public BatchResult {
        indexed = List.copyOf(Objects.requireNonNull(indexed, "indexed"));
        rejected = Map.copyOf(Objects.requireNonNull(rejected, "rejected"));
    }
}
