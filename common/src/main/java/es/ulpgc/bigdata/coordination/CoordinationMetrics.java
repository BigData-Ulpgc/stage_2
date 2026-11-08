package es.ulpgc.bigdata.coordination;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * What coordinating the books cost this process, per stage (CF-12, enunciado §6.4 "coordination
 * cost"). Coordination records it at each transition; the services show it in GET /status.
 * Each one is a LatencyHistogram, since the process started:
 * <ul>
 *   <li><b>queue_wait</b>: from the moment the id entered the queue (BookTask.enqueuedAt) to its
 *       claim. Time a book waited for a free worker.</li>
 *   <li><b>claim</b>: from the start of the transaction whose poll took the id to its commit:
 *       poll, getForUpdate, set and the two-phase commit. If the queue was empty when the poll
 *       began, it also includes the wait for the id to arrive (up to Coordination.MAX_POLL);
 *       under load the queue is never empty.</li>
 *   <li><b>work</b>: from the claim (BookTask.startedAt) to the moment complete() is called. The
 *       real work of the worker: download, split and save, or index.</li>
 *   <li><b>complete</b>: the complete() call itself (the replace, or for INGESTION the
 *       transaction with the offer to index-queue).</li>
 * </ul>
 * claim + complete is what Hazelcast adds to every book; queue_wait says whether there were too
 * few workers. Only successful transitions are recorded: a claim that found nothing, a lost
 * lease or a failure are not. enqueuedAt and startedAt may come from another process (the
 * Control seeds, Ingestion offers to index-queue): on the single Stage 2 host they share the
 * clock.
 */
public final class CoordinationMetrics {

    private final Map<Stage, LatencyHistogram> queueWait = perStage();
    private final Map<Stage, LatencyHistogram> claim = perStage();
    private final Map<Stage, LatencyHistogram> work = perStage();
    private final Map<Stage, LatencyHistogram> complete = perStage();

    public LatencyHistogram queueWait(Stage stage) {
        return queueWait.get(stage);
    }

    public LatencyHistogram claim(Stage stage) {
        return claim.get(stage);
    }

    public LatencyHistogram work(Stage stage) {
        return work.get(stage);
    }

    public LatencyHistogram complete(Stage stage) {
        return complete.get(stage);
    }

    /** {"ingestion": {"queue_wait": {...}, "claim": {...}, "work": {...}, "complete": {...}}, "indexing": ...} */
    public Map<String, Object> summary() {
        Map<String, Object> summary = new LinkedHashMap<>();
        for (Stage stage : Stage.values()) {
            summary.put(stage.name().toLowerCase(Locale.ROOT), summary(stage));
        }
        return summary;
    }

    /** {"queue_wait": {...}, "claim": {...}, "work": {...}, "complete": {...}}: the stage a service works on. */
    public Map<String, Object> summary(Stage stage) {
        Map<String, Object> ofStage = new LinkedHashMap<>();
        ofStage.put("queue_wait", queueWait(stage).summary());
        ofStage.put("claim", claim(stage).summary());
        ofStage.put("work", work(stage).summary());
        ofStage.put("complete", complete(stage).summary());
        return ofStage;
    }

    private static Map<Stage, LatencyHistogram> perStage() {
        Map<Stage, LatencyHistogram> histograms = new EnumMap<>(Stage.class);
        for (Stage stage : Stage.values()) {
            histograms.put(stage, new LatencyHistogram());
        }
        return histograms;
    }
}
