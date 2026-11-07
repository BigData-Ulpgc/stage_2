package es.ulpgc.bigdata.ingestion.worker;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** What the workers of one replica have done since it started: shown in GET /status. */
public final class WorkerStats {

    final AtomicLong claimed = new AtomicLong();
    final AtomicLong downloaded = new AtomicLong();
    final AtomicLong alreadyAvailable = new AtomicLong();
    final AtomicLong requeued = new AtomicLong();
    final AtomicLong failed = new AtomicLong();
    final AtomicLong leaseLost = new AtomicLong();
    final AtomicLong fetchRetries = new AtomicLong();

    public Map<String, Object> asMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("claimed", claimed.get());
        map.put("downloaded", downloaded.get());
        map.put("already_available", alreadyAvailable.get());
        map.put("requeued", requeued.get());
        map.put("failed", failed.get());
        map.put("lease_lost", leaseLost.get());
        map.put("fetch_retries", fetchRetries.get());
        return map;
    }
}
