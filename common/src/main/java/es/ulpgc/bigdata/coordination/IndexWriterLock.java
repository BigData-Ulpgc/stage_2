package es.ulpgc.bigdata.coordination;

import com.hazelcast.map.IMap;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * The lock of the single index writer (contract §5): only the Indexing replica that holds it
 * claims from index-queue and writes the index; another replica waits in {@link #lock()} as a
 * standby.
 *
 * It is IMap.lock on the map "locks", NOT a CP FencedLock: the CP Subsystem is an Enterprise
 * feature in Hazelcast 5.7 and getCPSubsystem().getLock() throws UnsupportedOperationException
 * (measured in CF-01). Consequences for the user of this class:
 * <ul>
 *   <li>The lock belongs to the THREAD that took it: the same thread must unlock it.</li>
 *   <li>Hazelcast releases it when the holder's connection closes (measured 5-10 s after a
 *       kill -9; up to the 60 s client heartbeat timeout if the process hangs).</li>
 *   <li>If the client loses the cluster, stop writing at once
 *       ({@link Coordination#onConnectionChange}): when the cluster is back the lock may be
 *       gone. Take it again before writing anything else.</li>
 *   <li>No fencing token: a writer that froze and comes back late is not stopped by
 *       Hazelcast. Its claims fail once the Control has requeued its books, and every write
 *       is idempotent, so the damage is limited to repeated work.</li>
 * </ul>
 */
public final class IndexWriterLock {

    private final IMap<String, String> locks;

    IndexWriterLock(IMap<String, String> locks) {
        this.locks = locks;
    }

    /** Blocks until this thread holds the lock. Reentrant. */
    public void lock() {
        locks.lock(CoordinationNames.INDEX_WRITER_LOCK);
    }

    /** Waits at most {@code wait}; true if this thread now holds the lock. */
    public boolean tryLock(Duration wait) throws InterruptedException {
        return locks.tryLock(CoordinationNames.INDEX_WRITER_LOCK, wait.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Releases one hold of the lock; must be called by the thread that holds it. */
    public void unlock() {
        locks.unlock(CoordinationNames.INDEX_WRITER_LOCK);
    }

    /** Whether some writer (this one or another) holds the lock now. */
    public boolean isLocked() {
        return locks.isLocked(CoordinationNames.INDEX_WRITER_LOCK);
    }
}
