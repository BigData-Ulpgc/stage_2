package es.ulpgc.bigdata.coordination;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Durations recorded since the process started, in microseconds, with their count, mean, max
 * and percentiles (CF-12). Lock-free: every worker records into the same instance.
 *
 * The percentiles come from logarithmic buckets: exact below 16 µs, then 16 buckets per power of
 * two. A percentile is the middle of its bucket, so it is off by at most 1/32 (about 3 %) of the
 * true value; the count, the mean, the max (and so the percentile of the last sample) are exact.
 * 960 buckets cover every long.
 */
public final class LatencyHistogram {

    private static final int SUB_BUCKET_BITS = 4;
    private static final int SUB_BUCKETS = 1 << SUB_BUCKET_BITS;
    private static final int BUCKETS = SUB_BUCKETS + (63 - SUB_BUCKET_BITS) * SUB_BUCKETS;

    private final AtomicLongArray buckets = new AtomicLongArray(BUCKETS);
    private final AtomicLong count = new AtomicLong();
    private final AtomicLong sum = new AtomicLong();
    private final AtomicLong max = new AtomicLong();

    /** A negative duration (two clocks a little apart) counts as 0. */
    public void record(long micros) {
        long value = Math.max(0, micros);
        buckets.incrementAndGet(bucket(value));
        count.incrementAndGet();
        sum.addAndGet(value);
        max.accumulateAndGet(value, Math::max);
    }

    public long count() {
        return count.get();
    }

    /**
     * The value under which a fraction {@code q} of the durations fall, in microseconds; 0 if
     * nothing was recorded.
     */
    public long percentile(double q) {
        if (q < 0 || q > 1) {
            throw new IllegalArgumentException("q must be in [0, 1]: " + q);
        }
        long total = count.get();
        if (total == 0) {
            return 0;
        }
        long rank = Math.max(1, (long) Math.ceil(q * total));
        if (rank >= total) {
            return max.get();                           // the highest one is known exactly
        }
        long seen = 0;
        for (int i = 0; i < BUCKETS; i++) {
            seen += buckets.get(i);
            if (seen >= rank) {
                return Math.min(middle(i), max.get());
            }
        }
        return max.get();                               // a record() in flight: counted, not yet in a bucket
    }

    /**
     * count, and mean, p50, p95, p99 and max in milliseconds with 3 decimals: the form of
     * GET /status. Read while other threads record, the fields may be a few samples apart.
     */
    public Map<String, Object> summary() {
        Map<String, Object> summary = new LinkedHashMap<>();
        long total = count.get();
        summary.put("count", total);
        summary.put("mean_ms", total == 0 ? 0.0 : millis((double) sum.get() / total));
        summary.put("p50_ms", millis(percentile(0.50)));
        summary.put("p95_ms", millis(percentile(0.95)));
        summary.put("p99_ms", millis(percentile(0.99)));
        summary.put("max_ms", millis(max.get()));
        return summary;
    }

    static int bucket(long value) {
        if (value < SUB_BUCKETS) {
            return (int) value;
        }
        int exponent = 63 - Long.numberOfLeadingZeros(value);           // >= SUB_BUCKET_BITS
        int sub = (int) (value >>> (exponent - SUB_BUCKET_BITS)) & (SUB_BUCKETS - 1);
        return SUB_BUCKETS + (exponent - SUB_BUCKET_BITS) * SUB_BUCKETS + sub;
    }

    /** The middle of a bucket: the bucket's own value below 16. */
    static long middle(int bucket) {
        if (bucket < SUB_BUCKETS) {
            return bucket;
        }
        int exponent = (bucket - SUB_BUCKETS) / SUB_BUCKETS + SUB_BUCKET_BITS;
        int sub = (bucket - SUB_BUCKETS) % SUB_BUCKETS;
        long width = 1L << (exponent - SUB_BUCKET_BITS);
        long low = (1L << exponent) + sub * width;
        return low + width / 2;
    }

    private static double millis(double micros) {
        return Math.round(micros) / 1000.0;
    }
}
