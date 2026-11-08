package es.ulpgc.bigdata.coordination;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LatencyHistogramTest {

    @Test
    void emptyItSaysZero() {
        LatencyHistogram histogram = new LatencyHistogram();
        assertEquals(0, histogram.percentile(0.99));
        assertEquals(Map.of("count", 0L, "mean_ms", 0.0, "p50_ms", 0.0, "p95_ms", 0.0, "p99_ms", 0.0, "max_ms", 0.0),
                histogram.summary());
    }

    @Test
    void smallValuesAreExact() {
        LatencyHistogram histogram = new LatencyHistogram();
        for (int value = 1; value <= 10; value++) {
            histogram.record(value);
        }
        assertEquals(5, histogram.percentile(0.50));
        assertEquals(10, histogram.percentile(0.95));
        assertEquals(1, histogram.percentile(0.0));
    }

    @Test
    void percentilesAreWithinThreePerCentOfTheTrueOnes() {
        LatencyHistogram histogram = new LatencyHistogram();
        long[] values = new long[100_000];
        Random random = new Random(42);
        for (int i = 0; i < values.length; i++) {
            values[i] = (long) Math.exp(random.nextDouble() * Math.log(60_000_000));   // 1 µs to 1 min
            histogram.record(values[i]);
        }
        java.util.Arrays.sort(values);
        for (double q : new double[] {0.5, 0.9, 0.95, 0.99, 0.999}) {
            long exact = values[(int) Math.ceil(q * values.length) - 1];
            long estimate = histogram.percentile(q);
            assertTrue(Math.abs(estimate - exact) <= Math.max(1, exact / 32.0),
                    "p" + q + ": " + estimate + " vs " + exact);
        }
    }

    @Test
    void theSummaryIsInMillisecondsAndTheMaxIsExact() {
        LatencyHistogram histogram = new LatencyHistogram();
        histogram.record(1_500);
        histogram.record(2_500);
        histogram.record(123_456);
        Map<String, Object> summary = histogram.summary();
        assertEquals(3L, summary.get("count"));
        assertEquals(42.485, summary.get("mean_ms"));
        assertEquals(123.456, summary.get("max_ms"));
        assertEquals(123.456, summary.get("p99_ms"), "never above the max");
    }

    @Test
    void aNegativeDurationCountsAsZero() {
        LatencyHistogram histogram = new LatencyHistogram();
        histogram.record(-5);
        assertEquals(1, histogram.count());
        assertEquals(0, histogram.percentile(1.0));
    }

    @Test
    void everyBucketMapsBackIntoItself() {
        for (long value : new long[] {0, 15, 16, 17, 31, 32, 1_000, 1L << 40, Long.MAX_VALUE}) {
            int bucket = LatencyHistogram.bucket(value);
            assertEquals(bucket, LatencyHistogram.bucket(LatencyHistogram.middle(bucket)), "value " + value);
        }
        assertThrows(IllegalArgumentException.class, () -> new LatencyHistogram().percentile(1.5));
    }

    @Test
    void concurrentRecordsAreAllCounted() throws Exception {
        LatencyHistogram histogram = new LatencyHistogram();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        for (int t = 0; t < 8; t++) {
            pool.execute(() -> {
                for (int i = 0; i < 10_000; i++) {
                    histogram.record(i);
                }
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(80_000, histogram.count());
        assertEquals(9_999, histogram.percentile(1.0));
    }
}
