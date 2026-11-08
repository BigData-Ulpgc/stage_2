package es.ulpgc.bigdata.indexing.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;

/**
 * How many books were indexed at each moment of a run of the indexing experiments (DR-13): one
 * sample per poll, in seconds since the run started. What the report needs comes out of it:
 *
 * <ul>
 *   <li>{@link #secondsToReach}: when the n-th book was indexed. With n = 1 it is the start-up
 *       of the service (JVM, Hazelcast, the first batch), which is not indexing speed.</li>
 *   <li>{@link #rate}: books per second between two fractions of the run. Between 10 % and 90 %
 *       it leaves out the start-up and the last batch, which may be incomplete, so it is the
 *       steady rate; per tenth it shows whether the rate falls as the posting lists grow.</li>
 * </ul>
 *
 * Not thread-safe: one poller adds the samples.
 */
public final class Progress {

    public record Sample(double seconds, long books) {
    }

    private final List<Sample> samples = new ArrayList<>();

    /** @throws IllegalArgumentException if time or books go backwards */
    public void add(double seconds, long books) {
        if (!samples.isEmpty()) {
            Sample last = samples.get(samples.size() - 1);
            if (seconds < last.seconds() || books < last.books()) {
                throw new IllegalArgumentException("samples must not go backwards: " + last + " then "
                        + new Sample(seconds, books));
            }
        }
        samples.add(new Sample(seconds, books));
    }

    public List<Sample> samples() {
        return List.copyOf(samples);
    }

    /** The first sample with at least {@code books} books, if it was reached. */
    public OptionalDouble secondsToReach(long books) {
        for (Sample s : samples) {
            if (s.books() >= books) {
                return OptionalDouble.of(s.seconds());
            }
        }
        return OptionalDouble.empty();
    }

    /**
     * Books per second between the moments {@code from} and {@code to} (fractions of
     * {@code total}) were reached: (books at the second moment - books at the first) / time.
     * {@code from} = 0 is the first sample, the start of the run. Empty if the run did not get
     * that far or no time passed.
     */
    public OptionalDouble rate(long total, double from, double to) {
        if (total <= 0 || from < 0 || to > 1 || from >= to) {
            throw new IllegalArgumentException("need 0 <= from < to <= 1 and total > 0");
        }
        Sample start = from == 0
                ? (samples.isEmpty() ? null : samples.get(0))    // the start of the run, start-up included
                : firstAtLeast((long) Math.ceil(total * from));
        Sample end = firstAtLeast((long) Math.ceil(total * to));
        if (start == null || end == null || end.seconds() <= start.seconds()) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of((end.books() - start.books()) / (end.seconds() - start.seconds()));
    }

    private Sample firstAtLeast(long books) {
        for (Sample s : samples) {
            if (s.books() >= books) {
                return s;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "Progress" + Objects.toString(samples);
    }
}
