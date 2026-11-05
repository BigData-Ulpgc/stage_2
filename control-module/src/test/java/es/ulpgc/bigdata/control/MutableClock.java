package es.ulpgc.bigdata.control;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicLong;

/** A clock the test moves by hand: leases expire and publication intervals pass without waiting. */
final class MutableClock extends Clock {

    private final AtomicLong millis = new AtomicLong(Instant.parse("2026-11-04T10:00:00Z").toEpochMilli());

    void advance(Duration d) {
        millis.addAndGet(d.toMillis());
    }

    @Override
    public long millis() {
        return millis.get();
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(millis());
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }
}
