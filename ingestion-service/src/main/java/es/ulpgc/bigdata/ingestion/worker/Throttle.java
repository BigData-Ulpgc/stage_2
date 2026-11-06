package es.ulpgc.bigdata.ingestion.worker;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

/**
 * The pause of the whole replica when the source throttles us (HTTP 429, or the 403 of
 * Gutenberg's anti-bot protection). Shared by every worker of the replica: while it lasts,
 * nobody claims books, so the books in the queue keep their attempts instead of all failing at
 * once (review of #20).
 *
 * The pause starts at 30 s and doubles at each new throttling, up to 10 min; a successful
 * download brings it back to 30 s. Several workers throttled at the same moment start ONE
 * pause, not one each.
 */
public final class Throttle {

    static final Duration FIRST_PAUSE = Duration.ofSeconds(30);
    static final Duration MAX_PAUSE = Duration.ofMinutes(10);

    private final Clock clock;
    private long pausedUntil;
    private Duration nextPause = FIRST_PAUSE;
    private long pauses;

    public Throttle(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** The source refused a download: pause the replica, unless it is already paused. */
    public synchronized void throttled() {
        long now = clock.millis();
        if (now < pausedUntil) {
            return;
        }
        pausedUntil = now + nextPause.toMillis();
        nextPause = nextPause.multipliedBy(2).compareTo(MAX_PAUSE) > 0 ? MAX_PAUSE : nextPause.multipliedBy(2);
        pauses++;
    }

    /** A download went through: the next pause, if any, is short again. */
    public synchronized void succeeded() {
        nextPause = FIRST_PAUSE;
    }

    /** How long the replica must still wait; zero when it may claim. */
    public synchronized Duration remaining() {
        return Duration.ofMillis(Math.max(0, pausedUntil - clock.millis()));
    }

    /** Pauses started since the replica began (GET /status, CF-12). */
    public synchronized long pauses() {
        return pauses;
    }
}
