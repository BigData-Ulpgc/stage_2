package es.ulpgc.bigdata.ingestion.worker;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ThrottleTest {

    private final MutableClock clock = new MutableClock();
    private final Throttle throttle = new Throttle(clock);

    @Test
    void withoutThrottlingThereIsNoPause() {
        assertEquals(Duration.ZERO, throttle.remaining());
    }

    @Test
    void theFirstPauseIs30Seconds() {
        throttle.throttled();
        assertEquals(Duration.ofSeconds(30), throttle.remaining());
        clock.advance(Duration.ofSeconds(30));
        assertEquals(Duration.ZERO, throttle.remaining());
    }

    @Test
    void severalWorkersThrottledAtOnceStartOnePause() {
        for (int worker = 0; worker < 4; worker++) {
            throttle.throttled();
        }
        assertEquals(Duration.ofSeconds(30), throttle.remaining());
        assertEquals(1, throttle.pauses());
    }

    @Test
    void eachNewThrottlingDoublesThePauseUpToTenMinutes() {
        long[] expected = {30, 60, 120, 240, 480, 600, 600};
        for (long seconds : expected) {
            throttle.throttled();
            assertEquals(Duration.ofSeconds(seconds), throttle.remaining());
            clock.advance(throttle.remaining());
        }
    }

    @Test
    void aSuccessfulDownloadMakesTheNextPauseShortAgain() {
        throttle.throttled();
        clock.advance(Duration.ofSeconds(30));
        throttle.throttled();
        clock.advance(Duration.ofSeconds(60));
        throttle.succeeded();
        throttle.throttled();
        assertEquals(Duration.ofSeconds(30), throttle.remaining());
    }
}
