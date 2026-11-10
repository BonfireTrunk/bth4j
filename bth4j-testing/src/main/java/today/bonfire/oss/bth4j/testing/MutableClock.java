package today.bonfire.oss.bth4j.testing;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.locks.ReentrantLock;

/** A clock that only moves when the test says so. */
public final class MutableClock extends Clock {

    private volatile Instant now;

    public MutableClock(Instant start) {
        this.now = start;
    }

    public MutableClock() {
        this(Instant.parse("2030-01-01T00:00:00Z"));
    }

    private final ReentrantLock lock = new ReentrantLock();

    public void set(Instant instant) {
        lock.lock();
        try {
            this.now = instant;
        } finally {
            lock.unlock();
        }
    }

    public void advance(Duration duration) {
        lock.lock();
        try {
            this.now = now.plus(duration);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    /** @return a clock in that zone that shows this clock's time, also after it moves */
    @Override
    public Clock withZone(ZoneId zone) {
        var source = this;
        return new Clock() {
            @Override
            public ZoneId getZone() {
                return zone;
            }

            @Override
            public Clock withZone(ZoneId other) {
                return source.withZone(other);
            }

            @Override
            public Instant instant() {
                return source.instant();
            }
        };
    }

    @Override
    public Instant instant() {
        return now;
    }
}
