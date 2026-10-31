package today.bonfire.oss.bth4j.admin.http;

import java.time.Clock;
import java.util.concurrent.ConcurrentHashMap;

/** Blocks a remote address for a while after repeated failed logins. */
final class LoginThrottle {

    private record Entry(int failures, long blockedUntil, long lastFailure) {}

    private static final int MAX_FAILURES = 5;
    private static final long BLOCK_MILLIS = 10_000;
    private static final long FORGET_MILLIS = 600_000;
    private static final int MAX_ENTRIES = 10_000;

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private final Clock clock;

    LoginThrottle(Clock clock) {
        this.clock = clock;
    }

    /** @return milliseconds the address must wait, 0 when it may try */
    long retryAfterMillis(String address) {
        var entry = entries.get(address);
        return entry == null ? 0 : Math.max(0, entry.blockedUntil - clock.millis());
    }

    void failure(String address) {
        var now = clock.millis();
        if (entries.size() >= MAX_ENTRIES) {
            entries.values().removeIf(e -> now - e.lastFailure > FORGET_MILLIS && e.blockedUntil < now);
            if (entries.size() >= MAX_ENTRIES) {
                entries.clear();
            }
        }
        entries.merge(address, new Entry(1, 0, now), (old, ignored) -> {
            var failures = old.failures + 1;
            return failures >= MAX_FAILURES
                    ? new Entry(0, now + BLOCK_MILLIS, now)
                    : new Entry(failures, old.blockedUntil, now);
        });
    }

    void success(String address) {
        entries.remove(address);
    }
}
