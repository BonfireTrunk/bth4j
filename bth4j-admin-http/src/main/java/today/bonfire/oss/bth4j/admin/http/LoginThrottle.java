package today.bonfire.oss.bth4j.admin.http;

import java.time.Clock;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Blocks a remote address for a while after repeated failed logins. A successful login takes back only its own
 * attempt, so logging in with one account does not clear the failures counted against another.
 */
final class LoginThrottle {

    private record Entry(int attempts, long blockedUntil, long lastAttempt) {}

    private static final int MAX_FAILURES = 5;
    private static final long BLOCK_MILLIS = 10_000;
    private static final long FORGET_MILLIS = 600_000;
    private static final int MAX_ENTRIES = 10_000;
    private static final int EVICT_AT_CAP = MAX_ENTRIES / 10;

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private final Clock clock;

    LoginThrottle(Clock clock) {
        this.clock = clock;
    }

    /**
     * Checks and counts one login attempt in a single step per address, before the password is verified, so
     * concurrent attempts cannot all pass a block. {@link #success} takes this attempt back.
     *
     * @return milliseconds the address must wait, 0 when this attempt may go ahead
     */
    long attempt(String address) {
        var now = clock.millis();
        if (!entries.containsKey(address) && entries.size() >= MAX_ENTRIES) {
            makeRoom(now);
        }
        var wait = new long[1];
        entries.compute(address, (key, old) -> {
            if (old != null && old.blockedUntil > now) {
                wait[0] = old.blockedUntil - now;
                return old;
            }
            var attempts = old == null ? 1 : old.attempts + 1;
            return attempts >= MAX_FAILURES ? new Entry(0, now + BLOCK_MILLIS, now) : new Entry(attempts, 0, now);
        });
        return wait[0];
    }

    /** Takes back the attempt that just succeeded, and the block it set when it was the fifth. */
    void success(String address) {
        entries.computeIfPresent(address, (key, e) -> {
            var attempts = (e.blockedUntil > 0 ? MAX_FAILURES : e.attempts) - 1;
            return attempts <= 0 ? null : new Entry(attempts, 0, e.lastAttempt);
        });
    }

    /** Active blocks are kept, so flooding the map from many addresses cannot lift one. */
    private void makeRoom(long now) {
        entries.values().removeIf(e -> now - e.lastAttempt > FORGET_MILLIS && e.blockedUntil < now);
        if (entries.size() >= MAX_ENTRIES) {
            entries.entrySet().stream()
                    .filter(e -> e.getValue().blockedUntil < now)
                    .sorted(Comparator.comparingLong(e -> e.getValue().lastAttempt))
                    .limit(EVICT_AT_CAP)
                    .map(Map.Entry::getKey)
                    .toList()
                    .forEach(entries::remove);
        }
    }
}
