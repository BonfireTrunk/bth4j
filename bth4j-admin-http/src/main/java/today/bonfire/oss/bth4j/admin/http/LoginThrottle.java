package today.bonfire.oss.bth4j.admin.http;

import java.time.Clock;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Blocks a key for a while after repeated failed logins. The server keeps one throttle keyed by where a login
 * comes from and one keyed by the user it names. A successful login takes back only its own attempt, so logging
 * in with one account does not clear the failures counted against another. Failures are forgotten when no
 * attempt followed them for fifteen minutes.
 *
 * <p>The first block of a key lasts ten seconds and each further block twice as long as the one before, up to
 * fifteen minutes. A key starts again at ten seconds when no attempt of it followed its last block for fifteen
 * minutes. A blocked attempt changes nothing, so repeating attempts against a block does not lengthen it.
 *
 * <p>The throttle never holds more keys than its capacity and the keys named at construction. When every key it
 * holds is blocked or named, a key it does not know is let through without being counted until a block ends.
 */
final class LoginThrottle {

    /**
     * What the throttle knows about a key. {@code blockedUntil} is set only while a block set by the latest
     * attempt may still be taken back. {@code touched} is the last attempt or block end that keeps the key's
     * number of blocks, and {@code previousTouched} its value before the latest attempt. {@code refusalReported}
     * says that {@link #firstRefusal} was answered for the block that is in force.
     */
    private record Entry(
            int attempts,
            long blockedUntil,
            long lastAttempt,
            int blocks,
            long touched,
            long previousTouched,
            boolean refusalReported) {}

    private static final int MAX_FAILURES = 5;
    private static final long BLOCK_MILLIS = 10_000;
    private static final long MAX_BLOCK_MILLIS = 900_000;
    private static final int MAX_BLOCKS = 7;
    // not shorter than the longest block: four attempts per window must not allow more guesses than five and a block
    private static final long FORGET_MILLIS = 900_000;
    private static final long RESET_MILLIS = 900_000;
    static final int DEFAULT_CAPACITY = 10_000;

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    // a key enters the map only under this lock, so that concurrent first attempts cannot pass the capacity together
    private final ReentrantLock admission = new ReentrantLock();
    private final Clock clock;
    private final Set<String> kept;
    private final int capacity;
    // while every key is blocked or kept nothing can make room, and looking again for each attempt of a flood would
    // sort the whole map every time
    private volatile long noRoomBefore;

    LoginThrottle(Clock clock) {
        this(clock, Set.of(), DEFAULT_CAPACITY);
    }

    /**
     * @param kept     keys that are never forgotten to make room, a small fixed set such as the names of the users
     *                 that exist: flooding the throttle with other keys cannot take their failures away
     * @param capacity how many keys the throttle holds before it makes room
     */
    LoginThrottle(Clock clock, Set<String> kept, int capacity) {
        this.clock = clock;
        this.kept = Set.copyOf(kept);
        this.capacity = capacity;
    }

    /**
     * Checks and counts one login attempt in a single step per key, before the password is verified, so
     * concurrent attempts cannot all pass a block. {@link #takeBack} takes this attempt back.
     *
     * <p>A key the throttle does not hold is not counted when the throttle is full and can forget nothing: the
     * attempt goes ahead. Refusing it instead would let whoever filled the throttle keep everyone else out, and the
     * keys named at construction are counted whatever the throttle holds.
     *
     * @return milliseconds the key must wait, 0 when this attempt may go ahead
     */
    long attempt(String key) {
        var now = clock.millis();
        var wait = new long[] {-1};
        entries.computeIfPresent(key, (k, old) -> count(old, now, wait));
        if (wait[0] >= 0) {
            return wait[0];
        }
        admission.lock();
        try {
            if (!entries.containsKey(key) && !kept.contains(key) && entries.size() >= capacity && !makeRoom(now)) {
                return 0;
            }
            entries.compute(key, (k, old) -> count(old, now, wait));
            return wait[0];
        } finally {
            admission.unlock();
        }
    }

    private static Entry count(Entry old, long now, long[] wait) {
        if (old != null && old.blockedUntil > now) {
            wait[0] = old.blockedUntil - now;
            return old;
        }
        wait[0] = 0;
        var blocks = old != null && now - old.touched <= RESET_MILLIS ? old.blocks : 0;
        var remembered = old != null && now - old.lastAttempt <= FORGET_MILLIS;
        var attempts = remembered ? old.attempts + 1 : 1;
        var before = old == null ? 0 : old.touched;
        if (attempts < MAX_FAILURES) {
            return new Entry(attempts, 0, now, blocks, now, before, false);
        }
        var block = Math.min(BLOCK_MILLIS << Math.min(blocks, MAX_BLOCKS), MAX_BLOCK_MILLIS);
        return new Entry(0, now + block, now, blocks + 1, now + block, before, false);
    }

    /**
     * Whether this is the first refused attempt of the key's block, for the caller's log line: attempts against
     * a block come from clients that are not signed in, and one line per attempt would let them fill the log.
     */
    boolean firstRefusal(String key) {
        var first = new boolean[1];
        entries.computeIfPresent(key, (k, e) -> {
            if (e.refusalReported) {
                return e;
            }
            first[0] = true;
            return new Entry(e.attempts, e.blockedUntil, e.lastAttempt, e.blocks, e.touched, e.previousTouched, true);
        });
        return first[0];
    }

    /**
     * Takes back the attempt just counted for the key, and the block it set when it was the fifth, with the
     * longer step that block would have brought: for a login that succeeded, and for one that the other
     * throttle refused before the password was looked at. It touches only this key.
     */
    void takeBack(String key) {
        entries.computeIfPresent(key, (k, e) -> {
            if (e.blockedUntil > 0) {
                noRoomBefore = 0;
                return new Entry(
                        MAX_FAILURES - 1, 0, e.lastAttempt, e.blocks - 1, e.previousTouched, e.previousTouched, false);
            }
            var attempts = e.attempts - 1;
            if (attempts <= 0 && e.blocks == 0) {
                return null;
            }
            return new Entry(
                    Math.max(attempts, 0), 0, e.lastAttempt, e.blocks, e.previousTouched, e.previousTouched, false);
        });
    }

    /**
     * Active blocks and the keys named at construction are kept, so flooding the map with many keys cannot
     * lift a block or take the failures of such a key away. Of the rest, a key without
     * a block history goes first, so flooding cannot make the throttle forget how often a key was blocked
     * before it has to forget a key that was never blocked. Among those, a key with fewer failures goes before
     * one with more, so a key that is one failure short of a block outlives keys that were tried once. Enough
     * keys with as many failures still push it out: the map is bounded, and so is what it can remember.
     *
     * @return false when every key is blocked or kept, so that there is no room for another one
     */
    private boolean makeRoom(long now) {
        if (now < noRoomBefore) {
            return false;
        }
        entries.values()
                .removeIf(e ->
                        e.blockedUntil <= now && now - idleSince(e) > (e.blocks > 0 ? RESET_MILLIS : FORGET_MILLIS));
        if (entries.size() < capacity) {
            return true;
        }
        var forgotten = entries.entrySet().stream()
                .filter(e -> e.getValue().blockedUntil <= now && !kept.contains(e.getKey()))
                .sorted(Comparator.<Map.Entry<String, Entry>>comparingInt(e -> e.getValue().blocks)
                        .thenComparingInt(e -> e.getValue().attempts)
                        .thenComparingLong(e -> e.getValue().lastAttempt))
                .limit(Math.max(1, capacity / 10))
                .map(Map.Entry::getKey)
                .toList();
        forgotten.forEach(entries::remove);
        if (!forgotten.isEmpty()) {
            return true;
        }
        noRoomBefore = entries.entrySet().stream()
                .filter(e -> !kept.contains(e.getKey()))
                .mapToLong(e -> e.getValue().blockedUntil)
                .min()
                .orElse(now);
        return false;
    }

    private static long idleSince(Entry e) {
        return e.blocks > 0 ? e.touched : e.lastAttempt;
    }

    int size() {
        return entries.size();
    }
}
