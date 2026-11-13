package today.bonfire.oss.bth4j.admin.http;

import java.time.Clock;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Blocks a key for a while after repeated failed logins. The server keeps one throttle keyed by where a login
 * comes from and one keyed by the user it names. A successful login takes back only its own attempt, so logging
 * in with one account does not clear the failures counted against another. Failures are forgotten when no
 * attempt followed them for fifteen minutes.
 *
 * <p>The first block of a key lasts ten seconds and each further block twice as long as the one before, up to
 * fifteen minutes. A key starts again at ten seconds when no attempt of it followed its last block for fifteen
 * minutes. A blocked attempt changes nothing, so repeating attempts against a block does not lengthen it.
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
    private static final int MAX_ENTRIES = 10_000;
    private static final int EVICT_AT_CAP = MAX_ENTRIES / 10;

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private final Clock clock;

    LoginThrottle(Clock clock) {
        this.clock = clock;
    }

    /**
     * Checks and counts one login attempt in a single step per key, before the password is verified, so
     * concurrent attempts cannot all pass a block. {@link #takeBack} takes this attempt back.
     *
     * @return milliseconds the key must wait, 0 when this attempt may go ahead
     */
    long attempt(String key) {
        var now = clock.millis();
        if (!entries.containsKey(key) && entries.size() >= MAX_ENTRIES) {
            makeRoom(now);
        }
        var wait = new long[1];
        entries.compute(key, (k, old) -> {
            if (old != null && old.blockedUntil > now) {
                wait[0] = old.blockedUntil - now;
                return old;
            }
            var blocks = old != null && now - old.touched <= RESET_MILLIS ? old.blocks : 0;
            var remembered = old != null && now - old.lastAttempt <= FORGET_MILLIS;
            var attempts = remembered ? old.attempts + 1 : 1;
            var before = old == null ? 0 : old.touched;
            if (attempts < MAX_FAILURES) {
                return new Entry(attempts, 0, now, blocks, now, before, false);
            }
            var block = Math.min(BLOCK_MILLIS << Math.min(blocks, MAX_BLOCKS), MAX_BLOCK_MILLIS);
            return new Entry(0, now + block, now, blocks + 1, now + block, before, false);
        });
        return wait[0];
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
     * Active blocks are kept, so flooding the map with many keys cannot lift one. Of the rest, a key without
     * a block history goes first, so flooding cannot make the throttle forget how often a key was blocked
     * before it has to forget a key that was never blocked. Among those, a key with fewer failures goes before
     * one with more, so a key that is one failure short of a block outlives keys that were tried once. Enough
     * keys with as many failures still push it out: the map is bounded, and so is what it can remember.
     */
    private void makeRoom(long now) {
        entries.values()
                .removeIf(e ->
                        e.blockedUntil <= now && now - idleSince(e) > (e.blocks > 0 ? RESET_MILLIS : FORGET_MILLIS));
        if (entries.size() >= MAX_ENTRIES) {
            entries.entrySet().stream()
                    .filter(e -> e.getValue().blockedUntil <= now)
                    .sorted(Comparator.<Map.Entry<String, Entry>>comparingInt(e -> e.getValue().blocks)
                            .thenComparingInt(e -> e.getValue().attempts)
                            .thenComparingLong(e -> e.getValue().lastAttempt))
                    .limit(EVICT_AT_CAP)
                    .map(Map.Entry::getKey)
                    .toList()
                    .forEach(entries::remove);
        }
    }

    private static long idleSince(Entry e) {
        return e.blocks > 0 ? e.touched : e.lastAttempt;
    }
}
