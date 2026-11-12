package today.bonfire.oss.bth4j.redis;

import static today.bonfire.oss.bth4j.redis.Keys.*;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import today.bonfire.oss.bth4j.TaskState;
import today.bonfire.oss.bth4j.exceptions.TaskStoreException;
import today.bonfire.oss.bth4j.exceptions.UnreadableTaskException;
import today.bonfire.oss.bth4j.spi.RecordJson;
import today.bonfire.oss.bth4j.spi.TaskRecord;

/** Maps {@link TaskRecord} to and from the task hash; instants are epoch milliseconds, durations milliseconds. */
final class TaskHashes {

    private TaskHashes() {}

    /**
     * The non-null hash fields of a new task as field/value pairs. State, due and stateChanged are left out:
     * the script sets them from the store clock. The payload, steps and failures live in their own keys.
     */
    static List<String> fields(TaskRecord t) {
        var f = new ArrayList<String>();
        put(f, F_REVISION, t.revision());
        put(f, F_QUEUE, t.queueId());
        put(f, F_TYPE, t.typeId());
        put(f, F_PAYLOAD_VERSION, t.payloadVersion());
        put(f, F_ATTEMPTS, t.attempts());
        put(f, F_MAX_ATTEMPTS, t.maxAttempts());
        put(f, F_CREATED, t.createdAt());
        put(f, F_RUNNER, t.runnerId());
        put(f, F_LEASE, t.leaseUntil());
        put(f, F_CLAIMED_AT, t.claimedAt());
        put(f, F_ACCOUNT, t.account());
        put(f, F_HEADERS, RecordJson.encodeHeaders(t.headers()));
        put(f, F_UNIQUE_KEY, t.uniqueKey());
        put(f, F_KEY_KEEP, t.keyKeep());
        put(f, F_KEY_UNTIL, t.keyUntil());
        put(f, F_RETRY_POLICY, t.retryPolicy());
        put(f, F_SCHEDULE, t.scheduleId());
        put(f, F_RETRY_OF, t.retryOf());
        put(f, F_START_BY, t.startBy());
        put(f, F_MAX_RUN_TIME, t.maxRunTime());
        put(f, F_CANCEL_AT, t.cancelRequestedAt());
        put(f, F_CANCEL_REASON, t.cancelReason());
        put(f, F_PROGRESS, t.progress());
        return f;
    }

    /** Reads one stored task field by its hash field name; null when the task has none. */
    @FunctionalInterface
    interface Fields {
        String get(String name);
    }

    /** Position of each {@link RedisScripts#CLAIM_FIELDS} field in a claim reply entry. */
    private static final Map<String, Integer> CLAIM_POSITIONS = positions(RedisScripts.CLAIM_FIELDS, 4);

    private static final int CLAIM_ENTRY_SIZE = 4 + RedisScripts.CLAIM_FIELDS.length;

    /** Fields the claim script writes itself, so its reply does not carry them; the claim sets them instead. */
    private static final Set<String> SET_BY_CLAIM = Set.of(F_STATE_CHANGED, F_RUNNER, F_LEASE, F_CLAIMED_AT);

    /**
     * @param h        the task hash
     * @param data     the {@code d:<id>} string, or null
     * @param steps    the {@code st:<id>} string, or null
     * @param failures the {@code f:<id>} entries in order, empty when there are none
     * @throws UnreadableTaskException if a stored value cannot be read
     */
    static TaskRecord fromHash(
            long id, Map<String, String> h, String encodedPayload, String steps, List<String> failures) {
        return decoded(
                id, () -> read(id, h::get, encodedPayload, steps, failures).build());
    }

    /**
     * One entry of a CLAIM reply, {id, payload, steps, {failure...}, then the {@link RedisScripts#CLAIM_FIELDS}
     * values as stored before the claim}, as the task the claim leased: the fields the script changed are set
     * from what it wrote, without a detour through the hash's text form.
     *
     * @param at         the claim time the script returned, epoch milliseconds
     * @param leaseUntil the lease end the script wrote, epoch milliseconds
     * @throws TaskStoreException      if the entry does not have the shape the script returns
     * @throws UnreadableTaskException if a stored value cannot be read
     */
    static TaskRecord fromClaim(Object entry, long at, long leaseUntil, int runnerId) {
        return fromClaim(entry, CLAIM_POSITIONS, CLAIM_ENTRY_SIZE, at, leaseUntil, runnerId);
    }

    /** {@link #fromClaim(Object, long, long, int)} for a reply carrying {@code fields}; for tests of drift. */
    static TaskRecord fromClaim(Object entry, String[] fields, long at, long leaseUntil, int runnerId) {
        return fromClaim(entry, positions(fields, 4), 4 + fields.length, at, leaseUntil, runnerId);
    }

    private static TaskRecord fromClaim(
            Object entry, Map<String, Integer> positions, int size, long at, long leaseUntil, int runnerId) {
        if (!(entry instanceof List<?> e) || e.size() != size) {
            throw new TaskStoreException("claim reply entry is not a list of " + size + " values: "
                    + (entry instanceof List<?> l
                            ? l.size() + " values"
                            : entry == null ? "null" : entry.getClass().getSimpleName()));
        }
        var idText = text(e.get(0), "id", "?");
        long id;
        try {
            id = Long.parseLong(idText);
        } catch (NumberFormatException ex) {
            throw new TaskStoreException("claim reply entry has a malformed task id " + idText, ex);
        }
        Fields stored = name -> {
            var position = positions.get(name);
            if (position != null) return text(e.get(position), name, idText);
            if (SET_BY_CLAIM.contains(name)) return null;
            throw new IllegalStateException(
                    "the claim reply does not carry the task field " + name + "; add it to RedisScripts.CLAIM_FIELDS");
        };
        var payload = text(e.get(1), "payload", idText);
        var steps = text(e.get(2), "steps", idText);
        var failures = texts(e.get(3), idText);
        return decoded(
                id,
                () -> read(id, stored, payload, steps, failures)
                        .state(TaskState.LEASED)
                        .stateChangedAt(Instant.ofEpochMilli(at))
                        .revision(integer(stored, F_REVISION) + 1)
                        .attempts(integer(stored, F_ATTEMPTS) + 1)
                        .claimedAt(Instant.ofEpochMilli(at))
                        .leaseUntil(Instant.ofEpochMilli(leaseUntil))
                        .runnerId(runnerId)
                        .build());
    }

    private static TaskRecord decoded(long id, Supplier<TaskRecord> decode) {
        try {
            return decode.get();
        } catch (IllegalArgumentException | DateTimeException e) {
            // the cause's message may quote the stored value, so it stays out of issues and stored failures
            throw new UnreadableTaskException(id, "task " + id + " has a stored value that cannot be read", e);
        }
    }

    private static String text(Object value, String what, String id) {
        return switch (value) {
            case null -> null;
            case String s -> s;
            default ->
                throw new TaskStoreException("claim reply for task " + id + " has a "
                        + value.getClass().getSimpleName() + " as " + what + ", expected text");
        };
    }

    private static List<String> texts(Object value, String id) {
        return switch (value) {
            case null -> List.of();
            case List<?> l -> {
                var out = new ArrayList<String>(l.size());
                for (var item : l) out.add(text(item, "a failure", id));
                yield out;
            }
            default ->
                throw new TaskStoreException("claim reply for task " + id + " has a "
                        + value.getClass().getSimpleName() + " as failures, expected a list");
        };
    }

    /** Field name to its index plus {@code offset}; a name listed twice would make positions ambiguous. */
    static Map<String, Integer> positions(String[] fields, int offset) {
        var out = new HashMap<String, Integer>();
        for (var i = 0; i < fields.length; i++) {
            if (out.put(fields[i], offset + i) != null) {
                throw new IllegalStateException("field " + fields[i] + " is listed twice");
            }
        }
        return Map.copyOf(out);
    }

    private static TaskRecord.Builder read(
            long id, Fields h, String encodedPayload, String steps, List<String> failures) {
        var state = TaskState.fromCode(integer(h, F_STATE));
        var leased = state == TaskState.LEASED;
        return TaskRecord.builder()
                .id(id)
                .revision(integer(h, F_REVISION))
                .queueId(integer(h, F_QUEUE))
                .typeId(integer(h, F_TYPE))
                .payloadVersion(integer(h, F_PAYLOAD_VERSION))
                .state(state)
                .attempts(integer(h, F_ATTEMPTS))
                .maxAttempts(integer(h, F_MAX_ATTEMPTS))
                .createdAt(instant(h, F_CREATED))
                .due(instant(h, F_DUE))
                .stateChangedAt(instant(h, F_STATE_CHANGED))
                .encodedPayload(encodedPayload)
                .runnerId((leased || state.isTerminal()) && h.get(F_RUNNER) != null ? integer(h, F_RUNNER) : null)
                .leaseUntil(leased ? instant(h, F_LEASE) : null)
                .claimedAt(instant(h, F_CLAIMED_AT))
                .account(h.get(F_ACCOUNT))
                .headers(RecordJson.decodeHeaders(h.get(F_HEADERS)))
                .uniqueKey(h.get(F_UNIQUE_KEY))
                .keyKeep(duration(h, F_KEY_KEEP))
                .keyUntil(instant(h, F_KEY_UNTIL))
                .retryPolicy(h.get(F_RETRY_POLICY))
                .scheduleId(h.get(F_SCHEDULE))
                .retryOf(h.get(F_RETRY_OF) != null ? Long.parseLong(h.get(F_RETRY_OF)) : null)
                .startBy(instant(h, F_START_BY))
                .maxRunTime(duration(h, F_MAX_RUN_TIME))
                .cancelRequestedAt(instant(h, F_CANCEL_AT))
                .cancelReason(h.get(F_CANCEL_REASON))
                .progress(h.get(F_PROGRESS))
                .steps(RecordJson.decodeSteps(steps))
                .failures(
                        failures.isEmpty()
                                ? null
                                : failures.stream()
                                        .map(RecordJson::decodeFailure)
                                        .toList());
    }

    private static void put(List<String> f, String name, Object value) {
        if (value == null) {
            return;
        }
        f.add(name);
        f.add(
                switch (value) {
                    case Instant i -> String.valueOf(i.toEpochMilli());
                    case Duration d -> String.valueOf(d.toMillis());
                    default -> value.toString();
                });
    }

    private static int integer(Fields h, String name) {
        var v = h.get(name);
        return v == null ? 0 : Integer.parseInt(v);
    }

    private static Instant instant(Fields h, String name) {
        var v = h.get(name);
        return v == null ? null : Instant.ofEpochMilli(Long.parseLong(v));
    }

    private static Duration duration(Fields h, String name) {
        var v = h.get(name);
        return v == null ? null : Duration.ofMillis(Long.parseLong(v));
    }
}
