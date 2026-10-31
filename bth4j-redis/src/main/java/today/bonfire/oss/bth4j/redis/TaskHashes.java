package today.bonfire.oss.bth4j.redis;

import static today.bonfire.oss.bth4j.redis.Keys.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import today.bonfire.oss.bth4j.spi.RecordJson;
import today.bonfire.oss.bth4j.spi.TaskRecord;
import today.bonfire.oss.bth4j.spi.TaskState;

/** Maps {@link TaskRecord} to and from the task hash; instants are epoch milliseconds, durations milliseconds. */
final class TaskHashes {

    private TaskHashes() {}

    /**
     * The non-null fields of a new task as field/value pairs. State, due and stateChanged are left out: the
     * script sets them from the store clock.
     */
    static List<String> fields(TaskRecord t) {
        var f = new ArrayList<String>();
        put(f, F_REVISION, t.revision());
        put(f, F_QUEUE, t.queueId());
        put(f, F_EVENT, t.eventId());
        put(f, F_PAYLOAD_VERSION, t.payloadVersion());
        put(f, F_ATTEMPTS, t.attempts());
        put(f, F_MAX_ATTEMPTS, t.maxAttempts());
        put(f, F_CREATED, t.created());
        put(f, F_DATA, t.data());
        put(f, F_WORKER, t.workerId());
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
        put(f, F_MAX_RUN, t.maxRun());
        put(f, F_CANCEL_AT, t.cancelRequestedAt());
        put(f, F_CANCEL_REASON, t.cancelReason());
        put(f, F_PROGRESS, t.progress());
        put(f, F_STEPS, RecordJson.encodeSteps(t.steps()));
        put(f, F_FAILURES, RecordJson.encodeFailures(t.failures()));
        return f;
    }

    static TaskRecord fromHash(long id, Map<String, String> h) {
        return TaskRecord.builder()
                .id(id)
                .revision(integer(h, F_REVISION))
                .queueId(integer(h, F_QUEUE))
                .eventId(integer(h, F_EVENT))
                .payloadVersion(integer(h, F_PAYLOAD_VERSION))
                .state(TaskState.fromCode(integer(h, F_STATE)))
                .attempts(integer(h, F_ATTEMPTS))
                .maxAttempts(integer(h, F_MAX_ATTEMPTS))
                .created(instant(h, F_CREATED))
                .due(instant(h, F_DUE))
                .stateChanged(instant(h, F_STATE_CHANGED))
                .data(h.get(F_DATA))
                .workerId(h.containsKey(F_WORKER) ? integer(h, F_WORKER) : null)
                .leaseUntil(instant(h, F_LEASE))
                .claimedAt(instant(h, F_CLAIMED_AT))
                .account(h.get(F_ACCOUNT))
                .headers(RecordJson.decodeHeaders(h.get(F_HEADERS)))
                .uniqueKey(h.get(F_UNIQUE_KEY))
                .keyKeep(duration(h, F_KEY_KEEP))
                .keyUntil(instant(h, F_KEY_UNTIL))
                .retryPolicy(h.get(F_RETRY_POLICY))
                .scheduleId(h.get(F_SCHEDULE))
                .retryOf(h.containsKey(F_RETRY_OF) ? Long.parseLong(h.get(F_RETRY_OF)) : null)
                .startBy(instant(h, F_START_BY))
                .maxRun(duration(h, F_MAX_RUN))
                .cancelRequestedAt(instant(h, F_CANCEL_AT))
                .cancelReason(h.get(F_CANCEL_REASON))
                .progress(h.get(F_PROGRESS))
                .steps(RecordJson.decodeSteps(h.get(F_STEPS)))
                .failures(RecordJson.decodeFailures(h.get(F_FAILURES)))
                .build();
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

    private static int integer(Map<String, String> h, String name) {
        var v = h.get(name);
        return v == null ? 0 : Integer.parseInt(v);
    }

    private static Instant instant(Map<String, String> h, String name) {
        var v = h.get(name);
        return v == null ? null : Instant.ofEpochMilli(Long.parseLong(v));
    }

    private static Duration duration(Map<String, String> h, String name) {
        var v = h.get(name);
        return v == null ? null : Duration.ofMillis(Long.parseLong(v));
    }
}
