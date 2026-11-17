package today.bonfire.oss.bth4j.spi;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import today.bonfire.oss.bth4j.FailureInfo;
import today.bonfire.oss.bth4j.StepResult;
import today.bonfire.oss.bth4j.TaskState;

/**
 * A task as the store keeps it. Fields that do not apply to the task's state are null. A store never
 * writes a null field (Redis omits the hash field, PostgreSQL stores SQL null). Text is stored as given: an
 * empty string reads back empty, not null. The library never hands a store text that contains U+0000,
 * which PostgreSQL cannot keep; it refuses such producer and operator input and replaces U+0000 with U+FFFD
 * in text from processor code.
 *
 * @param id                unique per namespace, allocated by the client from {@link TaskStore#allocateIds}
 * @param revision          +1 on every state change; never on renew, step or progress writes; the fence every
 *                          transition checks. The client starts a new task at a random value below 2^30, so that
 *                          a lease on an earlier task with the same id practically cannot match it (one chance in
 *                          2^30 for each reuse of an id while such a lease is still held)
 * @param queueId           see {@link TaskStore#queueId}
 * @param typeId            see {@link TaskStore#typeId}
 * @param payloadVersion    the task type's payload version when the task was enqueued
 * @param state             current state
 * @param attempts          how often the task was claimed
 * @param maxAttempts       after this many failed attempts the task is dead
 * @param createdAt         when the client built the task (client clock)
 * @param dueAt             when the task may run: the enqueue time for an immediate task, the due time for a
 *                          scheduled one, the retry time after a failure (store clock); never null once stored
 * @param stateChangedAt    when the current state was entered (store clock); the finish time of a terminal task
 * @param encodedPayload    encoded payload; null when there is none or it was dropped on success
 * @param runnerId          the runner that claimed the task; kept after it finished
 * @param leaseUntil        when the lease expires unless renewed; LEASED only
 * @param claimedAt         when the last attempt started
 * @param account           application-defined owner, or null
 * @param headers           application headers, or null
 * @param uniqueKey         the unique key while it is held, or null
 * @param keyKeep           how long the key stays held after the task ends, at most
 *                          {@link today.bonfire.oss.bth4j.Uniqueness#MAX_KEEP_AFTER_DONE}; null releases it at the end
 * @param keyUntil          when the held key of a finished task is released
 * @param retryPolicy       name of the retry policy; null means the runner's default
 * @param scheduleId        the recurring definition this task is an occurrence of, or null
 * @param retryOf           the dead task this one was created from by an admin retry, or null
 * @param startBy           the task expires when it has not started by then, or null
 * @param maxRunTime        longest time one attempt may run before the runner cancels it, or null for the runner's
 *                          default
 * @param cancelRequestedAt when a cancel was requested, or null
 * @param cancelReason      why, or null
 * @param progress          free-form progress text set by the processor, or null
 * @param steps             completed steps by name, or null
 * @param failures          the last failures, oldest first, or null
 */
public record TaskRecord(
        long id,
        int revision,
        int queueId,
        int typeId,
        int payloadVersion,
        TaskState state,
        int attempts,
        int maxAttempts,
        Instant createdAt,
        Instant dueAt,
        Instant stateChangedAt,
        String encodedPayload,
        Integer runnerId,
        Instant leaseUntil,
        Instant claimedAt,
        String account,
        Map<String, String> headers,
        String uniqueKey,
        Duration keyKeep,
        Instant keyUntil,
        String retryPolicy,
        String scheduleId,
        Long retryOf,
        Instant startBy,
        Duration maxRunTime,
        Instant cancelRequestedAt,
        String cancelReason,
        String progress,
        Map<String, StepResult> steps,
        List<FailureInfo> failures) {

    public TaskRecord {
        headers = headers == null || headers.isEmpty() ? null : Map.copyOf(headers);
        steps = steps == null || steps.isEmpty() ? null : Map.copyOf(steps);
        failures = failures == null || failures.isEmpty() ? null : List.copyOf(failures);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * @return this record as {@link ExecutionStore#enqueue} stores a new task: the fields a producer sets, zero
     *         attempts, and no runner, lease, claim time, key-until, cancel request or reason, progress, steps or
     *         failures. A {@link #keyKeep()} without a {@link #uniqueKey()} keeps nothing and is dropped.
     */
    public TaskRecord asNew() {
        return toBuilder()
                .keyKeep(uniqueKey == null ? null : keyKeep)
                .attempts(0)
                .runnerId(null)
                .leaseUntil(null)
                .claimedAt(null)
                .keyUntil(null)
                .cancelRequestedAt(null)
                .cancelReason(null)
                .progress(null)
                .steps(null)
                .failures(null)
                .build();
    }

    public Builder toBuilder() {
        var b = new Builder();
        b.id = id;
        b.revision = revision;
        b.queueId = queueId;
        b.typeId = typeId;
        b.payloadVersion = payloadVersion;
        b.state = state;
        b.attempts = attempts;
        b.maxAttempts = maxAttempts;
        b.createdAt = createdAt;
        b.dueAt = dueAt;
        b.stateChangedAt = stateChangedAt;
        b.encodedPayload = encodedPayload;
        b.runnerId = runnerId;
        b.leaseUntil = leaseUntil;
        b.claimedAt = claimedAt;
        b.account = account;
        b.headers = headers;
        b.uniqueKey = uniqueKey;
        b.keyKeep = keyKeep;
        b.keyUntil = keyUntil;
        b.retryPolicy = retryPolicy;
        b.scheduleId = scheduleId;
        b.retryOf = retryOf;
        b.startBy = startBy;
        b.maxRunTime = maxRunTime;
        b.cancelRequestedAt = cancelRequestedAt;
        b.cancelReason = cancelReason;
        b.progress = progress;
        b.steps = steps;
        b.failures = failures;
        return b;
    }

    /** @return the last recorded failure, or null */
    public FailureInfo lastFailure() {
        return failures == null ? null : failures.get(failures.size() - 1);
    }

    public static final class Builder {
        private long id;
        private int revision;
        private int queueId;
        private int typeId;
        private int payloadVersion = 1;
        private TaskState state = TaskState.QUEUED;
        private int attempts;
        private int maxAttempts = 1;
        private Instant createdAt;
        private Instant dueAt;
        private Instant stateChangedAt;
        private String encodedPayload;
        private Integer runnerId;
        private Instant leaseUntil;
        private Instant claimedAt;
        private String account;
        private Map<String, String> headers;
        private String uniqueKey;
        private Duration keyKeep;
        private Instant keyUntil;
        private String retryPolicy;
        private String scheduleId;
        private Long retryOf;
        private Instant startBy;
        private Duration maxRunTime;
        private Instant cancelRequestedAt;
        private String cancelReason;
        private String progress;
        private Map<String, StepResult> steps;
        private List<FailureInfo> failures;

        private Builder() {}

        public Builder id(long v) {
            id = v;
            return this;
        }

        public Builder revision(int v) {
            revision = v;
            return this;
        }

        public Builder queueId(int v) {
            queueId = v;
            return this;
        }

        public Builder typeId(int v) {
            typeId = v;
            return this;
        }

        public Builder payloadVersion(int v) {
            payloadVersion = v;
            return this;
        }

        public Builder state(TaskState v) {
            state = v;
            return this;
        }

        public Builder attempts(int v) {
            attempts = v;
            return this;
        }

        public Builder maxAttempts(int v) {
            maxAttempts = v;
            return this;
        }

        public Builder createdAt(Instant v) {
            createdAt = v;
            return this;
        }

        public Builder dueAt(Instant v) {
            dueAt = v;
            return this;
        }

        public Builder stateChangedAt(Instant v) {
            stateChangedAt = v;
            return this;
        }

        public Builder encodedPayload(String v) {
            encodedPayload = v;
            return this;
        }

        public Builder runnerId(Integer v) {
            runnerId = v;
            return this;
        }

        public Builder leaseUntil(Instant v) {
            leaseUntil = v;
            return this;
        }

        public Builder claimedAt(Instant v) {
            claimedAt = v;
            return this;
        }

        public Builder account(String v) {
            account = v;
            return this;
        }

        public Builder headers(Map<String, String> v) {
            headers = v;
            return this;
        }

        public Builder uniqueKey(String v) {
            uniqueKey = v;
            return this;
        }

        public Builder keyKeep(Duration v) {
            keyKeep = v;
            return this;
        }

        public Builder keyUntil(Instant v) {
            keyUntil = v;
            return this;
        }

        public Builder retryPolicy(String v) {
            retryPolicy = v;
            return this;
        }

        public Builder scheduleId(String v) {
            scheduleId = v;
            return this;
        }

        public Builder retryOf(Long v) {
            retryOf = v;
            return this;
        }

        public Builder startBy(Instant v) {
            startBy = v;
            return this;
        }

        public Builder maxRunTime(Duration v) {
            maxRunTime = v;
            return this;
        }

        public Builder cancelRequestedAt(Instant v) {
            cancelRequestedAt = v;
            return this;
        }

        public Builder cancelReason(String v) {
            cancelReason = v;
            return this;
        }

        public Builder progress(String v) {
            progress = v;
            return this;
        }

        public Builder steps(Map<String, StepResult> v) {
            steps = v;
            return this;
        }

        public Builder failures(List<FailureInfo> v) {
            failures = v;
            return this;
        }

        public TaskRecord build() {
            return new TaskRecord(
                    id,
                    revision,
                    queueId,
                    typeId,
                    payloadVersion,
                    state,
                    attempts,
                    maxAttempts,
                    createdAt,
                    dueAt,
                    stateChangedAt,
                    encodedPayload,
                    runnerId,
                    leaseUntil,
                    claimedAt,
                    account,
                    headers,
                    uniqueKey,
                    keyKeep,
                    keyUntil,
                    retryPolicy,
                    scheduleId,
                    retryOf,
                    startBy,
                    maxRunTime,
                    cancelRequestedAt,
                    cancelReason,
                    progress,
                    steps,
                    failures);
        }
    }
}
